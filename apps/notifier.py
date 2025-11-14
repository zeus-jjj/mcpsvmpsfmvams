"""
для отправки через определённое время юзерам заготовленного
уведомления в ТГ. Уведомления задаются в messages.json.
"""
import asyncio
import time
#
from os import getenv
from datetime import datetime, timedelta
from aiohttp import ClientSession
#
from modules import MAX_CHARS_USERS_HISTORY, MAP, create_connect
import apps.logger as logger
#
from apps.funcs import send_message, run_action, save_event
from modules import bot
from apps.bot_info import bot_info

# для дискорда
ds_token = getenv('DS_TOKEN')
ds_channel = getenv('DS_CHANNEL')
headers = {
        "Authorization": f"Bot {ds_token}",
        "Content-Type": "application/json"
    }


class Notifier():

    def __init__(self):
        self.bot = bot
        self.MESSAGES = MAP

    async def main(self):
        await logger.info(f"Модуль отложенных уведомлений успешно запущен!")
        while True:
            try:
                # получаем словарь уведомлений, которые уже нужно отправить
                notifications = await self.load_notifications()
                for notification in notifications:
                    await self.send_notification(notification=notification)
                await asyncio.sleep(0.5)
            except Exception as error:
                await logger.error(f"Критическая ошибка в объекте рассыльщика уведомлений. Ошибка: {error}. Выполняем попытку перезапуска...")


    async def add_notifications(self, user_id: int, notifications: list):
        db = await create_connect()
        # ожидание до отправки в секундах
        for notification in notifications:

            label = notification.get('message')
            wait = notification.get('at_time')

            if (wait_seconds:=wait.get("wait_seconds", None)):
                # определяем, во сколько должно отправится уведомление
                send_time = int(time.time()) + wait_seconds
            elif (target_datetime:=wait.get("target_datetime", None)):
                # target_datetime имеет вид "дд.мм.гггг чч:мм" по GMT (в +0 часовом поясе!)
                dt = datetime.strptime(target_datetime, "%d.%m.%Y %H:%M")
                # Конвертируем в timestamp
                send_time = int(dt.timestamp())
            # если это указание определенного времени отправки и сдвиг дней
            else:
                # Получение текущего времени в секундах с начала эпохи
                now_seconds = int(time.time())

                # Получение текущего времени в формате datetime
                now = datetime.fromtimestamp(now_seconds)
                target_time_str = wait.get("time", "00:00")
                delta_days = wait.get("delta_days", 1)

                # Разделение строки времени на часы и минуты
                target_hour, target_minute = map(int, target_time_str.split(':'))
                # Вычисление целевой даты и времени
                target_date = now + timedelta(days=delta_days)
                target_datetime = datetime(target_date.year, target_date.month, target_date.day, target_hour, target_minute)

                # Преобразование целевого времени в секунды с начала эпохи
                send_time = int(target_datetime.timestamp())

            # # раскомментить при первом запуске, чтобы создать уникальный фильтр на добавление записей
            # await db.execute(
            #     """
            #     ALTER TABLE notifications ADD UNIQUE (user_id, label);
            #     """
            #     )

            # если это сообщение, которое может быть отправлено несколько раз, исходя из событий
            if notification.get('reusable', False):
                await db.execute(
                     """INSERT INTO notifications (user_id, time_to_send, label, is_active)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id, label) DO UPDATE 
                        SET time_to_send = $2, 
                        is_active = $4""", 
                        user_id, send_time, label, True
                    )
            # если это не многоразовое оповещение (которое отправить нужно всего 1 раз)
            else:
                await db.execute(
                    """INSERT INTO notifications (user_id, time_to_send, label, is_active) VALUES ($1, $2, $3, $4) 
                    ON CONFLICT (user_id, label) DO NOTHING""", 
                    user_id, send_time, label, True
                )

        await db.close()
        

    async def load_notifications(self):
        now = int(time.time())
        db = await create_connect()

        # закрываем уведомления для тех, кто прошёл воронку (пока название воронки всегда=default, т.к. пока нет механизма раздающего разные названия разным воронкам)
        await db.execute(
            """
            UPDATE notifications
            SET is_active = FALSE
            WHERE user_id IN (
                SELECT user_id FROM funnel_passed WHERE funnel_name = 'default' AND passed = TRUE
            )
            """
        )

        # Теперь выбираем уведомления, исключая заблокированных пользователей
        notifications = await db.fetch(
            """
            SELECT n.id, n.user_id, n.time_to_send, n.label 
            FROM notifications n
            LEFT JOIN users u ON u.id = n.user_id
            WHERE n.is_active = TRUE
            AND u.user_block = FALSE
            AND n.time_to_send < $1
            AND n.user_id NOT IN (
                SELECT user_id FROM funnel_passed WHERE funnel_name = 'default' AND passed = TRUE
            )
            """,
            now
        )

        await db.close()
        return notifications


    async def close_notification(self, notification_id=None, user_id=None, label=None):
        # закрывает уведомление в БД по его id
        db = await create_connect()
        if notification_id:
            await db.execute("UPDATE notifications SET is_active = $1 WHERE id = $2", False, notification_id)
        else:
            await db.execute("UPDATE notifications SET is_active = $1 WHERE user_id = $2 AND label = $3 AND is_active = $4", 
            False, user_id, label, True)
        await db.close()

    async def blocked(self, user_id, is_blocked: bool = False):
        # ставит метку в БД, заблокировал ли бота юзер, или разблокировал
        # возвращает юзернейм и id пользователя, а также личность бота
        db = await create_connect()

        if is_blocked:
            action = "заблокировал"
        else:
            action = "разблокировал"

        # получаем никнейм юзера 
        username = await db.fetchrow(
            """SELECT username FROM users WHERE id = $1""", user_id
        )
        # преобразуем в словарь
        username = username.get('username') if type(username) == dict else "unknown"

        # сохраняем в историю юзера
        await db.execute(
            """INSERT INTO user_history (user_id, text) VALUES ($1, $2)""", user_id,
            f"Пользователь {action} бота!"
        )
        # делаем отметку в БД, заблочен-ли бот
        await db.execute("""
                            UPDATE users 
                            SET user_block = $1 
                            WHERE id = $2""",
                            is_blocked, 
                            user_id
                        )
        await db.close()

        return username

    async def discord_alert(self, text: str):
        # для отправки сообщения в ДС
        try:
            # Url АПИ
            url = f"https://discord.com/api/v9/channels/{ds_channel}/messages"
            # сообщение
            payload = {"content": text}
            async with ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    return response
        except Exception as error:
            await logger.error(f"При отправке алерта в дискорд возникла ошибка: {error}")
            return None

    async def send_notification(self, notification):
        # отправка уведомления в ТГ и закрытие в БД
        try:
            user_id = notification.get('user_id')
            msg_data = self.MESSAGES["callback"].get(notification.get('label'), None)
            if msg_data is None:
                await self.close_notification(user_id=user_id, label=notification.get('label'))
                return

            # проверяем, если прошло много времени с тех пор как уведомление должно было быть
            # доставлено, то закрываем его (172800 = 2 суток)
            if int(time.time()) - notification.get('time_to_send') > 172800:
                # закрываем уведомление
                await self.close_notification(notification_id=notification.get('id'))
                await logger.error(f"Прошло слишком много времени с наступления момента, когда должно было быть отправлено уведомление. Уведомление автоматически закрыто!")
                return

            # проверяем, есть-ли действие для этого сообщения
            result = False
            if (act:=msg_data.get("action")) or (act:=msg_data.get("actions")):
                if type(act) == list:
                    for action in act:
                        result = await run_action(action=action, user_id=user_id, bot=bot)
                elif type(act) == dict:
                    result = await run_action(action=act, user_id=user_id, bot=bot)
                else:
                    await logger.error(f"Ошибка в типе данных action(s)! Должен быть list/dict, а передан {type(act)}!")
            # Если результат выполнения action(s) положительный, то закроем уведомление и подменим данные сообщения
            if result:
                # если True, то подменяем данные для ответа и маршрут
                route = act.get('is_ok', None)
                msg_data = MAP['callback'].get(route, None)

            # проверяем, есть-ли ивент, который нужно записать
            if (event:=msg_data.get("event", None)):
                await save_event(user_id=user_id, event=event)


            


            # если msg_data пустой, то вероятно это заглушка, которая появилась из-за
            # срабатывания action. Поэтому закрываем уведомление
            if not msg_data:
                await self.close_notification(notification_id=notification.get('id'))

            if msg_data.get("text", None) or msg_data.get("file", None):
                await logger.debug(f"[notifier] Сейчас будет отправлено уведомление для {user_id}!")
                sending_result = await send_message(bot=self.bot, 
                    user_id=user_id, 
                    msg_data=msg_data,
                    route=notification.get('label'))

                # добавляем отложенное уведомление (не добавится, если уже было добавлено 
                # ранее, либо если для этой личности нет уведомлений)
                if sending_result:
                    if (new_notifications:=msg_data.get('notifications', None)):
                        if type(new_notifications) == list:
                            await self.add_notifications(user_id=user_id, 
                                notifications=new_notifications)
                        else:
                            await logger.error(f"В map.json неверно указан блок notifications в {notification.get('label')}, должен быть тип list!")

                    # закрываем уведомление
                    await self.close_notification(notification_id=notification.get('id'))
                    await logger.info(f"Уведомление {notification.get('id')} отправлено и закрыто!")

                    if msg_data.get("text", None):
                        # сохраняем в историю юзера, если в уведомлении был текст
                        db = await create_connect()
                        await db.execute(
                            """INSERT INTO user_history (user_id, text) VALUES ($1, LEFT($2, $3))""", user_id,
                            f"Получил уведомление с текстом: {self.MESSAGES['callback'][notification.get('label')]['text']}",
                            MAX_CHARS_USERS_HISTORY
                        )
                        await db.close()

            # если в map.json не указан текст в блоке, но указано отложенное уведомление - добавляем (т.к. там
            # скорее всего есть action)
            elif not msg_data.get("text") and (new_notifications:=msg_data.get('notifications', None)):
                await self.add_notifications(user_id=user_id, 
                    notifications=new_notifications)

        except Exception as error:
            # если юзер заблокировал бота
            if "blocked" in str(error):
                # делаем запись в БД, что юзер заблочил бота
                username = await self.blocked(user_id=user_id, is_blocked=True)
                await logger.error(f"Пользователь @{username.get('username', None)} заблокировал бота!")
                # если username извлечён из БД
                if username:
                    await self.discord_alert(text=f"? @{bot_info.get_username()}\n```Пользователь @{username.get('username', None)} [id{notification.get('user_id')}] заблокировал бота!\nПока пользователь его не разблокирует, бот не сможет отправлять ему уведомления```")
            await logger.error(f"Не удалось отправить уведомление {notification.get('id')}. Ошибка: {error}")
            


# создаём объект, чтобы потом его легко импортировать из разных модулей
notificator = Notifier()


"""
Тут не сделал отправку сообщений в JIVO, т.к. этот модуль пока вообще отключен.
Если будем делать воронку через модуль личностей бота, то нужно будет тут и в остальных
местах сделать интеграцию с JIVO, как это сделано в bot.py или в ph_notifier.py (?)
"""