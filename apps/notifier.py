"""
для отправки через определённое время юзерам заготовленного
уведомления в ТГ. Уведомления задаются в messages.json.
"""
import asyncio
import time
from collections import defaultdict
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

        # ДОБАВЛЕНО: поддержка "догрева"
        self._dogrev_users_cache = set()
        self._paused_notifications = defaultdict(dict)

    async def main(self):
        await logger.info(f"Модуль отложенных уведомлений успешно запущен!")
        while True:
            try:
                notifications = await self.load_notifications()
                for notification in notifications:
                    await self.send_notification(notification=notification)
                await asyncio.sleep(0.5)
            except Exception as error:
                await logger.error(f"Критическая ошибка в объекте рассыльщика уведомлений. Ошибка: {error}. Выполняем попытку перезапуска...")


    async def add_notifications(self, user_id: int, notifications: list):
        db = await create_connect()
        for notification in notifications:

            label = notification.get('message')
            wait = notification.get('at_time')

            if (wait_seconds:=wait.get("wait_seconds", None)):
                send_time = int(time.time()) + wait_seconds
            elif (target_datetime:=wait.get("target_datetime", None)):
                dt = datetime.strptime(target_datetime, "%d.%m.%Y %H:%M")
                send_time = int(dt.timestamp())
            else:
                now_seconds = int(time.time())
                now = datetime.fromtimestamp(now_seconds)
                target_time_str = wait.get("time", "00:00")
                delta_days = wait.get("delta_days", 1)
                target_hour, target_minute = map(int, target_time_str.split(':'))
                target_date = now + timedelta(days=delta_days)
                target_datetime = datetime(
                    target_date.year, target_date.month, target_date.day,
                    target_hour, target_minute
                )
                send_time = int(target_datetime.timestamp())

            if notification.get('reusable', False):
                await db.execute(
                     """INSERT INTO notifications (user_id, time_to_send, label, is_active)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT (user_id, label) DO UPDATE
                        SET time_to_send = $2,
                        is_active = $4""",
                        user_id, send_time, label, True
                )
            else:
                await db.execute(
                    """INSERT INTO notifications (user_id, time_to_send, label, is_active)
                       VALUES ($1, $2, $3, $4)
                       ON CONFLICT (user_id, label) DO NOTHING""",
                    user_id, send_time, label, True
                )

        await db.close()


    async def load_notifications(self):
        now = int(time.time())
        db = await create_connect()

        await db.execute(
            """
            UPDATE notifications
            SET is_active = FALSE
            WHERE user_id IN (
                SELECT user_id FROM funnel_passed
                WHERE funnel_name = 'default' AND passed = TRUE
            )
            """
        )

        # ДОБАВЛЕНО поле is_dogrev
        raw_notifications = await db.fetch(
            """
            SELECT n.id, n.user_id, n.time_to_send, n.label,
                   COALESCE(u.is_dogrev, FALSE) AS is_dogrev
            FROM notifications n
            LEFT JOIN users u ON u.id = n.user_id
            WHERE n.is_active = TRUE
            AND COALESCE(u.user_block, FALSE) = FALSE
            AND n.time_to_send < $1
            AND n.user_id NOT IN (
                SELECT user_id FROM funnel_passed
                WHERE funnel_name = 'default' AND passed = TRUE
            )
            """,
            now
        )

        # ДОБАВЛЕН новый обработчик догрева
        processed = await self._process_loaded_notifications(db, raw_notifications)

        await db.close()
        return processed


    async def _process_loaded_notifications(self, db, raw_notifications):

        current_dogrev = set()
        filtered = []

        for notification in raw_notifications:
            data = dict(notification)
            is_dogrev = data.pop("is_dogrev", False)

            if is_dogrev:
                user_id = data["user_id"]
                notification_id = data["id"]
                current_dogrev.add(user_id)
                self._paused_notifications[user_id][notification_id] = data["time_to_send"]
                continue

            filtered.append(data)

        # пользователи, которые вышли из догрева
        exited = self._dogrev_users_cache - current_dogrev
        if exited:
            await self._reactivate_notifications(db, exited)

        for user in exited:
            self._paused_notifications.pop(user, None)

        self._dogrev_users_cache = current_dogrev

        return filtered


    async def _reactivate_notifications(self, db, user_ids):
        now_ts = int(time.time())

        expired = set()
        future = set()

        for user_id in user_ids:
            paused = self._paused_notifications.get(user_id, {})
            for nid, send_at in paused.items():
                if send_at is None or send_at < now_ts:
                    expired.add(nid)
                else:
                    future.add(nid)

        if expired:
            await db.execute(
                """
                UPDATE notifications
                SET is_active = TRUE,
                    time_to_send = $2
                WHERE id = ANY($1::int[])
                """,
                list(expired), now_ts
            )

        if future:
            await db.execute(
                """
                UPDATE notifications
                SET is_active = TRUE
                WHERE id = ANY($1::int[])
                """,
                list(future)
            )


    async def close_notification(self, notification_id=None, user_id=None, label=None):
        db = await create_connect()
        if notification_id:
            await db.execute("UPDATE notifications SET is_active = $1 WHERE id = $2",
                             False, notification_id)
        else:
            await db.execute("""
                UPDATE notifications
                SET is_active = $1
                WHERE user_id = $2 AND label = $3 AND is_active = $4
            """, False, user_id, label, True)
        await db.close()


    async def blocked(self, user_id, is_blocked: bool = False):
        db = await create_connect()

        action = "заблокировал" if is_blocked else "разблокировал"

        username = await db.fetchrow("SELECT username FROM users WHERE id = $1", user_id)
        username = username.get("username") if isinstance(username, dict) else "unknown"

        await db.execute(
            """INSERT INTO user_history (user_id, text) VALUES ($1, $2)""",
            user_id,
            f"Пользователь {action} бота!"
        )

        await db.execute("""
            UPDATE users
            SET user_block = $1
            WHERE id = $2
        """, is_blocked, user_id)

        await db.close()
        return username


    async def discord_alert(self, text: str):
        try:
            url = f"https://discord.com/api/v9/channels/{ds_channel}/messages"
            payload = {"content": text}
            async with ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    return response
        except Exception as error:
            await logger.error(f"Ошибка Discord: {error}")
            return None


    async def send_notification(self, notification):
        try:
            user_id = notification.get('user_id')
            msg_data = self.MESSAGES["callback"].get(notification.get('label'))

            if msg_data is None:
                await self.close_notification(user_id=user_id, label=notification.get('label'))
                return

            # если уведомлению более 2 дней — закрываем
            if int(time.time()) - notification.get('time_to_send') > 172800:
                await self.close_notification(notification_id=notification.get('id'))
                await logger.error("Уведомление протухло и закрыто.")
                return

            result = False
            if (act:=msg_data.get("action")) or (act:=msg_data.get("actions")):
                if isinstance(act, list):
                    for action in act:
                        result = await run_action(action=action, user_id=user_id, bot=bot)
                elif isinstance(act, dict):
                    result = await run_action(action=act, user_id=user_id, bot=bot)
                else:
                    await logger.error(f"action должен быть list/dict, но пришёл {type(act)}")

            if result:
                route = act.get('is_ok')
                msg_data = MAP['callback'].get(route)

            if (event := msg_data.get("event")):
                await save_event(user_id=user_id, event=event)

            if not msg_data:
                await self.close_notification(notification_id=notification.get('id'))
                return

            if msg_data.get("text") or msg_data.get("file"):
                sending = await send_message(
                    bot=self.bot,
                    user_id=user_id,
                    msg_data=msg_data,
                    route=notification.get('label')
                )

                if sending:
                    new = msg_data.get("notifications")
                    if new:
                        await self.add_notifications(user_id=user_id, notifications=new)

                    await self.close_notification(notification_id=notification.get('id'))

                    db = await create_connect()
                    await db.execute(
                        """INSERT INTO user_history (user_id, text) VALUES ($1, LEFT($2,$3))""",
                        user_id,
                        f"Получил уведомление: {msg_data.get('text','')}",
                        MAX_CHARS_USERS_HISTORY
                    )
                    await db.close()

            elif (new := msg_data.get("notifications")):
                await self.add_notifications(user_id=user_id, notifications=new)

        except Exception as error:

            if "blocked" in str(error):
                username = await self.blocked(user_id=user_id, is_blocked=True)
                await self.discord_alert(
                    text=f"? @{bot_info.get_username()}\n"
                         f"Пользователь @{username} [id{user_id}] заблокировал бота!"
                )

            await logger.error(f"Ошибка отправки уведомления: {error}")


notificator = Notifier()
