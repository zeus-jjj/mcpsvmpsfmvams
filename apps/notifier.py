"""
Умный модуль отложенных уведомлений с контролем активности пользователей
"""
import asyncio
import time
from collections import defaultdict
from os import getenv
from datetime import datetime, timedelta
from typing import Dict, Tuple
from aiohttp import ClientSession

from modules import DEFAULT_FUNNEL, MAX_CHARS_USERS_HISTORY, create_connect, get_funnel
import apps.logger as logger
from apps.funcs import send_message, run_action, save_event
from modules import bot
from apps.bot_info import bot_info


def _extract_next_route(action_data):
    """Возвращает маршрут из action(s), если он указан."""
    if isinstance(action_data, dict):
        return action_data.get('is_ok')
    if isinstance(action_data, list):
        for action in reversed(action_data):
            if isinstance(action, dict) and action.get('is_ok'):
                return action['is_ok']
    return None


# Настройки Discord
ds_token = getenv('DS_TOKEN')
ds_channel = getenv('DS_CHANNEL')
headers = {
    "Authorization": f"Bot {ds_token}",
    "Content-Type": "application/json"
}


class SmartNotifier:
    def __init__(self):
        self.bot = bot
        self._notification_funnels: Dict[Tuple[int, str], str] = {}

        # Настройки активности
        self.INACTIVITY_THRESHOLD_DAYS = 45  # 1.5 месяца
        self.MAX_NOTIFICATION_PERIOD_DAYS = 60  # 2 месяца максимум для рассылки

        # Кэш для паузы уведомлений
        self._paused_notifications = defaultdict(dict)

    async def main(self):
        """Основной цикл обработки уведомлений"""
        await logger.info("Умный модуль отложенных уведомлений запущен!")
        while True:
            try:
                notifications = await self.load_notifications()
                for notification in notifications:
                    # Проверяем активность перед отправкой
                    if await self.check_user_activity(notification['user_id']):
                        await self.send_notification(notification)
                    else:
                        await self.pause_user_notifications(notification['user_id'])
                await asyncio.sleep(0.5)
            except Exception as error:
                await logger.error(f"Ошибка в рассыльщике: {error}")

    async def check_user_activity(self, user_id: int) -> bool:
        """
        Проверяет активность пользователя
        Возвращает True если пользователь активен, False если нужно остановить рассылку
        """
        db = await create_connect()

        # Получаем данные об активности
        activity = await db.fetchrow("""
            SELECT
                -- Последняя активность (из last_activity или истории)
                COALESCE(
                    u.last_activity,
                    (SELECT MAX(timestamp) FROM user_history WHERE user_id = $1)
                ) as last_activity,
                -- Последний запуск бота
                (SELECT MAX(timestamp) FROM user_history
                 WHERE user_id = $1 AND text LIKE '%запустил бота%') as last_start,
                -- Первое уведомление в текущей серии
                (SELECT MIN(created_at) FROM notifications
                 WHERE user_id = $1 AND is_active = true) as first_notification,
                -- Количество отправленных уведомлений за последние 2 месяца
                (SELECT COUNT(*) FROM user_history
                 WHERE user_id = $1
                 AND text LIKE 'Получил уведомление%'
                 AND timestamp > NOW() - INTERVAL '2 months') as notifications_sent
            FROM users u
            WHERE u.id = $1
        """, user_id)

        await db.close()

        if not activity:
            return True  # Новый пользователь - отправляем

        now = datetime.now()

        # 1. Проверяем последнюю активность
        if activity['last_activity']:
            days_inactive = (now - activity['last_activity']).days

            # Если неактивен более 1.5 месяцев - блокируем
            if days_inactive > self.INACTIVITY_THRESHOLD_DAYS:
                await logger.info(f"Пользователь {user_id} неактивен {days_inactive} дней - блокируем")
                return False

        # 2. Проверяем длительность текущей серии уведомлений
        if activity['first_notification']:
            notification_period = (now - activity['first_notification']).days

            # Если отправляем уведомления более 2 месяцев без реакции
            if notification_period > self.MAX_NOTIFICATION_PERIOD_DAYS:
                if not activity['last_activity'] or activity['last_activity'] < activity['first_notification']:
                    await logger.info(f"Пользователь {user_id} не реагирует {notification_period} дней")
                    return False

        # 3. Проверяем количество игнорируемых уведомлений
        if activity['notifications_sent'] and activity['notifications_sent'] > 10:
            if activity['last_activity']:
                days_inactive = (now - activity['last_activity']).days
                if days_inactive > 14:  # 2 недели
                    await logger.info(f"Пользователь {user_id} проигнорировал {activity['notifications_sent']} уведомлений")
                    return False

        return True

    async def pause_user_notifications(self, user_id: int):
        """Ставит уведомления пользователя на паузу"""
        db = await create_connect()

        # Деактивируем все активные уведомления
        await db.execute("""
            UPDATE notifications
            SET is_active = false,
                paused_at = NOW(),
                pause_reason = 'inactivity'
            WHERE user_id = $1 AND is_active = true
        """, user_id)

        # Записываем в историю
        await db.execute("""
            INSERT INTO user_history (user_id, text)
            VALUES ($1, $2)
        """, user_id, "Уведомления приостановлены из-за неактивности")

        await db.close()
        await logger.info(f"Уведомления пользователя {user_id} поставлены на паузу")

    async def resume_user_notifications(self, user_id: int):
        """Возобновляет уведомления при активности пользователя"""
        db = await create_connect()

        # НОВОЕ: Проверяем, зарегистрирован ли пользователь
        is_registered = await db.fetchrow("""
            SELECT COUNT(*) as cnt FROM events
            WHERE user_id = $1 AND event_type = 'course_registration'
        """, user_id)

        # Проверяем приостановленные уведомления
        paused = await db.fetch("""
            SELECT id, label, time_to_send
            FROM notifications
            WHERE user_id = $1
            AND is_active = false
            AND pause_reason = 'inactivity'
            AND paused_at > NOW() - INTERVAL '6 months'
        """, user_id)

        if paused:
            now = int(time.time())

            for notif in paused:
                # НОВОЕ: Пропускаем догревочные уведомления для зарегистрированных
                if notif['label'].startswith('warmup_') and is_registered and is_registered['cnt'] > 0:
                    await logger.info(f"Пропускаем догревочное уведомление {notif['label']} для зарегистрированного пользователя {user_id}")
                    continue

                new_time = now + 300  # Через 5 минут после активности

                await db.execute("""
                    UPDATE notifications
                    SET is_active = true,
                        time_to_send = $1,
                        paused_at = NULL,
                        pause_reason = NULL
                    WHERE id = $2
                """, new_time, notif['id'])

            resumed_count = len([n for n in paused if not (n['label'].startswith('warmup_') and is_registered and is_registered['cnt'] > 0)])

            if resumed_count > 0:
                await logger.info(f"Возобновлены {resumed_count} уведомлений для пользователя {user_id}")

                await db.execute("""
                    INSERT INTO user_history (user_id, text)
                    VALUES ($1, $2)
                """, user_id, "Уведомления возобновлены после активности")

        await db.close()

    def _remember_notification_funnel(self, user_id: int, label: str, funnel_name: str | None):
        if not user_id or not label:
            return
        self._notification_funnels[(user_id, label)] = (funnel_name or DEFAULT_FUNNEL).lower()

    def _drop_notification_funnel(self, user_id: int | None, label: str | None):
        if not user_id or not label:
            return
        self._notification_funnels.pop((user_id, label), None)

    def _resolve_notification_funnel(self, notification) -> str:
        user_id = notification.get('user_id')
        label = notification.get('label')
        cached = self._notification_funnels.get((user_id, label))
        if cached:
            return cached
        fallback = notification.get('funnel_name') or DEFAULT_FUNNEL
        if fallback != DEFAULT_FUNNEL:
            self._remember_notification_funnel(user_id, label, fallback)
        return fallback

    async def add_notifications(self, user_id: int, notifications: list, funnel_name: str = DEFAULT_FUNNEL):
        """
        Добавляет уведомления с проверкой статуса пользователя.

        ЛОГИКА:
        1. Зарегистрированные пользователи → НЕ получают догрев, получают обычные уведомления
        2. Незарегистрированные НЕ в воронке курса → получают догрев
        3. Незарегистрированные В воронке курса → получают обычные уведомления (без догрева)
        """
        funnel_name = (funnel_name or DEFAULT_FUNNEL).lower()
        db = await create_connect()

        # Проверяем статус пользователя
        user_status = await db.fetchrow("""
            SELECT
                u.timestamp_registration,
                -- Проверяем, зарегистрирован ли на курсе
                (SELECT COUNT(*) FROM events
                 WHERE user_id = $1
                 AND event_type = 'course_registration') as is_registered,
                -- Проверяем, в воронке ли пользователя курса
                (SELECT COUNT(*) FROM user_funnel
                 WHERE user_id = $1
                 AND (label LIKE '%course%' OR label LIKE '%spin%'
                      OR label LIKE '%mtt%' OR label LIKE '%cash%')) as in_course_funnel,
                -- Последняя активность
                COALESCE(
                    u.last_activity,
                    (SELECT MAX(timestamp) FROM user_history WHERE user_id = $1)
                ) as last_activity
            FROM users u
            WHERE u.id = $1
        """, user_id)

        if user_status:
            # ДОГРЕВОЧНЫЕ УВЕДОМЛЕНИЯ:
            # Только если НЕ в воронке курса И НЕ зарегистрирован
            should_add_warmup = (
                user_status['in_course_funnel'] == 0 and
                user_status['is_registered'] == 0
            )

            if should_add_warmup:
                await self._add_warmup_notifications(user_id, db, funnel_name)
            else:
                await logger.info(
                    f"Пользователь {user_id}: "
                    f"зарегистрирован={user_status['is_registered']}, "
                    f"в_воронке={user_status['in_course_funnel']} "
                    f"→ догрев НЕ добавляем, обычные уведомления ДОБАВЛЯЕМ"
                )

        # Добавляем ОБЫЧНЫЕ уведомления (для всех, независимо от статуса)
        for notification in notifications:
            label = notification.get('message')
            wait = notification.get('at_time')
            notification_funnel = (notification.get('funnel') or funnel_name or DEFAULT_FUNNEL).lower()

            # Вычисляем время отправки
            send_time = self._calculate_send_time(wait)

            if notification.get('reusable', False):
                await db.execute("""
                    INSERT INTO notifications (user_id, time_to_send, label, is_active)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (user_id, label) DO UPDATE
                    SET time_to_send = $2, is_active = $4
                """, user_id, send_time, label, True)
            else:
                await db.execute("""
                    INSERT INTO notifications (user_id, time_to_send, label, is_active)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (user_id, label) DO NOTHING
                """, user_id, send_time, label, True)

            self._remember_notification_funnel(user_id, label, notification_funnel)

        await db.close()

    async def _add_warmup_notifications(self, user_id: int, db, funnel_name: str):
        """
        Добавляет догревочные уведомления ТОЛЬКО для незарегистрированных
        пользователей, которые НЕ находятся в воронке курса
        """
        # Проверяем существующие догревочные уведомления
        existing = await db.fetchrow("""
            SELECT COUNT(*) as cnt FROM notifications
            WHERE user_id = $1 AND label LIKE 'warmup_%' AND is_active = true
        """, user_id)

        if existing and existing['cnt'] > 0:
            await logger.info(f"У пользователя {user_id} уже есть догревочные уведомления")
            return

        # КРИТИЧЕСКИ ВАЖНО: Проверяем, что пользователь НЕ зарегистрирован
        is_registered = await db.fetchrow("""
            SELECT COUNT(*) as cnt FROM events
            WHERE user_id = $1 AND event_type = 'course_registration'
        """, user_id)

        if is_registered and is_registered['cnt'] > 0:
            await logger.info(f"Пользователь {user_id} зарегистрирован, догрев не нужен")
            return

        # Проверяем, что пользователь НЕ в воронке курса
        in_course_funnel = await db.fetchrow("""
            SELECT COUNT(*) as cnt FROM user_funnel
            WHERE user_id = $1
            AND (label LIKE '%course%' OR label LIKE '%spin%'
                 OR label LIKE '%mtt%' OR label LIKE '%cash%')
        """, user_id)

        if in_course_funnel and in_course_funnel['cnt'] > 0:
            await logger.info(f"Пользователь {user_id} уже в воронке курса, догрев не нужен")
            return

        await logger.info(f"Добавляем догревочные уведомления для пользователя {user_id}")

        # Серия догревочных сообщений (ТОЛЬКО для незарегистрированных)
        warmup_messages = [
            {"label": "warmup_why_poker", "days": 1, "time": "10:00"},
            {"label": "warmup_success_stories", "days": 2, "time": "14:00"},
            {"label": "warmup_free_course", "days": 3, "time": "10:00"},
            {"label": "warmup_last_chance", "days": 5, "time": "19:00"},
            {"label": "warmup_special_offer", "days": 7, "time": "14:00"}
        ]

        base_time = datetime.now()

        for msg in warmup_messages:
            target_date = base_time + timedelta(days=msg['days'])
            hour, minute = map(int, msg['time'].split(':'))
            target_datetime = datetime(
                target_date.year, target_date.month, target_date.day,
                hour, minute
            )
            send_time = int(target_datetime.timestamp())

            await db.execute("""
                INSERT INTO notifications (user_id, time_to_send, label, is_active)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
            """, user_id, send_time, msg['label'], True)
            self._remember_notification_funnel(user_id, msg['label'], funnel_name)

    def _calculate_send_time(self, wait):
        """Вычисляет время отправки уведомления"""
        if wait_seconds := wait.get("wait_seconds"):
            return int(time.time()) + wait_seconds
        elif target_datetime := wait.get("target_datetime"):
            dt = datetime.strptime(target_datetime, "%d.%m.%Y %H:%M")
            return int(dt.timestamp())
        else:
            now = datetime.fromtimestamp(int(time.time()))
            target_time_str = wait.get("time", "00:00")
            delta_days = wait.get("delta_days", 1)

            target_hour, target_minute = map(int, target_time_str.split(':'))
            target_date = now + timedelta(days=delta_days)
            target_datetime = datetime(
                target_date.year, target_date.month, target_date.day,
                target_hour, target_minute
            )

            return int(target_datetime.timestamp())

    async def load_notifications(self):
        """Загружает уведомления для отправки с учетом активности"""
        now = int(time.time())
        db = await create_connect()

        # Закрываем уведомления для прошедших воронку
        await db.execute("""
            UPDATE notifications
            SET is_active = FALSE
            WHERE user_id IN (
                SELECT user_id FROM funnel_passed
                WHERE funnel_name = 'default' AND passed = TRUE
            )
        """)

        # Выбираем активные уведомления
        notifications = await db.fetch("""
            SELECT n.id, n.user_id, n.time_to_send, n.label
            FROM notifications n
            LEFT JOIN users u ON u.id = n.user_id
            WHERE n.is_active = TRUE
            AND COALESCE(u.user_block, FALSE) = FALSE
            AND n.time_to_send < $1
            AND COALESCE(n.pause_reason, '') != 'inactivity'
            AND n.user_id NOT IN (
                SELECT user_id FROM funnel_passed
                WHERE funnel_name = 'default' AND passed = TRUE
            )
        """, now)

        await db.close()
        return notifications

    async def close_notification(self, notification_id=None, user_id=None, label=None, funnel_name: str | None = None):
        """Закрывает уведомление"""
        db = await create_connect()
        if notification_id:
            await db.execute(
                "UPDATE notifications SET is_active = $1 WHERE id = $2",
                False, notification_id
            )
        else:
            await db.execute("""
                UPDATE notifications SET is_active = $1
                WHERE user_id = $2 AND label = $3 AND is_active = $4
            """, False, user_id, label, True)
        await db.close()
        if user_id and label:
            self._drop_notification_funnel(user_id, label)

    async def send_notification(self, notification):
        """Отправляет уведомление пользователю"""
        try:
            user_id = notification.get('user_id')
            label = notification.get('label')
            funnel_name = self._resolve_notification_funnel(notification)
            funnel_map = get_funnel(funnel_name)
            msg_data = funnel_map["callback"].get(label)

            if msg_data is None:
                await logger.error(
                    f"Не найдено сообщение '{label}' для воронки '{funnel_name}', закрываю уведомление"
                )
                await self.close_notification(user_id=user_id, label=label, funnel_name=funnel_name)
                return

            # Проверяем давность уведомления
            if int(time.time()) - notification.get('time_to_send') > 172800:  # 2 дня
                await self.close_notification(
                    notification_id=notification.get('id'),
                    user_id=user_id,
                    label=label,
                    funnel_name=funnel_name,
                )
                await logger.info("Уведомление устарело и закрыто")
                return

            # Обработка actions
            result = False
            if act := (msg_data.get("action") or msg_data.get("actions")):
                if isinstance(act, list):
                    for action in act:
                        result = await run_action(action=action, user_id=user_id, bot=bot)
                elif isinstance(act, dict):
                    result = await run_action(action=act, user_id=user_id, bot=bot)

            if result:
                next_route = _extract_next_route(act)
                if next_route:
                    route = next_route
                    msg_data = funnel_map['callback'].get(route)

            # Сохраняем event
            if event := msg_data.get("event"):
                await save_event(user_id=user_id, event=event)

            if not msg_data:
                await self.close_notification(
                    notification_id=notification.get('id'),
                    user_id=user_id,
                    label=label,
                    funnel_name=funnel_name,
                )
                return

            # Отправляем сообщение
            if msg_data.get("text") or msg_data.get("file"):
                sending = await send_message(
                    bot=self.bot,
                    user_id=user_id,
                    msg_data=msg_data,
                    route=notification.get('label'),
                    funnel_name=funnel_name,
                )

                if sending:
                    # Добавляем новые уведомления если есть
                    if new := msg_data.get("notifications"):
                        await self.add_notifications(
                            user_id=user_id,
                            notifications=new,
                            funnel_name=funnel_name,
                        )

                    await self.close_notification(
                        notification_id=notification.get('id'),
                        user_id=user_id,
                        label=label,
                        funnel_name=funnel_name,
                    )
                    await logger.info(f"Уведомление {notification.get('id')} отправлено")

                    # Сохраняем в историю
                    db = await create_connect()
                    await db.execute("""
                        INSERT INTO user_history (user_id, text)
                        VALUES ($1, LEFT($2, $3))
                    """, user_id, f"Получил уведомление: {notification.get('label')}", MAX_CHARS_USERS_HISTORY)
                    await db.close()

            elif new := msg_data.get("notifications"):
                await self.add_notifications(
                    user_id=user_id,
                    notifications=new,
                    funnel_name=funnel_name,
                )

        except Exception as error:
            await logger.error(f"Ошибка отправки уведомления: {error}")

            if "blocked" in str(error):
                await self.blocked(user_id=user_id, is_blocked=True)

    async def blocked(self, user_id, is_blocked: bool = False):
        """Помечает пользователя как заблокировавшего бота"""
        db = await create_connect()

        action = "заблокировал" if is_blocked else "разблокировал"

        username = await db.fetchrow("SELECT username FROM users WHERE id = $1", user_id)
        username = username.get("username") if username else "unknown"

        await db.execute("""
            INSERT INTO user_history (user_id, text) VALUES ($1, $2)
        """, user_id, f"Пользователь {action} бота!")

        await db.execute("""
            UPDATE users SET user_block = $1 WHERE id = $2
        """, is_blocked, user_id)

        await db.close()

        # Отправляем алерт в Discord
        await self.discord_alert(
            f"🚫 @{bot_info.get_username()}\n"
            f"Пользователь @{username} [id{user_id}] {action} бота!"
        )

        return username

    async def discord_alert(self, text: str):
        """Отправляет уведомление в Discord"""
        try:
            url = f"https://discord.com/api/v9/channels/{ds_channel}/messages"
            payload = {"content": text}
            async with ClientSession() as session:
                async with session.post(url, json=payload, headers=headers) as response:
                    if response.status == 200:
                        await logger.info("Уведомление отправлено в Discord")
                    return response
        except Exception as error:
            await logger.error(f"Ошибка Discord: {error}")
            return None


# Создаём объект для импорта
notificator = SmartNotifier()
