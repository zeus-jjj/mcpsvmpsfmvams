"""
Тесты для проверки логики догревочных (warmup) уведомлений
"""
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
import os
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("BOT_TOKEN", "123456:ABCDEF")

# Стабы для модулей (как в оригинальном файле)
asyncpg_stub = types.ModuleType("asyncpg")
async def _connect_stub(*args, **kwargs):
    raise AssertionError("asyncpg.connect should be patched in tests")
asyncpg_stub.connect = _connect_stub
sys.modules.setdefault("asyncpg", asyncpg_stub)

funcs_stub = types.ModuleType("apps.funcs")
async def _return_true(*args, **kwargs):
    return True
async def _return_false(*args, **kwargs):
    return False
funcs_stub.send_message = _return_true
funcs_stub.run_action = _return_false
funcs_stub.save_event = _return_true
funcs_stub.close_old_notifications = _return_true
sys.modules.setdefault("apps.funcs", funcs_stub)

logger_stub = types.ModuleType("apps.logger")
async def _log_stub(*args, **kwargs):
    return None
logger_stub.info = _log_stub
logger_stub.error = _log_stub
logger_stub.debug = _log_stub
sys.modules.setdefault("apps.logger", logger_stub)

from apps.notifier import SmartNotifier


class FakeDB:
    """Расширенная версия FakeDB с поддержкой проверок для dogrev"""

    def __init__(
        self,
        *,
        notifications: List[Dict[str, Any]] | None = None,
        users: Dict[int, Dict[str, Any]] | None = None,
        funnel_passed: Dict[int, Dict[str, Any]] | None = None,
        events: Dict[int, List[str]] | None = None,  # НОВОЕ: события пользователей
        user_funnel: Dict[int, List[str]] | None = None,  # НОВОЕ: воронка пользователей
    ) -> None:
        self.notifications: List[Dict[str, Any]] = notifications or []
        self.users: Dict[int, Dict[str, Any]] = users or {}
        self.funnel_passed: Dict[int, Dict[str, Any]] = funnel_passed or {}
        self.events: Dict[int, List[str]] = events or {}  # {user_id: [event_type, ...]}
        self.user_funnel: Dict[int, List[str]] = user_funnel or {}  # {user_id: [label, ...]}
        self.closed = False
        self._next_notification_id = (
            max((item["id"] for item in self.notifications), default=0) + 1
        )

    async def execute(self, query: str, *args: Any) -> None:
        normalized = " ".join(query.split())

        if "ALTER TABLE users" in normalized and "last_activity" in normalized:
            return
        elif "UPDATE users SET last_activity" in normalized:
            return
        elif "INSERT INTO notifications" in normalized:
            await self._handle_insert(normalized, *args)
        elif "UPDATE notifications SET is_active = FALSE WHERE user_id IN" in normalized:
            self._deactivate_funnel_passed()
        elif normalized.startswith("UPDATE notifications SET is_active = $1 WHERE id = $2"):
            self._set_active_by_id(*args)
        elif normalized.startswith("UPDATE notifications SET is_active = $1 WHERE user_id = $2 AND label = $3"):
            self._set_active_by_label(*args)
        elif normalized.startswith("INSERT INTO user_history"):
            return
        # НОВОЕ: Обработка UPDATE с перерасчётом времени (для одного уведомления)
        elif "UPDATE notifications SET is_active = true" in normalized and "time_to_send = $1" in normalized and "WHERE id = $2" in normalized:
            new_time = args[0]
            notification_id = args[1]  # Одно ID, не список
            for item in self.notifications:
                if item["id"] == notification_id:
                    item["is_active"] = True
                    item["time_to_send"] = new_time
                    break
            return
        # НОВОЕ: Обработка простой активации без изменения времени
        elif "UPDATE notifications SET is_active = TRUE" in normalized and "WHERE id IN" in normalized:
            # Массовая активация уведомлений без изменения времени
            notification_ids = args[0]  # Список ID
            if isinstance(notification_ids, (list, tuple)):
                for notif_id in notification_ids:
                    self._set_active_by_id(True, notif_id)
            else:
                self._set_active_by_id(True, notification_ids)
            return
        else:
            raise AssertionError(f"Unsupported query in FakeDB: {query}")

    async def fetch(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        normalized = " ".join(query.split())

        # Загрузка уведомлений для отправки
        if normalized.startswith("SELECT n.id, n.user_id, n.time_to_send, n.label"):
            now = args[0]
            return [
                {k: v for k, v in item.items() if k in {"id", "user_id", "time_to_send", "label"}}
                for item in self._iter_notifications(now)
            ]

        # НОВОЕ: Получение приостановленных уведомлений для возобновления
        if "SELECT id, label, time_to_send FROM notifications" in normalized and "pause_reason = 'inactivity'" in normalized:
            user_id = args[0]
            paused = [
                {"id": n["id"], "label": n["label"], "time_to_send": n["time_to_send"]}
                for n in self.notifications
                if n["user_id"] == user_id and not n.get("is_active", True)
            ]
            return paused

        raise AssertionError(f"Unsupported fetch query in FakeDB: {query}")

    async def fetchrow(self, query: str, *args: Any) -> Dict[str, Any] | None:
        normalized = " ".join(query.split())

        # Запрос статуса пользователя для add_notifications
        if "SELECT u.timestamp_registration" in normalized:
            user_id = args[0]
            if user_id not in self.users:
                return None

            # Проверяем регистрацию
            is_registered = 1 if 'course_registration' in self.events.get(user_id, []) else 0

            # Проверяем нахождение в воронке курса
            user_labels = self.user_funnel.get(user_id, [])
            in_course_funnel = 1 if any(
                'course' in label or 'spin' in label or 'mtt' in label or 'cash' in label
                for label in user_labels
            ) else 0

            return {
                "timestamp_registration": datetime.now(),
                "is_registered": is_registered,
                "in_course_funnel": in_course_funnel,
                "last_activity": datetime.now()
            }

        # НОВОЕ: Проверка COUNT(*) для warmup уведомлений (_add_warmup_notifications)
        if "SELECT COUNT(*) as cnt FROM notifications" in normalized and "warmup_" in normalized:
            user_id = args[0]
            cnt = sum(
                1 for n in self.notifications
                if n['user_id'] == user_id and n['label'].startswith('warmup_') and n['is_active']
            )
            return {"cnt": cnt}

        # НОВОЕ: Проверка COUNT(*) для регистрации (_add_warmup_notifications)
        if "SELECT COUNT(*) as cnt FROM events" in normalized and "course_registration" in normalized:
            user_id = args[0]
            cnt = 1 if 'course_registration' in self.events.get(user_id, []) else 0
            return {"cnt": cnt}

        # НОВОЕ: Проверка COUNT(*) для воронки (_add_warmup_notifications)
        if "SELECT COUNT(*) as cnt FROM user_funnel" in normalized:
            user_id = args[0]
            user_labels = self.user_funnel.get(user_id, [])
            cnt = 1 if any(
                'course' in label or 'spin' in label or 'mtt' in label or 'cash' in label
                for label in user_labels
            ) else 0
            return {"cnt": cnt}

        # Проверка существующих warmup уведомлений (старый вариант для совместимости)
        if "EXISTS(SELECT 1 FROM notifications" in normalized and "warmup_" in normalized:
            user_id = args[0]
            has_warmup = any(
                n['user_id'] == user_id and n['label'].startswith('warmup_') and n['is_active']
                for n in self.notifications
            )
            is_registered = 'course_registration' in self.events.get(user_id, [])
            user_labels = self.user_funnel.get(user_id, [])
            in_funnel = any(
                'course' in label or 'spin' in label or 'mtt' in label or 'cash' in label
                for label in user_labels
            )

            return {
                "has_warmup": has_warmup,
                "is_registered": is_registered,
                "in_funnel": in_funnel
            }

        if normalized.startswith("SELECT username FROM users WHERE id = $1"):
            user_id = args[0]
            user = self.users.get(user_id)
            if user is None:
                return None
            return {"username": user.get("username", "unknown")}

        if normalized.startswith("SELECT id, key_text, answer FROM msg_keys WHERE user_id = $1"):
            return None

        raise AssertionError(f"Unsupported fetchrow query in FakeDB: {query}")

    async def close(self) -> None:
        self.closed = True

    # Helpers
    async def _handle_insert(self, query: str, *args: Any) -> None:
        user_id, send_time, label, is_active = args
        send_time = int(send_time)
        existing = self._find_notification(user_id=user_id, label=label)
        if "DO UPDATE" in query:
            if existing:
                existing.update(time_to_send=send_time, is_active=is_active)
            else:
                self._add_notification(user_id, send_time, label, is_active)
        else:
            if not existing:
                self._add_notification(user_id, send_time, label, is_active)

    def _add_notification(self, user_id: int, send_time: int, label: str, is_active: bool) -> None:
        self.notifications.append({
            "id": self._next_notification_id,
            "user_id": user_id,
            "time_to_send": send_time,
            "label": label,
            "is_active": is_active,
        })
        self._next_notification_id += 1

    def _find_notification(self, *, user_id: int, label: str) -> Dict[str, Any] | None:
        for item in self.notifications:
            if item["user_id"] == user_id and item["label"] == label:
                return item
        return None

    def _deactivate_funnel_passed(self) -> None:
        passed_users = {
            user_id
            for user_id, info in self.funnel_passed.items()
            if info.get("funnel_name") == "default" and info.get("passed") is True
        }
        for item in self.notifications:
            if item["user_id"] in passed_users:
                item["is_active"] = False

    def _set_active_by_id(self, is_active: bool, notification_id: int) -> None:
        for item in self.notifications:
            if item["id"] == notification_id:
                item["is_active"] = is_active
                break

    def _set_active_by_label(self, is_active: bool, user_id: int, label: str, current_active: bool) -> None:
        for item in self.notifications:
            if (item["user_id"] == user_id and item["label"] == label
                and item["is_active"] == current_active):
                item["is_active"] = is_active

    def _iter_notifications(self, now: int) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for item in self.notifications:
            if not item.get("is_active", False):
                continue
            if item["time_to_send"] >= now:
                continue
            user = self.users.get(item["user_id"], {"user_block": False})
            if user.get("user_block"):
                continue
            funnel = self.funnel_passed.get(item["user_id"])
            if funnel and funnel.get("funnel_name") == "default" and funnel.get("passed"):
                continue
            results.append(item)
        return results


# ============================================================================
# ТЕСТЫ ДЛЯ ДОГРЕВОЧНЫХ УВЕДОМЛЕНИЙ
# ============================================================================

def test_warmup_adds_for_new_unregistered_user(monkeypatch) -> None:
    """
    Тест 1: Новому незарегистрированному пользователю вне воронки
    должны добавиться warmup уведомления
    """
    async def runner() -> None:
        fake_db = FakeDB(
            users={500: {"username": "newuser"}},
            events={},  # НЕТ регистрации
            user_funnel={}  # НЕТ в воронке
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = SmartNotifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        base_timestamp = 1_700_000_000
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        # Добавляем обычное уведомление
        await notifier.add_notifications(
            user_id=500,
            notifications=[
                {"message": "welcome", "at_time": {"wait_seconds": 3600}},
            ],
        )

        # Проверяем: должны быть warmup + обычное уведомление
        warmup_notifications = [n for n in fake_db.notifications if n['label'].startswith('warmup_')]
        regular_notifications = [n for n in fake_db.notifications if not n['label'].startswith('warmup_')]

        assert len(warmup_notifications) == 5, f"Должно быть 5 warmup, получили {len(warmup_notifications)}"
        assert len(regular_notifications) == 1, f"Должно быть 1 обычное, получили {len(regular_notifications)}"
        assert regular_notifications[0]['label'] == 'welcome'

        # Проверяем наличие всех warmup сообщений
        warmup_labels = [n['label'] for n in warmup_notifications]
        expected_warmup = ['warmup_why_poker', 'warmup_success_stories',
                          'warmup_free_course', 'warmup_last_chance', 'warmup_special_offer']
        for expected in expected_warmup:
            assert expected in warmup_labels, f"Отсутствует {expected}"

    asyncio.run(runner())
    print("✅ Тест 1: Догрев добавляется для новых пользователей")


def test_warmup_not_added_for_registered_user(monkeypatch) -> None:
    """
    Тест 2: Зарегистрированному пользователю НЕ должны добавляться warmup,
    но обычные уведомления должны добавляться
    """
    async def runner() -> None:
        fake_db = FakeDB(
            users={501: {"username": "registered_user"}},
            events={501: ['course_registration']},  # ЕСТЬ регистрация
            user_funnel={501: ['spin']}  # В воронке
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = SmartNotifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        base_timestamp = 1_700_000_000
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        await notifier.add_notifications(
            user_id=501,
            notifications=[
                {"message": "course_manager", "at_time": {"wait_seconds": 1800}},
                {"message": "notif_stat", "at_time": {"time": "10:00", "delta_days": 2}},
            ],
        )

        # Проверяем: НЕ должно быть warmup, но должны быть обычные
        warmup_notifications = [n for n in fake_db.notifications if n['label'].startswith('warmup_')]
        regular_notifications = [n for n in fake_db.notifications if not n['label'].startswith('warmup_')]

        assert len(warmup_notifications) == 0, f"Не должно быть warmup, получили {len(warmup_notifications)}"
        assert len(regular_notifications) == 2, f"Должно быть 2 обычных, получили {len(regular_notifications)}"

        # Проверяем метки обычных уведомлений
        labels = [n['label'] for n in regular_notifications]
        assert 'course_manager' in labels
        assert 'notif_stat' in labels

    asyncio.run(runner())
    print("✅ Тест 2: Догрев НЕ добавляется зарегистрированным")


def test_warmup_not_added_for_user_in_funnel(monkeypatch) -> None:
    """
    Тест 3: Незарегистрированному пользователю В воронке НЕ должны
    добавляться warmup, но обычные уведомления должны добавляться
    """
    async def runner() -> None:
        fake_db = FakeDB(
            users={502: {"username": "in_funnel_user"}},
            events={},  # НЕТ регистрации
            user_funnel={502: ['free_learning', 'spin']}  # В воронке курса
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = SmartNotifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        base_timestamp = 1_700_000_000
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        await notifier.add_notifications(
            user_id=502,
            notifications=[
                {"message": "notif_cousre_1", "at_time": {"time": "10:00", "delta_days": 1}},
            ],
        )

        # Проверяем: НЕ должно быть warmup, но должно быть обычное
        warmup_notifications = [n for n in fake_db.notifications if n['label'].startswith('warmup_')]
        regular_notifications = [n for n in fake_db.notifications if not n['label'].startswith('warmup_')]

        assert len(warmup_notifications) == 0, f"Не должно быть warmup, получили {len(warmup_notifications)}"
        assert len(regular_notifications) == 1, f"Должно быть 1 обычное, получили {len(regular_notifications)}"
        assert regular_notifications[0]['label'] == 'notif_cousre_1'

    asyncio.run(runner())
    print("✅ Тест 3: Догрев НЕ добавляется пользователям в воронке")


def test_warmup_not_duplicated(monkeypatch) -> None:
    """
    Тест 4: Если у пользователя уже есть активные warmup уведомления,
    они НЕ должны добавляться повторно
    """
    async def runner() -> None:
        base_timestamp = 1_700_000_000

        fake_db = FakeDB(
            users={503: {"username": "has_warmup"}},
            events={},  # НЕТ регистрации
            user_funnel={},  # НЕТ в воронке
            notifications=[
                {
                    "id": 1,
                    "user_id": 503,
                    "time_to_send": base_timestamp + 86400,
                    "label": "warmup_why_poker",
                    "is_active": True,
                }
            ]
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = SmartNotifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        await notifier.add_notifications(
            user_id=503,
            notifications=[
                {"message": "test", "at_time": {"wait_seconds": 3600}},
            ],
        )

        # Проверяем: должно остаться только 1 warmup (существующее) + 1 обычное
        warmup_notifications = [n for n in fake_db.notifications if n['label'].startswith('warmup_')]

        assert len(warmup_notifications) == 1, f"Должен быть 1 warmup (существующий), получили {len(warmup_notifications)}"
        assert warmup_notifications[0]['id'] == 1, "Должен остаться существующий warmup"

    asyncio.run(runner())
    print("✅ Тест 4: Догрев НЕ дублируется")


def test_warmup_skipped_when_user_registers(monkeypatch) -> None:
    """
    Тест 5: При возобновлении уведомлений после активности,
    warmup должны пропускаться для зарегистрированных пользователей
    """
    async def runner() -> None:
        base_timestamp = 1_700_000_000

        fake_db = FakeDB(
            users={504: {"username": "registered_later"}},
            events={504: ['course_registration']},  # Зарегистрировался
            user_funnel={504: ['spin']},
            notifications=[
                {
                    "id": 10,
                    "user_id": 504,
                    "time_to_send": base_timestamp - 3600,
                    "label": "warmup_why_poker",
                    "is_active": False,  # Был на паузе
                },
                {
                    "id": 11,
                    "user_id": 504,
                    "time_to_send": base_timestamp - 1800,
                    "label": "course_manager",
                    "is_active": False,  # Был на паузе
                }
            ]
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = SmartNotifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        # Имитируем возобновление уведомлений
        await notifier.resume_user_notifications(504)

        # Проверяем: warmup должен остаться неактивным, обычное должно активироваться
        warmup = fake_db._find_notification(user_id=504, label="warmup_why_poker")
        regular = fake_db._find_notification(user_id=504, label="course_manager")

        assert warmup['is_active'] is False, "Warmup должен остаться неактивным"
        assert regular['is_active'] is True, "Обычное уведомление должно активироваться"

    asyncio.run(runner())
    print("✅ Тест 5: Догрев пропускается при возобновлении для зарегистрированных")


# ============================================================================
# ЗАПУСК ВСЕХ ТЕСТОВ
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("ТЕСТЫ ЛОГИКИ ДОГРЕВОЧНЫХ (WARMUP) УВЕДОМЛЕНИЙ")
    print("="*70 + "\n")

    import pytest

    # Используем monkeypatch из pytest
    test_warmup_adds_for_new_unregistered_user(pytest.MonkeyPatch())
    test_warmup_not_added_for_registered_user(pytest.MonkeyPatch())
    test_warmup_not_added_for_user_in_funnel(pytest.MonkeyPatch())
    test_warmup_not_duplicated(pytest.MonkeyPatch())
    test_warmup_skipped_when_user_registers(pytest.MonkeyPatch())

    print("\n" + "="*70)
    print("ВСЕ ТЕСТЫ ДОГРЕВА ПРОЙДЕНЫ УСПЕШНО ✅")
    print("="*70 + "\n")
