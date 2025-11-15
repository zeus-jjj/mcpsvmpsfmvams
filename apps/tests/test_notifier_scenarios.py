import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
import os
import sys
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("BOT_TOKEN", "123456:ABCDEF")

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

from apps.notifier import Notifier


class FakeDB:
    """Very small asyncpg-like stub tailored for notifier tests."""

    def __init__(
        self,
        *,
        notifications: List[Dict[str, Any]] | None = None,
        users: Dict[int, Dict[str, Any]] | None = None,
        funnel_passed: Dict[int, Dict[str, Any]] | None = None,
    ) -> None:
        self.notifications: List[Dict[str, Any]] = notifications or []
        self.users: Dict[int, Dict[str, Any]] = users or {}
        self.funnel_passed: Dict[int, Dict[str, Any]] = funnel_passed or {}
        self.closed = False
        # emulate autoincrement behaviour for ids
        self._next_notification_id = (
            max((item["id"] for item in self.notifications), default=0) + 1
        )

    async def execute(self, query: str, *args: Any) -> None:
        normalized = " ".join(query.split())

        # Обработка запросов для last_activity (НОВОЕ!)
        if "ALTER TABLE users" in normalized and "last_activity" in normalized:
            # Игнорируем ALTER TABLE запросы - в тестах структура таблицы не важна
            return
        elif "UPDATE users SET last_activity" in normalized:
            # Игнорируем обновление last_activity в тестах
            return
        # Существующие обработчики
        elif "INSERT INTO notifications" in normalized:
            await self._handle_insert(normalized, *args)
        elif "UPDATE notifications SET is_active = FALSE WHERE user_id IN" in normalized:
            self._deactivate_funnel_passed()
        elif normalized.startswith("UPDATE notifications SET is_active = $1 WHERE id = $2"):
            self._set_active_by_id(*args)
        elif normalized.startswith("UPDATE notifications SET is_active = $1 WHERE user_id = $2 AND label = $3"):
            self._set_active_by_label(*args)
        elif normalized.startswith("INSERT INTO user_history"):
            # history is irrelevant for the scenarios, but we keep the call no-op
            return
        else:
            raise AssertionError(f"Unsupported query in FakeDB: {query}")

    async def fetch(self, query: str, *args: Any) -> List[Dict[str, Any]]:
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT n.id, n.user_id, n.time_to_send, n.label"):
            now = args[0]
            return [
                {k: v for k, v in item.items() if k in {"id", "user_id", "time_to_send", "label"}}
                for item in self._iter_notifications(now)
            ]
        raise AssertionError(f"Unsupported fetch query in FakeDB: {query}")

    async def fetchrow(self, query: str, *args: Any) -> Dict[str, Any] | None:
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT username FROM users WHERE id = $1"):
            user_id = args[0]
            user = self.users.get(user_id)
            if user is None:
                return None
            return {"username": user.get("username", "unknown")}
        if normalized.startswith(
            "SELECT id, key_text, answer FROM msg_keys WHERE user_id = $1"
        ):
            # Notifier does not rely on msg_keys in these tests
            return None
        raise AssertionError(f"Unsupported fetchrow query in FakeDB: {query}")

    async def close(self) -> None:
        # mark the connection as closed but keep state for subsequent reuse
        self.closed = True

    # Helpers -----------------------------------------------------------------
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

    def _add_notification(
        self, user_id: int, send_time: int, label: str, is_active: bool
    ) -> None:
        self.notifications.append(
            {
                "id": self._next_notification_id,
                "user_id": user_id,
                "time_to_send": send_time,
                "label": label,
                "is_active": is_active,
            }
        )
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

    def _set_active_by_label(
        self, is_active: bool, user_id: int, label: str, current_active: bool
    ) -> None:
        for item in self.notifications:
            if (
                item["user_id"] == user_id
                and item["label"] == label
                and item["is_active"] == current_active
            ):
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

def test_add_notifications_wait_seconds(monkeypatch) -> None:
    async def runner() -> None:
        fake_db = FakeDB()

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = Notifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        base_timestamp = 1_700_000_000
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        await notifier.add_notifications(
            user_id=101,
            notifications=[
                {"message": "welcome", "at_time": {"wait_seconds": 3600}},
            ],
        )

        assert len(fake_db.notifications) == 1
        record = fake_db.notifications[0]
        assert record["user_id"] == 101
        assert record["label"] == "welcome"
        assert record["is_active"] is True
        assert record["time_to_send"] == base_timestamp + 3600

    asyncio.run(runner())


def test_add_notifications_target_datetime(monkeypatch) -> None:
    async def runner() -> None:
        fake_db = FakeDB()

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = Notifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        target = "01.02.2025 12:00"
        expected_timestamp = int(datetime.strptime(target, "%d.%m.%Y %H:%M").timestamp())

        await notifier.add_notifications(
            user_id=202,
            notifications=[
                {"message": "webinar", "at_time": {"target_datetime": target}},
            ],
        )

        record = fake_db.notifications[0]
        assert record["time_to_send"] == expected_timestamp

    asyncio.run(runner())


def test_add_notifications_time_and_delta(monkeypatch) -> None:
    async def runner() -> None:
        fake_db = FakeDB()

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = Notifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        base_dt = datetime(2025, 1, 15, 9, 30)
        base_timestamp = int(base_dt.timestamp())
        current_time = {"value": base_timestamp}
        monkeypatch.setattr("apps.notifier.time.time", lambda: current_time["value"])

        await notifier.add_notifications(
            user_id=303,
            notifications=[
                {
                    "message": "daily-reminder",
                    "at_time": {"time": "10:00", "delta_days": 2},
                }
            ],
        )

        expected_dt = datetime(
            *(base_dt + timedelta(days=2)).timetuple()[:3],
            10,
            0,
        )
        expected_timestamp = int(expected_dt.timestamp())

        record = fake_db.notifications[0]
        assert record["time_to_send"] == expected_timestamp

    asyncio.run(runner())


def test_reusable_notification_updates_existing(monkeypatch) -> None:
    async def runner() -> None:
        base_dt = datetime(2025, 1, 10, 8, 0)
        base_timestamp = int(base_dt.timestamp())
        current_time = {"value": base_timestamp}

        fake_db = FakeDB(
            notifications=[
                {
                    "id": 1,
                    "user_id": 404,
                    "time_to_send": base_timestamp - 100,
                    "label": "reusable",
                    "is_active": False,
                }
            ]
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = Notifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)
        monkeypatch.setattr("apps.notifier.time.time", lambda: current_time["value"])

        await notifier.add_notifications(
            user_id=404,
            notifications=[
                {
                    "message": "reusable",
                    "reusable": True,
                    "at_time": {"wait_seconds": 7200},
                }
            ],
        )

        assert len(fake_db.notifications) == 1
        record = fake_db.notifications[0]
        assert record["is_active"] is True
        assert record["time_to_send"] == base_timestamp + 7200

        # simulate that user returned after 60 days and reminder is rescheduled
        current_time["value"] = base_timestamp + 60 * 24 * 3600

        await notifier.add_notifications(
            user_id=404,
            notifications=[
                {
                    "message": "reusable",
                    "reusable": True,
                    "at_time": {"time": "10:00", "delta_days": 1},
                }
            ],
        )

        updated = fake_db.notifications[0]["time_to_send"]
        expected_dt = datetime(
            *(datetime.fromtimestamp(current_time["value"]) + timedelta(days=1)).timetuple()[:3],
            10,
            0,
        )
        expected_timestamp = int(expected_dt.timestamp())
        assert updated == expected_timestamp

    asyncio.run(runner())


def test_load_notifications_filters_inactive_states(monkeypatch) -> None:
    async def runner() -> None:
        base_dt = datetime(2025, 3, 1, 9, 0)
        base_timestamp = int(base_dt.timestamp())
        monkeypatch.setattr("apps.notifier.time.time", lambda: base_timestamp)

        fake_db = FakeDB(
            notifications=[
                {
                    "id": 1,
                    "user_id": 1,
                    "time_to_send": base_timestamp - 10,
                    "label": "ready",
                    "is_active": True,
                },
                {
                    "id": 2,
                    "user_id": 2,
                    "time_to_send": base_timestamp - 10,
                    "label": "blocked",
                    "is_active": True,
                },
                {
                    "id": 3,
                    "user_id": 3,
                    "time_to_send": base_timestamp + 500,
                    "label": "future",
                    "is_active": True,
                },
                {
                    "id": 4,
                    "user_id": 4,
                    "time_to_send": base_timestamp - 10,
                    "label": "passed",
                    "is_active": True,
                },
            ],
            users={
                1: {"user_block": False},
                2: {"user_block": True},
                3: {"user_block": False},
                4: {"user_block": False},
            },
            funnel_passed={4: {"funnel_name": "default", "passed": True}},
        )

        async def fake_connect() -> FakeDB:
            return fake_db

        notifier = Notifier()
        monkeypatch.setattr("apps.notifier.create_connect", fake_connect)

        notifications = await notifier.load_notifications()
        assert len(notifications) == 1
        assert notifications[0]["user_id"] == 1
        # ensure that funnel-passed notification has been deactivated
        assert fake_db._find_notification(user_id=4, label="passed")["is_active"] is False

    asyncio.run(runner())
