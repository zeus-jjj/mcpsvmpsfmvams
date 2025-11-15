import os
import sys
import types
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("BOT_TOKEN", "000000:TESTTOKEN")
os.environ.setdefault("CRYPT_KEY", "a" * 32)

if "asyncpg" not in sys.modules:
    asyncpg_stub = types.ModuleType("asyncpg")

    async def _connect_stub(*args, **kwargs):  # pragma: no cover - не вызывается в тестах
        raise RuntimeError("asyncpg.connect should not be called during tests")

    asyncpg_stub.connect = _connect_stub
    sys.modules["asyncpg"] = asyncpg_stub

if "cryptography" not in sys.modules:
    cryptography_stub = types.ModuleType("cryptography")
    fernet_stub = types.ModuleType("cryptography.fernet")

    class _DummyFernet:  # pragma: no cover - заглушка для тестов
        def __init__(self, *args, **kwargs):
            pass

        def encrypt(self, message):
            return b"test"

    fernet_stub.Fernet = _DummyFernet
    sys.modules["cryptography"] = cryptography_stub
    sys.modules["cryptography.fernet"] = fernet_stub

from apps.notifier import Notifier


class FakeDB:
    def __init__(self):
        self.calls = []

    async def execute(self, query, *params):
        self.calls.append((" ".join(query.split()), params))


class NotifierProcessingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.notifier = Notifier()

    async def test_filters_out_dogrev_notifications(self):
        db = FakeDB()
        raw_notifications = [
            {"id": 1, "user_id": 100, "time_to_send": 10, "label": "foo", "is_dogrev": False},
            {"id": 2, "user_id": 200, "time_to_send": 20, "label": "bar", "is_dogrev": True},
        ]

        result = await self.notifier._process_loaded_notifications(db, raw_notifications)

        self.assertEqual(result, [{"id": 1, "user_id": 100, "time_to_send": 10, "label": "foo"}])
        self.assertEqual(db.calls, [])
        self.assertEqual(self.notifier._dogrev_users_cache, {200})

    async def test_reactivates_expired_notifications_when_user_leaves_dogrev(self):
        db = FakeDB()
        # Первичный проход — пользователь в догреве, уведомление откладывается
        await self.notifier._process_loaded_notifications(
            db,
            [{"id": 3, "user_id": 300, "time_to_send": 100, "label": "foo", "is_dogrev": True}],
        )

        with patch("apps.notifier.time.time", return_value=500):
            result = await self.notifier._process_loaded_notifications(
                db,
                [{"id": 3, "user_id": 300, "time_to_send": 100, "label": "foo", "is_dogrev": False}],
            )

        self.assertEqual(result, [{"id": 3, "user_id": 300, "time_to_send": 100, "label": "foo"}])
        self.assertEqual(len(db.calls), 1)
        query, params = db.calls[0]
        self.assertIn("UPDATE notifications".upper(), query.upper())
        self.assertIn("time_to_send = $2", query)
        self.assertEqual(params, ([3], 500))
        self.assertNotIn(300, self.notifier._paused_notifications)

    async def test_does_not_reschedule_future_notifications(self):
        db = FakeDB()
        await self.notifier._process_loaded_notifications(
            db,
            [{"id": 4, "user_id": 400, "time_to_send": 600, "label": "foo", "is_dogrev": True}],
        )

        with patch("apps.notifier.time.time", return_value=500):
            await self.notifier._process_loaded_notifications(
                db,
                [{"id": 4, "user_id": 400, "time_to_send": 600, "label": "foo", "is_dogrev": False}],
            )

        # Ожидаем один вызов — на активацию без изменения времени
        self.assertEqual(len(db.calls), 1)
        query, params = db.calls[0]
        self.assertIn("SET is_active = TRUE", query)
        self.assertNotIn("time_to_send", query)
        self.assertEqual(params, ([4],))
        self.assertNotIn(400, self.notifier._paused_notifications)


if __name__ == "__main__":
    unittest.main()
