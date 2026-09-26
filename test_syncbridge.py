import tempfile
import unittest

from syncbridge import ChangeEvent, MemoryTarget, SQLiteStore, SyncEngine


class SyncEngineTests(unittest.TestCase):
    def setUp(self):
        self.target = MemoryTarget()
        self.engine = SyncEngine(self.target)

    def test_upsert_is_applied_and_duplicate_is_ignored(self):
        event = ChangeEvent("users", "upsert", {"id": 1}, {"name": "Sara"}, event_id="fixed")
        self.assertTrue(self.engine.submit(event))
        self.assertFalse(self.engine.submit(event))
        self.assertEqual(self.target.rows["users"]['{"id": 1}']["name"], "Sara")
        self.assertEqual(self.engine.snapshot()["stats"]["duplicates"], 1)

    def test_delete_removes_existing_row(self):
        self.engine.submit(ChangeEvent("users", "insert", {"id": 2}, {"name": "Ali"}))
        self.engine.submit(ChangeEvent("users", "delete", {"id": 2}))
        self.assertEqual(self.target.rows["users"], {})

    def test_invalid_operation_is_recorded_as_failure(self):
        self.assertFalse(self.engine.submit(ChangeEvent("users", "merge", {"id": 3})))
        status = self.engine.snapshot()
        self.assertEqual(status["stats"]["failed"], 1)
        self.assertEqual(len(status["failed"]), 1)

    def test_changes_are_available_after_a_cursor(self):
        self.engine.submit(ChangeEvent("tasks", "insert", {"id": 1}, {"title": "One"}))
        self.engine.submit(ChangeEvent("tasks", "insert", {"id": 2}, {"title": "Two"}))
        changes = self.engine.changes_since(1)
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["key"], {"id": 2})

    def test_users_are_isolated_in_persistent_store(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as file:
            store = SQLiteStore(file.name)
            token_a = store.register("a@example.com", "password-a")
            token_b = store.register("b@example.com", "password-b")
            user_a, user_b = store.user_for_token(token_a), store.user_for_token(token_b)
            self.assertNotEqual(user_a, user_b)
            store.apply(ChangeEvent("tasks", "upsert", {"id": 1}, {"title": "A"}, user_id=user_a))
            store.apply(ChangeEvent("tasks", "upsert", {"id": 1}, {"title": "B"}, user_id=user_b))
            self.assertEqual(store.changes_since(user_a, 0)[0]["data"]["title"], "A")
            self.assertEqual(store.changes_since(user_b, 0)[0]["data"]["title"], "B")


if __name__ == "__main__":
    unittest.main()
