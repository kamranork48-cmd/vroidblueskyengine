import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from config import Settings
from database import AuditEvent, Base, BlueskyAccount, User, create_database
from safety import (
    classify_bluesky_error,
    get_global_control,
    is_global_emergency_stop_active,
    set_global_emergency_stop,
)
from worker import BotWorker


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.response = SimpleNamespace(
            status_code=status_code,
            content=SimpleNamespace(error=code, message=message),
            headers=headers or {},
        )


class SafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_classifies_nested_takedown_response(self) -> None:
        wrapped = RuntimeError("BlueSky login failed")
        wrapped.__cause__ = ApiError(401, "AccountTakedown", "Account has been taken down")

        result = classify_bluesky_error(wrapped)

        self.assertEqual(result.category, "suspended")
        self.assertEqual(result.code, "AccountTakedown")

    def test_captures_rate_limit_retry_after(self) -> None:
        result = classify_bluesky_error(
            ApiError(429, "RateLimitExceeded", "Too many requests", {"retry-after": "86400"})
        )

        self.assertEqual(result.category, "rate_limited")
        self.assertEqual(result.retry_after, "86400")

    def test_captures_rate_limit_reset_when_retry_after_is_missing(self) -> None:
        result = classify_bluesky_error(
            ApiError(429, "RateLimitExceeded", "Too many requests", {"ratelimit-reset": "1780000000"})
        )

        self.assertEqual(result.category, "rate_limited")
        self.assertEqual(result.retry_after, "")
        self.assertEqual(result.rate_limit_reset, "1780000000")

    def test_emergency_stop_persists_and_records_actor(self) -> None:
        state = set_global_emergency_stop(
            self.session_factory,
            True,
            "Operator requested a pause",
            actor_user_id=7,
            code="manual_emergency_stop",
        )

        self.assertTrue(state["active"])
        self.assertTrue(is_global_emergency_stop_active(self.session_factory))
        self.assertEqual(get_global_control(self.session_factory)["reason"], "Operator requested a pause")
        with self.session_factory() as session:
            event = session.scalar(select(AuditEvent))
            self.assertIsNotNone(event)
            self.assertEqual(event.actor_user_id, 7)
            self.assertEqual(event.event_type, "emergency_stop_activated")

    def test_worker_cannot_start_during_emergency_stop(self) -> None:
        set_global_emergency_stop(self.session_factory, True, "Test stop")
        worker = BotWorker(
            Settings(_env_file=None),
            self.session_factory,
            SimpleNamespace(handle="example.bsky.social"),
            lambda _: None,
        )

        self.assertFalse(asyncio.run(worker.start()))
        self.assertEqual(worker.status, "stopped")

    def test_takedown_activates_global_stop_and_marks_account(self) -> None:
        with self.session_factory() as session:
            user = User(username="operator", password_hash="hash")
            session.add(user)
            session.flush()
            account = BlueskyAccount(
                handle="example.bsky.social",
                encrypted_app_password="encrypted",
                created_by_user_id=user.id,
            )
            session.add(account)
            session.commit()
            account_id = account.id

        worker = BotWorker(
            Settings(_env_file=None),
            self.session_factory,
            SimpleNamespace(handle="example.bsky.social"),
            lambda _: None,
            account_id,
        )
        worker._handle_api_error(
            ApiError(401, "AccountTakedown", "Account has been taken down"),
            "login",
        )

        self.assertTrue(is_global_emergency_stop_active(self.session_factory))
        with self.session_factory() as session:
            account = session.get(BlueskyAccount, account_id)
            self.assertEqual(account.status, "suspended")
            events = list(session.scalars(select(AuditEvent).where(AuditEvent.account_id == account_id)))
            self.assertEqual({event.event_type for event in events}, {"api_error", "emergency_stop_activated"})
            api_error = next(event for event in events if event.event_type == "api_error")
            self.assertEqual(api_error.category, "suspended")
            self.assertEqual(api_error.http_status, 401)
            self.assertEqual(api_error.account_owner_user_id, user.id)

    def test_existing_audit_table_is_migrated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite"
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    """
                    CREATE TABLE audit_events (
                        id INTEGER PRIMARY KEY,
                        account_id INTEGER,
                        account_handle VARCHAR(255) DEFAULT '',
                        actor_user_id INTEGER,
                        event_type VARCHAR(80) NOT NULL,
                        severity VARCHAR(16) DEFAULT 'info',
                        code VARCHAR(100) DEFAULT '',
                        message VARCHAR(1000) DEFAULT '',
                        created_at DATETIME NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE bluesky_accounts (
                        id INTEGER PRIMARY KEY,
                        handle VARCHAR(255) NOT NULL,
                        did VARCHAR(255),
                        encrypted_app_password VARCHAR(1024) NOT NULL,
                        status VARCHAR(32) DEFAULT 'stopped',
                        last_error VARCHAR(500),
                        created_by_user_id INTEGER NOT NULL,
                        created_at DATETIME NOT NULL
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO audit_events (id, event_type, created_at) VALUES (1, 'legacy', '2026-01-01')"
                )
                connection.commit()
            finally:
                connection.close()
            engine, migrated_session_factory = create_database(
                Settings(_env_file=None, database_url=f"sqlite:///{path.as_posix()}")
            )
            try:
                with migrated_session_factory() as session:
                    event = session.get(AuditEvent, 1)
                    self.assertIsNotNone(event)
                    self.assertEqual(event.category, "")
                    self.assertIsNone(event.http_status)
                    self.assertEqual(event.retry_after, "")
                    account_columns = {column["name"] for column in inspect(engine).get_columns("bluesky_accounts")}
                    self.assertIn("chat_sync_status", account_columns)
                    self.assertIn("chat_sync_retry_at", account_columns)
            finally:
                engine.dispose()


if __name__ == "__main__":
    unittest.main()
