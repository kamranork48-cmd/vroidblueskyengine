import asyncio
import logging
from collections.abc import Callable

from bsky_client import BlueSkyClient
from config import Settings
from database import AccountSettings, BlueskyAccount
from worker import BotWorker
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)


class WorkerManager:
    def __init__(self, base_settings: Settings, session_factory: sessionmaker[Session], publish_log: Callable[[int, str], None]) -> None:
        self.base_settings = base_settings
        self.session_factory = session_factory
        self.publish_log = publish_log
        self.workers: dict[int, BotWorker] = {}

    def _settings_for_account(self, account_settings: AccountSettings) -> Settings:
        return self.base_settings.model_copy(
            update={
                "dry_run": account_settings.dry_run,
                "max_follows_per_day": account_settings.max_follows_per_day,
                "max_followers": account_settings.max_followers,
                "follow_delay_min_seconds": account_settings.follow_delay_min_seconds,
                "follow_delay_max_seconds": account_settings.follow_delay_max_seconds,
                "recent_post_limit": account_settings.recent_post_limit,
                "search_keywords": [item.strip() for item in account_settings.search_keywords.split(",") if item.strip()],
                "artist_filter_enabled": self.base_settings.artist_filter_enabled,
                "exclude_commission_artists": self.base_settings.exclude_commission_artists,
                "artist_keywords": tuple(self.base_settings.artist_keywords),
                "audience_keywords": tuple(self.base_settings.audience_keywords),
            }
        )

    def create_worker(self, account: BlueskyAccount, account_settings: AccountSettings, app_password: str) -> BotWorker:
        worker = BotWorker(
            self._settings_for_account(account_settings),
            self.session_factory,
            BlueSkyClient(account.handle, app_password),
            lambda message, account_id=account.id: self.publish_log(account_id, message),
            account.id,
        )
        self.workers[account.id] = worker
        return worker

    def get(self, account_id: int) -> BotWorker | None:
        return self.workers.get(account_id)

    async def stop_all(self) -> None:
        workers = [worker for worker in self.workers.values() if worker.status in {"running", "paused"}]
        if workers:
            await asyncio.gather(*(worker.stop() for worker in workers))
