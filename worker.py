import asyncio
import logging
import random
import re
from collections.abc import Callable

from bsky_client import BlueSkyClient, Profile
from config import Settings
from database import BlueskyAccount, count_follows_today, mark_failed, mark_followed, save_discovered_user, target_exists
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)
RELEVANT_TERMS = ("furry", "3d", "2d", "model", "artist", "creator")


class BotWorker:
    def __init__(
        self,
        settings: Settings,
        session_factory: sessionmaker[Session],
        client: BlueSkyClient,
        publish_log: Callable[[str], None],
        account_id: int | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.client = client
        self.publish_log = publish_log
        self.account_id = account_id
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self.status = "stopped"
        self.last_error: str | None = None

    def log(self, message: str, level: int = logging.INFO) -> None:
        logger.log(level, message)
        self.publish_log(message)

    @staticmethod
    def _is_restriction_error(error: Exception) -> bool:
        message = str(error).casefold()
        return any(term in message for term in ("rate limit", "too many requests", "suspended", "forbidden", "unauthorized"))

    async def start(self) -> bool:
        if self._task and not self._task.done():
            return False
        self._stop_event.clear()
        self._pause_event.set()
        self.status = "running"
        self.last_error = None
        self._task = asyncio.create_task(self.run(), name="bsky-bot-worker")
        self.log("Worker started")
        return True

    def pause(self) -> None:
        if self.status == "running":
            self._pause_event.clear()
            self.status = "paused"
            self.log("Worker paused")

    def resume(self) -> None:
        if self.status == "paused":
            self._pause_event.set()
            self.status = "running"
            self.log("Worker resumed")

    async def stop(self) -> None:
        self._stop_event.set()
        self._pause_event.set()
        if self._task:
            await self._task
        self.status = "stopped"
        self.log("Worker stopped")

    async def run(self) -> None:
        try:
            await asyncio.to_thread(self.client.login)
            while not self._stop_event.is_set():
                await self._pause_event.wait()
                if self._stop_event.is_set():
                    break
                await self.scan_once()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = str(exc)
            self.status = "error"
            with self.session_factory() as session:
                account = session.get(BlueskyAccount, self.account_id) if self.account_id is not None else None
                if account is not None:
                    account.status = "error"
                    account.last_error = self.last_error[:500]
                    session.commit()
            self.log(f"Worker error: {exc}", logging.ERROR)
        finally:
            if self.status not in {"error", "paused"}:
                self.status = "stopped"

    async def scan_once(self) -> None:
        with self.session_factory() as session:
            if count_follows_today(session, self.account_id) >= self.settings.max_follows_per_day:
                self.log("Daily follow cap reached; scan paused")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass
                return

        for keyword in self.settings.search_keywords:
            if self._stop_event.is_set():
                return
            await self._pause_event.wait()
            try:
                posts = await asyncio.to_thread(self.client.search_posts, keyword, 25)
                self.log(f"Found {len(posts)} posts for '{keyword}'")
            except Exception as exc:
                self.log(f"Search failed for '{keyword}': {exc}", logging.WARNING)
                continue

            for post in posts:
                if self._stop_event.is_set():
                    return
                await self._inspect_likers(post, keyword)

    async def import_post_likers(self, post_url: str) -> int:
        post_uri = await asyncio.to_thread(self.client.post_uri_from_url, post_url)
        likers = await asyncio.to_thread(self.client.post_likers, post_uri, 100)
        matched = 0
        for like in likers:
            if self._stop_event.is_set():
                break
            actor = getattr(like, "actor", None)
            if await self._inspect_actor(actor, "manual post"):
                matched += 1
        self.log(f"Imported {len(likers)} likers and matched {matched} profiles")
        return matched

    async def _inspect_likers(self, post: object, keyword: str) -> None:
        uri = getattr(post, "uri", None)
        if not uri:
            return
        try:
            likers = await asyncio.to_thread(self.client.post_likers, uri, 100)
        except Exception as exc:
            self.log(f"Could not fetch likers for post: {exc}", logging.WARNING)
            return

        for like in likers:
            if self._stop_event.is_set():
                return
            await self._pause_event.wait()
            actor = getattr(like, "actor", None)
            await self._inspect_actor(actor, keyword)

    async def _inspect_actor(self, actor: object, keyword: str) -> bool:
            handle = getattr(actor, "handle", None)
            did = getattr(actor, "did", None)
            if not handle or not did:
                return False
            with self.session_factory() as session:
                if target_exists(session, did, handle, self.account_id):
                    return False
            try:
                profile = await asyncio.to_thread(self.client.get_profile, handle)
                if not await self._matches(profile, keyword):
                    return False
                with self.session_factory() as session:
                    save_discovered_user(
                        session,
                        account_id=self.account_id,
                        handle=profile.handle,
                        did=profile.did,
                        follower_count=profile.followers_count,
                        matched_keyword=keyword,
                    )
                self.log(f"Matched @{profile.handle} ({profile.followers_count} followers)")
                await self._follow_if_allowed(profile)
                return True
            except Exception as exc:
                self.log(f"Profile inspection failed for @{handle}: {exc}", logging.WARNING)
                return False

    async def _matches(self, profile: Profile, keyword: str) -> bool:
        if profile.followers_count >= self.settings.max_followers:
            return False
        text = f"{profile.bio} {profile.display_name}"
        if not self._contains_relevant_term(text):
            recent = await asyncio.to_thread(self.client.recent_post_text, profile.handle, self.settings.recent_post_limit)
            text = f"{text} {recent}"
        return self._contains_relevant_term(text)

    @staticmethod
    def _contains_relevant_term(text: str) -> bool:
        normalized = text.casefold()
        return any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in RELEVANT_TERMS)

    async def _follow_if_allowed(self, profile: Profile) -> None:
        with self.session_factory() as session:
            if count_follows_today(session, self.account_id) >= self.settings.max_follows_per_day:
                self.log("Daily follow cap reached; leaving matched profile discovered")
                return
        if self.settings.dry_run:
            self.log(f"DRY RUN: would follow @{profile.handle}")
            return
        try:
            record_uri = await asyncio.to_thread(self.client.follow, profile.did)
            with self.session_factory() as session:
                mark_followed(session, profile.did, self.account_id)
            self.log(f"Followed @{profile.handle} ({record_uri})")
            delay = random.randint(self.settings.follow_delay_min_seconds, self.settings.follow_delay_max_seconds)
            self.log(f"Waiting {delay}s before the next follow")
            await asyncio.sleep(delay)
        except Exception as exc:
            with self.session_factory() as session:
                mark_failed(session, profile.did, "follow_failed", self.account_id)
            if self._is_restriction_error(exc):
                self._pause_event.clear()
                self.status = "paused"
                self.log("Account paused after a rate-limit or restriction response; review it before resuming", logging.ERROR)
            self.log(f"Follow failed for @{profile.handle}: {exc}", logging.ERROR)
