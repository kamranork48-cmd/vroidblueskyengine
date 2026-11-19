import asyncio
import logging
import random
import re
from collections.abc import Callable

from artist_filter import assess_profile
from bsky_client import BlueSkyClient, Profile
from config import Settings
from database import (
    BlueskyAccount,
    count_follows_today,
    mark_failed,
    mark_followed,
    save_discovered_user,
    target_exists,
    update_account_status,
)
from safety import (
    classify_bluesky_error,
    is_global_emergency_stop_active,
    record_audit_event,
    set_global_emergency_stop,
)
from sqlalchemy.orm import Session, sessionmaker

logger = logging.getLogger(__name__)
RELEVANT_TERMS = ("furry", "twitch", "vtuber", "pngtuber", "envtuber", "dnd character", "mascot", "logo", "3d", "2d", "anime character",)


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

    def _handle_api_error(self, error: Exception, operation: str) -> None:
        info = classify_bluesky_error(error)
        if info.category == "not_found":
            return
        if info.category == "transient" and operation != "worker":
            self.log(f"Transient {operation} failure: {info.message}", logging.WARNING)
            return
        details = f"{operation}: {info.message}"
        if info.retry_after:
            details += f" (Retry-After: {info.retry_after})"
        severity = "critical" if info.category == "suspended" else "warning"
        record_audit_event(
            self.session_factory,
            "api_error",
            details,
            account_id=self.account_id,
            account_handle=self.client.handle,
            severity=severity,
            category=info.category,
            code=info.code or info.category,
            http_status=info.status_code,
            retry_after=info.retry_after,
        )

        if info.category == "suspended":
            self.status = "suspended"
            self.last_error = details[:500]
            self._stop_event.set()
            self._pause_event.set()
            with self.session_factory() as session:
                if self.account_id is not None:
                    update_account_status(session, self.account_id, "suspended", self.last_error)
            set_global_emergency_stop(
                self.session_factory,
                True,
                f"Bluesky reported @{self.client.handle} as taken down. Human review is required.",
                account_id=self.account_id,
                account_handle=self.client.handle,
                category=info.category,
                code=info.code or "AccountTakedown",
                http_status=info.status_code,
                retry_after=info.retry_after,
            )
            self.log("Global emergency stop activated after an explicit account takedown response", logging.CRITICAL)
        elif info.category in {"rate_limited", "restricted"}:
            self.status = "paused"
            self.last_error = details[:500]
            self._pause_event.clear()
            with self.session_factory() as session:
                if self.account_id is not None:
                    update_account_status(session, self.account_id, "paused", self.last_error)
            self.log("Account paused after a rate-limit or restriction response; review it before resuming", logging.ERROR)
        else:
            self.status = "error"
            self.last_error = details[:500]
            self._stop_event.set()
            self._pause_event.set()
            with self.session_factory() as session:
                if self.account_id is not None:
                    update_account_status(session, self.account_id, "error", self.last_error)

    async def start(self) -> bool:
        if is_global_emergency_stop_active(self.session_factory):
            self.log(
                "Worker start blocked while the global emergency stop is active",
                logging.WARNING,
            )
            return False
        if self.account_id is not None:
            with self.session_factory() as session:
                account = session.get(BlueskyAccount, self.account_id)
                if account is not None and account.status == "suspended":
                    self.log("Worker start blocked pending administrator suspension review", logging.WARNING)
                    return False
        if self._task and not self._task.done():
            return False
        self._stop_event.clear()
        self._pause_event.set()
        self.status = "running"
        self.last_error = None
        if self.account_id is not None:
            with self.session_factory() as session:
                update_account_status(session, self.account_id, "running")
        self._task = asyncio.create_task(self.run(), name="bsky-bot-worker")
        self.log("Worker started")
        return True

    def pause(self) -> None:
        if self.status == "running":
            self._pause_event.clear()
            self.status = "paused"
            if self.account_id is not None:
                with self.session_factory() as session:
                    update_account_status(session, self.account_id, "paused")
            self.log("Worker paused")

    def resume(self) -> None:
        if is_global_emergency_stop_active(self.session_factory):
            self.log("Worker resume blocked while the global emergency stop is active", logging.WARNING)
            return
        if self.status == "paused":
            self._pause_event.set()
            self.status = "running"
            if self.account_id is not None:
                with self.session_factory() as session:
                    update_account_status(session, self.account_id, "running")
            self.log("Worker resumed")

    async def stop(self) -> None:
        self._stop_event.set()
        self._pause_event.set()
        if self._task:
            await self._task
        if self.status != "suspended":
            self.status = "stopped"
        if self.account_id is not None and self.status != "suspended":
            with self.session_factory() as session:
                update_account_status(session, self.account_id, "stopped")
        self.log("Worker stopped")

    async def run(self) -> None:
        try:
            if is_global_emergency_stop_active(self.session_factory):
                self.status = "stopped"
                self.log("Worker login blocked while the global emergency stop is active", logging.WARNING)
                return
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
            self._handle_api_error(exc, "worker")
            self.log(f"Worker error: {exc}", logging.ERROR)
        finally:
            if self.status not in {"error", "paused", "suspended"}:
                self.status = "stopped"
                if self.account_id is not None:
                    with self.session_factory() as session:
                        update_account_status(session, self.account_id, "stopped")

    async def scan_once(self) -> None:
        if is_global_emergency_stop_active(self.session_factory):
            self._stop_event.set()
            return
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
                self._handle_api_error(exc, "search")
                self.log(f"Search failed for '{keyword}': {exc}", logging.WARNING)
                if self.status in {"paused", "suspended", "error"}:
                    return
                continue

            for post in posts:
                if self._stop_event.is_set():
                    return
                await self._inspect_likers(post, keyword)

    async def import_post_likers(self, post_url: str) -> int:
        if is_global_emergency_stop_active(self.session_factory):
            raise RuntimeError("Post import is blocked while the global emergency stop is active")
        try:
            post_uri = await asyncio.to_thread(self.client.post_uri_from_url, post_url)
            likers = await asyncio.to_thread(self.client.post_likers, post_uri, 100)
        except Exception as exc:
            self._handle_api_error(exc, "manual post import")
            raise
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
            self._handle_api_error(exc, "post likers")
            self.log(f"Could not fetch likers for post: {exc}", logging.WARNING)
            return

        for like in likers:
            if is_global_emergency_stop_active(self.session_factory):
                self._stop_event.set()
                return
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
        if is_global_emergency_stop_active(self.session_factory):
            self._stop_event.set()
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
            self._handle_api_error(exc, "profile lookup")
            self.log(f"Profile inspection failed for @{handle}: {exc}", logging.WARNING)
            return False

    async def _matches(self, profile: Profile, keyword: str) -> bool:
        if profile.followers_count >= self.settings.max_followers:
            return False
        text = f"{profile.bio} {profile.display_name}"
        recent = ""
        if self.settings.artist_filter_enabled or not self._contains_relevant_term(text):
            recent = await asyncio.to_thread(self.client.recent_post_text, profile.handle, self.settings.recent_post_limit)
        combined_text = f"{text} {recent}"
        if self.settings.artist_filter_enabled:
            assessment = assess_profile(
                combined_text,
                profile_text=text,
                artist_keywords=tuple(self.settings.artist_keywords),
                audience_keywords=tuple(self.settings.audience_keywords),
            )
            if assessment.excluded_provider and self.settings.exclude_commission_artists:
                self.log(
                    f"Skipped @{profile.handle}: artist or competing-service signal "
                    f"(signals {', '.join(assessment.signals)})"
                )
                return False
        return self._contains_relevant_term(combined_text)

    @staticmethod
    def _contains_relevant_term(text: str) -> bool:
        normalized = text.casefold()
        return any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in RELEVANT_TERMS)

    async def _follow_if_allowed(self, profile: Profile) -> None:
        if is_global_emergency_stop_active(self.session_factory):
            self._stop_event.set()
            return
        with self.session_factory() as session:
            if count_follows_today(session, self.account_id) >= self.settings.max_follows_per_day:
                self.log("Daily follow cap reached; leaving matched profile discovered")
                return
        if self.settings.dry_run:
            self.log(f"DRY RUN: would follow @{profile.handle}")
            return
        if is_global_emergency_stop_active(self.session_factory):
            self._stop_event.set()
            return
        try:
            record_uri = await asyncio.to_thread(self.client.follow, profile.did)
            with self.session_factory() as session:
                mark_followed(session, profile.did, self.account_id)
            record_audit_event(
                self.session_factory,
                "follow_completed",
                f"Followed @{profile.handle}",
                account_id=self.account_id,
                account_handle=self.client.handle,
                code="follow",
            )
            self.log(f"Followed @{profile.handle} ({record_uri})")
            delay = random.randint(self.settings.follow_delay_min_seconds, self.settings.follow_delay_max_seconds)
            self.log(f"Waiting {delay}s before the next follow")
            await asyncio.sleep(delay)
        except Exception as exc:
            with self.session_factory() as session:
                mark_failed(session, profile.did, "follow_failed", self.account_id)
            self._handle_api_error(exc, "follow")
            self.log(f"Follow failed for @{profile.handle}: {exc}", logging.ERROR)
