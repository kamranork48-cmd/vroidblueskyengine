import csv
import asyncio
import hashlib
import io
import logging
import secrets
import time
from email.utils import parsedate_to_datetime
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from openpyxl import load_workbook
from atproto import models

from auth import create_session, current_user, decrypt_secret, encrypt_secret, hash_password, revoke_session, revoke_user_sessions, verify_password
from bsky_client import BlueSkyClient
from config import get_settings
from database import (
    AccountSettings,
    AuditEvent,
    BlueskyAccount,
    ChatConversation,
    ChatMessage,
    SessionToken,
    User,
    TeamInvite,
    create_database,
    get_account,
    get_account_settings,
    get_user_by_username,
    list_accounts,
    list_accounts_for_user,
    list_targets,
    target_counts,
    count_follows_today,
    TargetUser,
    FollowbackNotification,
    AccountNotification,
    remove_account,
)
from safety import (
    classify_bluesky_error,
    get_global_control,
    is_global_emergency_stop_active,
    record_audit_event,
    set_global_emergency_stop,
)
from worker_manager import WorkerManager

settings = get_settings()
logger = logging.getLogger("bsky_bot")
logger.setLevel(settings.log_level.upper())
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
file_handler = logging.FileHandler(settings.log_path, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.handlers.clear()
logger.addHandler(console_handler)
logger.addHandler(file_handler)

engine, session_factory = create_database(settings)
logs: dict[int, list[dict[str, str]]] = defaultdict(list)
inbox_sync_tasks: dict[int, asyncio.Task] = {}
inbox_sync_errors: dict[int, str] = {}
notification_sync_errors: dict[int, str] = {}
notification_sync_last_started: dict[int, float] = {}
chat_clients: dict[int, BlueSkyClient] = {}
followback_task: asyncio.Task | None = None
account_notification_tasks: dict[int, asyncio.Task] = {}


def ensure_master_admin() -> None:
    username = settings.master_admin_username.strip().casefold()
    if not username:
        return
    with session_factory() as session:
        if session.scalar(select(User).where(User.is_master_admin.is_(True))) is not None:
            return
        user = get_user_by_username(session, username)
        if user is None:
            if len(settings.master_admin_password) < 8:
                raise RuntimeError("MASTER_ADMIN_PASSWORD must be at least 8 characters when creating the master admin")
            user = User(
                username=username,
                password_hash=hash_password(settings.master_admin_password),
                role="admin",
                is_admin=True,
                is_master_admin=True,
            )
            session.add(user)
        else:
            user.role = "admin"
            user.is_admin = True
            user.is_master_admin = True
        session.commit()


ensure_master_admin()


def publish_log(account_id: int, message: str) -> None:
    entries = logs[account_id]
    entries.append({"timestamp": datetime.now().isoformat(timespec="seconds"), "message": message})
    del entries[:-250]


worker_manager = WorkerManager(settings, session_factory, publish_log)
templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global followback_task
    logger.info("BlueSky Signal Desk ready")
    followback_task = asyncio.create_task(_followback_poll_loop(), name="followback-notification-poller")
    yield
    if followback_task is not None:
        followback_task.cancel()
        await asyncio.gather(followback_task, return_exceptions=True)
    await worker_manager.stop_all()
    engine.dispose()


app = FastAPI(title="BlueSky Signal Desk", lifespan=lifespan)


class LoginRequest(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=200)


class SetupRequest(LoginRequest):
    pass


class InviteAccept(LoginRequest):
    token: str = Field(min_length=20, max_length=200)


class RoleUpdate(BaseModel):
    role: str = Field(pattern="^(admin|member|viewer)$")


class UserCreate(BaseModel):
    username: str = Field(min_length=1, max_length=120)
    password: str = Field(min_length=8, max_length=200)
    role: str = Field(default="member", pattern="^(admin|member|viewer)$")


class PasswordReset(BaseModel):
    password: str = Field(min_length=8, max_length=200)


class AccountCreate(BaseModel):
    handle: str = Field(min_length=1, max_length=255)
    app_password: str = Field(min_length=1, max_length=500)


class AccountSettingsUpdate(BaseModel):
    keywords: list[str] = Field(min_length=1)
    max_followers: int = Field(ge=1, le=10000)
    dry_run: bool = True
    max_follows_per_day: int = Field(default=30, ge=0, le=1000)
    follow_delay_min_seconds: int = Field(default=60, ge=1, le=86400)
    follow_delay_max_seconds: int = Field(default=180, ge=1, le=86400)


class PostImport(BaseModel):
    post_url: str = Field(min_length=1, max_length=500)


class AccountSuspensionReview(BaseModel):
    review_note: str = Field(min_length=10, max_length=500)


class ChatMessageCreate(BaseModel):
    text: str = Field(min_length=1, max_length=10000)


def _chat_client_for_account(account_id: int) -> tuple[BlueskyAccount, BlueSkyClient]:
    with session_factory() as session:
        account = get_account(session, account_id)
        if account is None:
            raise HTTPException(404, "Account not found")
        block_reason = _inbox_sync_block_reason(account)
        if block_reason is not None:
            raise HTTPException(423, block_reason)
        client = chat_clients.get(account_id)
        if client is None:
            try:
                app_password = decrypt_secret(settings, account.encrypted_app_password)
            except RuntimeError as exc:
                raise HTTPException(503, str(exc)) from exc
            client = BlueSkyClient(account.handle, app_password)
            chat_clients[account_id] = client
    return account, client


def _aware_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _inbox_sync_block_reason(account: BlueskyAccount, now: datetime | None = None) -> str | None:
    status = account.chat_sync_status or "ok"
    if account.status == "suspended" or status == "suspended":
        return account.chat_sync_error or account.last_error or "Inbox sync is paused pending account review."
    if status in {"authentication", "restricted"}:
        return account.chat_sync_error or "Inbox sync is paused; reconnect or review this account before retrying."
    if status in {"rate_limited", "transient"}:
        retry_at = account.chat_sync_retry_at
        if retry_at is None:
            return account.chat_sync_error or "Inbox sync is paused until this account can be reviewed."
        current_time = now or datetime.now(timezone.utc)
        if _aware_utc(retry_at) > current_time:
            return account.chat_sync_error or f"Inbox sync can retry after {_aware_utc(retry_at).isoformat()}."
    return None


def _retry_at_from_error(error_info) -> datetime | None:
    now = datetime.now(timezone.utc)
    if error_info.retry_after:
        try:
            return now + timedelta(seconds=max(float(error_info.retry_after), 0))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(error_info.retry_after)
                return _aware_utc(retry_at)
            except (TypeError, ValueError, OverflowError):
                pass
    if error_info.rate_limit_reset:
        try:
            return datetime.fromtimestamp(float(error_info.rate_limit_reset), timezone.utc)
        except (ValueError, OverflowError, OSError):
            try:
                return _aware_utc(parsedate_to_datetime(error_info.rate_limit_reset))
            except (TypeError, ValueError, OverflowError):
                pass
    return None


def _record_inbox_sync_failure(account_id: int, error: Exception, operation: str = "inbox") -> str:
    error_info = classify_bluesky_error(error)
    retry_at = _retry_at_from_error(error_info)
    if error_info.category == "suspended":
        status = "suspended"
        message = f"Bluesky reports this account as taken down. {operation.capitalize()} sync is blocked pending administrator review."
    elif error_info.category == "rate_limited":
        status = "rate_limited"
        if retry_at is not None:
            message = f"Bluesky rate-limited {operation} sync. It will retry after {retry_at.isoformat()}."
        else:
            message = f"Bluesky rate-limited {operation} sync; automatic retries are paused because no retry window was provided."
    elif error_info.category == "authentication":
        status = "authentication"
        message = f"Bluesky rejected this account's credentials. Reconnect the account to restore {operation} sync."
    elif error_info.category == "restricted":
        status = "restricted"
        message = f"Bluesky denied {operation} access. Review the account status before retrying."
    else:
        status = "transient"
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=60)
        message = f"{operation.capitalize()} sync failed: {error_info.message[:350]}"

    account_handle = ""
    with session_factory() as session:
        account = get_account(session, account_id)
        if account is not None:
            account_handle = account.handle
            account.chat_sync_status = status
            account.chat_sync_error = message[:500]
            account.chat_sync_retry_at = retry_at
            if status == "suspended":
                account.status = "suspended"
                account.last_error = message[:500]
            session.commit()

    if status in {"suspended", "authentication"}:
        chat_clients.pop(account_id, None)
    if status == "suspended":
        set_global_emergency_stop(
            session_factory,
            True,
            f"Bluesky reported a takedown for @{account_handle or account_id}; automation is stopped pending review.",
            account_id=account_id,
            account_handle=account_handle,
            category=error_info.category,
            code=error_info.code,
            http_status=error_info.status_code,
            retry_after=error_info.retry_after,
        )
    record_audit_event(
        session_factory,
        "api_error",
        message,
        account_id=account_id,
        account_handle=account_handle,
        severity="critical" if status == "suspended" else "warning",
        category=error_info.category,
        code=error_info.code,
        http_status=error_info.status_code,
        retry_after=error_info.retry_after,
    )
    inbox_sync_errors[account_id] = message
    return message


def _check_account_followbacks(account_id: int) -> None:
    account, client = _chat_client_for_account(account_id)
    with session_factory() as session:
        followed = {
            target.did: target
            for target in session.scalars(
                select(TargetUser).where(
                    TargetUser.account_id == account_id,
                    TargetUser.status == "followed",
                )
            )
        }
    if not followed:
        return
    if _inbox_sync_block_reason(account) is not None:
        return
    client._require_login()
    account_did = str(getattr(getattr(client.client, "me", None), "did", "") or account.did or "")
    if not account_did:
        return
    cursor = None
    followers: dict[str, Any] = {}
    for _ in range(5):
        response = client.get_followers(account_did, cursor=cursor)
        for profile in getattr(response, "followers", []):
            did = str(getattr(profile, "did", "") or "")
            if did in followed:
                followers[did] = profile
        cursor = getattr(response, "cursor", None)
        if not cursor or len(followers) == len(followed):
            break
    with session_factory() as session:
        for did, profile in followers.items():
            if session.scalar(
                select(FollowbackNotification).where(
                    FollowbackNotification.account_id == account_id,
                    FollowbackNotification.did == did,
                )
            ):
                continue
            target = followed[did]
            handle = str(getattr(profile, "handle", "") or target.handle)
            display_name = str(getattr(profile, "display_name", "") or "")
            session.add(
                FollowbackNotification(
                    account_id=account_id,
                    did=did,
                    handle=handle,
                    display_name=display_name,
                    avatar=str(getattr(profile, "avatar", "") or ""),
                    message=f"@{handle} followed @{account.handle} back",
                )
            )
            logger.info("Follow-back detected for account %s from @%s", account_id, handle)
        session.commit()


def _notification_value(item: object, key: str, default: object = "") -> object:
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _sync_account_notifications(account_id: int) -> None:
    account, client = _chat_client_for_account(account_id)
    if _inbox_sync_block_reason(account) is not None:
        return
    cursor = None
    with session_factory() as session:
        for _ in range(3):
            response = client.list_notifications(limit=100, cursor=cursor)
            for item in getattr(response, "notifications", []):
                notification_id = str(_notification_value(item, "cid") or _notification_value(item, "uri") or "")
                if not notification_id:
                    continue
                reason = str(_notification_value(item, "reason", "unknown") or "unknown")
                author = _notification_value(item, "author", {})
                actor = _chat_profile(author)
                indexed_at = _parse_chat_timestamp(_notification_value(item, "indexed_at") or _notification_value(item, "indexedAt"))
                if indexed_at is None:
                    indexed_at = datetime.now(timezone.utc)
                is_read = bool(_notification_value(item, "is_read") or _notification_value(item, "isRead"))
                if session.scalar(
                    select(AccountNotification).where(
                        AccountNotification.account_id == account_id,
                        AccountNotification.notification_id == notification_id,
                    )
                ):
                    continue
                reason_label = {
                    "like": "liked your post",
                    "repost": "reposted your post",
                    "follow": "followed you",
                    "mention": "mentioned you",
                    "reply": "replied to your post",
                    "quote": "quoted your post",
                    "starterpack": "added you to a starter pack",
                    "starterpack-joined": "joined your starter pack",
                    "like-via-repost": "liked your post via a repost",
                    "repost-via-repost": "reposted your post via a repost",
                    "subscribed-post": "subscribed to your post",
                    "contact-match": "matched you as a contact",
                    "subscribed": "subscribed to you",
                    "verified": "verified you",
                    "unverified": "removed verification from you",
                }.get(reason, f"sent a {reason} notification")
                session.add(
                    AccountNotification(
                        account_id=account_id,
                        notification_id=notification_id,
                        reason=reason,
                        actor_did=actor["did"],
                        actor_handle=actor["handle"],
                        actor_display_name=actor["display_name"],
                        actor_avatar=actor["avatar"],
                        subject_uri=str(_notification_value(item, "uri", "") or ""),
                        message=f"@{actor['handle'] or 'Someone'} {reason_label}",
                        created_at=indexed_at,
                        read_at=indexed_at if is_read else None,
                    )
                )
            session.commit()
            cursor = getattr(response, "cursor", None)
            if not cursor:
                break
    notification_sync_errors.pop(account_id, None)


async def _followback_poll_loop() -> None:
    while True:
        with session_factory() as session:
            account_ids = [account.id for account in list_accounts(session)]
        for account_id in account_ids:
            try:
                await asyncio.to_thread(_check_account_followbacks, account_id)
            except Exception as exc:
                logger.exception("Follow-back notification check failed for account %s", account_id)
                if not (isinstance(exc, HTTPException) and exc.status_code == 423):
                    _record_inbox_sync_failure(account_id, exc)
        _schedule_account_notification_sync(account_ids)
        await asyncio.sleep(60)


async def _sync_account_notifications_in_background(account_id: int) -> None:
    try:
        await asyncio.to_thread(_sync_account_notifications, account_id)
    except HTTPException as exc:
        if exc.status_code == 423:
            notification_sync_errors[account_id] = str(exc.detail)
        else:
            logger.exception("Bluesky notification check failed for account %s", account_id)
            notification_sync_errors[account_id] = _record_inbox_sync_failure(
                account_id,
                exc,
                operation="notifications",
            )
    except Exception as exc:
        logger.exception("Bluesky notification check failed for account %s", account_id)
        notification_sync_errors[account_id] = _record_inbox_sync_failure(
            account_id,
            exc,
            operation="notifications",
        )
    finally:
        account_notification_tasks.pop(account_id, None)


def _schedule_account_notification_sync(account_ids: list[int]) -> list[int]:
    syncing: list[int] = []
    with session_factory() as session:
        accounts = {
            account.id: account
            for account in session.scalars(
                select(BlueskyAccount).where(BlueskyAccount.id.in_(account_ids))
            )
        } if account_ids else {}
    for account_id in account_ids:
        account = accounts.get(account_id)
        if account is None:
            continue
        block_reason = _inbox_sync_block_reason(account)
        if block_reason is not None:
            notification_sync_errors[account_id] = block_reason
            continue
        notification_sync_errors.pop(account_id, None)
        task = account_notification_tasks.get(account_id)
        if task is None or task.done():
            if time.monotonic() - notification_sync_last_started.get(account_id, 0) < 30:
                continue
            notification_sync_last_started[account_id] = time.monotonic()
            account_notification_tasks[account_id] = asyncio.create_task(
                _sync_account_notifications_in_background(account_id)
            )
            task = account_notification_tasks[account_id]
        if not task.done():
            syncing.append(account_id)
    return syncing


def _parse_chat_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _chat_profile(profile: object) -> dict[str, str]:
    if isinstance(profile, dict):
        get_value = profile.get
    else:
        get_value = lambda key, default=None: getattr(profile, key, default)
    return {
        "did": str(get_value("did") or ""),
        "handle": str(get_value("handle") or ""),
        "display_name": str(get_value("displayName") or get_value("display_name") or ""),
        "avatar": str(get_value("avatar") or ""),
    }


def _upsert_chat_conversation(session, account_id: int, payload: dict, account_did: str) -> ChatConversation:
    conversation_id = str(payload.get("id") or "")
    if not conversation_id:
        raise ValueError("Bluesky returned a conversation without an id")
    conversation = session.scalar(
        select(ChatConversation).where(
            ChatConversation.account_id == account_id,
            ChatConversation.conversation_id == conversation_id,
        )
    )
    if conversation is None:
        conversation = ChatConversation(account_id=account_id, conversation_id=conversation_id)
        session.add(conversation)
    members = [_chat_profile(member) for member in payload.get("members", [])]
    other = next((member for member in members if member["did"] != account_did), members[0] if members else {})
    latest = payload.get("lastMessage") or {}
    sender = _chat_profile(latest.get("sender"))
    conversation.participant_did = other.get("did") or None
    conversation.participant_handle = other.get("handle", "")
    conversation.participant_display_name = other.get("display_name", "")
    conversation.participant_avatar = other.get("avatar", "")
    conversation.latest_message_id = str(latest.get("id") or "")
    conversation.latest_message_preview = str(latest.get("text") or "")
    conversation.latest_message_at = _parse_chat_timestamp(latest.get("sentAt"))
    conversation.unread_count = int(payload.get("unreadCount") or 0)
    conversation.status = str(payload.get("status") or "active")[:32]
    return conversation


def _store_chat_message(session, account_id: int, conversation_id: str, payload: dict, account_did: str) -> ChatMessage | None:
    message_id = str(payload.get("id") or "")
    if not message_id:
        return None
    message = session.scalar(
        select(ChatMessage).where(
            ChatMessage.account_id == account_id,
            ChatMessage.conversation_id == conversation_id,
            ChatMessage.message_id == message_id,
        )
    )
    if message is None:
        sender = _chat_profile(payload.get("sender"))
        message = ChatMessage(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            sender_did=sender["did"],
            text=str(payload.get("text") or ""),
            sent_at=_parse_chat_timestamp(payload.get("sentAt")),
            is_from_account=sender["did"] == account_did,
        )
        session.add(message)
    return message


def _sync_account_conversations(account_id: int) -> None:
    account, client = _chat_client_for_account(account_id)
    block_reason = _inbox_sync_block_reason(account)
    if block_reason is not None:
        raise HTTPException(423, block_reason)
    client._require_login()
    account_did = str(getattr(getattr(client.client, "me", None), "did", "") or "")
    cursor = None
    with session_factory() as session:
        for _ in range(5):
            result = client.chat_list_conversations(cursor=cursor)
            conversations = result.content.get("convos", [])
            for payload in conversations:
                if isinstance(payload, dict):
                    _upsert_chat_conversation(session, account_id, payload, account_did)
            session.commit()
            cursor = result.content.get("cursor")
            if not cursor or not conversations:
                break
        stored_account = get_account(session, account_id)
        if stored_account is not None:
            stored_account.chat_sync_status = "ok"
            stored_account.chat_sync_error = None
            stored_account.chat_sync_retry_at = None
            session.commit()


async def _sync_account_in_background(account_id: int) -> None:
    try:
        await asyncio.to_thread(_sync_account_conversations, account_id)
        inbox_sync_errors.pop(account_id, None)
    except HTTPException as exc:
        logger.exception("Inbox synchronization failed for account %s", account_id)
        if exc.status_code == 423:
            inbox_sync_errors[account_id] = str(exc.detail)
        else:
            _record_inbox_sync_failure(account_id, exc)
    except Exception as exc:
        logger.exception("Inbox synchronization failed for account %s", account_id)
        _record_inbox_sync_failure(account_id, exc)
    finally:
        inbox_sync_tasks.pop(account_id, None)


def _schedule_inbox_sync(account_ids: list[int]) -> list[int]:
    scheduled: list[int] = []
    now = datetime.now(timezone.utc)
    with session_factory() as session:
        accounts = {
            account.id: account
            for account in session.scalars(
                select(BlueskyAccount).where(BlueskyAccount.id.in_(account_ids))
            )
        } if account_ids else {}
        blocked: dict[int, str] = {}
        changed = False
        for account_id, account in accounts.items():
            block_reason = _inbox_sync_block_reason(account, now)
            if block_reason is not None:
                blocked[account_id] = block_reason
                continue
            if account.chat_sync_status != "ok" or account.chat_sync_error or account.chat_sync_retry_at:
                account.chat_sync_status = "ok"
                account.chat_sync_error = None
                account.chat_sync_retry_at = None
                changed = True
        if changed:
            session.commit()
    for account_id in account_ids:
        if account_id in blocked:
            inbox_sync_errors[account_id] = blocked[account_id]
            continue
        inbox_sync_errors.pop(account_id, None)
        task = inbox_sync_tasks.get(account_id)
        if task is None or task.done():
            inbox_sync_tasks[account_id] = asyncio.create_task(_sync_account_in_background(account_id))
            scheduled.append(account_id)
    return scheduled


def _conversation_response(session, conversation: ChatConversation) -> dict:
    target = None
    if conversation.participant_did:
        target = session.scalar(
            select(TargetUser).where(
                TargetUser.account_id == conversation.account_id,
                TargetUser.did == conversation.participant_did,
            )
        )
    return {
        "id": conversation.conversation_id,
        "account_id": conversation.account_id,
        "participant": {
            "did": conversation.participant_did,
            "handle": conversation.participant_handle,
            "display_name": conversation.participant_display_name,
            "avatar": conversation.participant_avatar,
        },
        "latest_message": conversation.latest_message_preview,
        "latest_message_id": conversation.latest_message_id,
        "last_message_at": conversation.latest_message_at.isoformat() if conversation.latest_message_at else None,
        "unread_count": conversation.unread_count,
        "status": conversation.status,
        "discovery": {
            "matched_keyword": target.matched_keyword,
            "follow_status": target.status,
            "followed_at": target.followed_at_timestamp.isoformat() if target.followed_at_timestamp else None,
        } if target else None,
    }


def require_user(request: Request) -> User:
    with session_factory() as session:
        return current_user(request, session)


def effective_role(user: User) -> str:
    return "admin" if user.is_admin or user.role == "admin" else (user.role or "member")


def require_admin(request: Request) -> User:
    user = require_user(request)
    if effective_role(user) != "admin":
        raise HTTPException(403, "Administrator permission required")
    return user


def require_master_admin(request: Request) -> User:
    user = require_user(request)
    if not user.is_master_admin:
        raise HTTPException(403, "Master administrator permission required")
    return user


def require_editor(request: Request) -> User:
    user = require_user(request)
    if effective_role(user) == "viewer":
        raise HTTPException(403, "Viewer accounts are read-only")
    return user


def require_account(request: Request, account_id: int) -> BlueskyAccount:
    user = require_user(request)
    with session_factory() as session:
        account = session.scalar(
            select(BlueskyAccount).where(
                BlueskyAccount.id == account_id,
                BlueskyAccount.created_by_user_id == user.id,
            )
        )
        if account is None:
            raise HTTPException(404, "Account not found")
        return account


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    try:
        require_user(request)
    except HTTPException:
        return RedirectResponse("/login", status_code=303)
    return templates.TemplateResponse(request=request, name="team.html")


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request=request, name="login.html")


@app.get("/api/auth/status")
async def auth_status():
    with session_factory() as session:
        user_count = int(session.scalar(select(func.count(User.id))) or 0)
    return {"setup_required": user_count == 0}


@app.get("/healthz")
async def healthcheck():
    with session_factory() as session:
        account_count = int(session.scalar(select(func.count(BlueskyAccount.id))) or 0)
    return {"status": "ok", "accounts": account_count, "database": "connected"}


@app.post("/api/auth/setup")
async def setup_user(payload: SetupRequest):
    with session_factory() as session:
        if session.scalar(select(func.count(User.id))):
            raise HTTPException(409, "Initial setup has already been completed")
        user = User(username=payload.username.strip().casefold(), password_hash=hash_password(payload.password), is_admin=True, role="admin", is_master_admin=True)
        session.add(user)
        session.commit()
        token = create_session(session, user, settings)
    response = JSONResponse({"ok": True, "redirect": "/"})
    response.set_cookie("session", token, httponly=True, samesite="lax", max_age=settings.session_max_age_hours * 3600, path="/")
    return response


@app.post("/api/auth/login")
async def login(payload: LoginRequest):
    with session_factory() as session:
        user = get_user_by_username(session, payload.username.strip())
        if user is None or user.is_active is False or not verify_password(payload.password, user.password_hash):
            raise HTTPException(401, "Invalid username or password")
        token = create_session(session, user, settings)
    response = JSONResponse({"ok": True, "redirect": "/"})
    response.set_cookie("session", token, httponly=True, samesite="lax", max_age=settings.session_max_age_hours * 3600, path="/")
    return response


@app.post("/api/auth/logout")
async def logout(request: Request):
    with session_factory() as session:
        revoke_session(session, request.cookies.get("session"))
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session", path="/")
    return response


@app.get("/api/me")
async def me(request: Request):
    user = require_user(request)
    return {"id": user.id, "username": user.username, "role": effective_role(user), "is_master_admin": user.is_master_admin}


@app.get("/api/team/users")
async def team_users(request: Request):
    require_master_admin(request)
    with session_factory() as session:
        return [
            {"id": user.id, "username": user.username, "role": effective_role(user), "is_active": user.is_active, "is_master_admin": user.is_master_admin}
            for user in session.scalars(select(User).order_by(User.username))
        ]


@app.post("/api/team/invites")
async def create_invite(request: Request):
    admin = require_master_admin(request)
    raw_token = secrets.token_urlsafe(32)
    with session_factory() as session:
        session.add(
            TeamInvite(
                token_hash=hashlib.sha256(raw_token.encode()).hexdigest(),
                created_by_user_id=admin.id,
                expires_at=datetime.now(timezone.utc) + timedelta(days=2),
            )
        )
        session.commit()
    return {"token": raw_token, "expires_in_hours": 48}


@app.post("/api/team/users")
async def create_team_user(request: Request, payload: UserCreate):
    require_master_admin(request)
    username = payload.username.strip().casefold()
    if not username:
        raise HTTPException(400, "Username cannot be empty")
    with session_factory() as session:
        if get_user_by_username(session, username) is not None:
            raise HTTPException(409, "Username is already in use")
        user = User(
            username=username,
            password_hash=hash_password(payload.password),
            role=payload.role,
            is_admin=payload.role == "admin",
        )
        session.add(user)
        session.commit()
        return {"id": user.id, "username": user.username, "role": effective_role(user), "is_active": user.is_active}


@app.post("/api/team/users/{user_id}/role")
async def update_user_role(request: Request, user_id: int, payload: RoleUpdate):
    require_master_admin(request)
    with session_factory() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(404, "User not found")
        if user.is_master_admin:
            raise HTTPException(400, "The master administrator role cannot be changed")
        if effective_role(user) == "admin" and payload.role != "admin":
            admin_count = int(session.scalar(select(func.count(User.id)).where((User.role == "admin") | (User.is_admin.is_(True)), User.is_active.is_(True))) or 0)
            if admin_count <= 1:
                raise HTTPException(400, "The team must keep at least one active administrator")
        user.role = payload.role
        user.is_admin = payload.role == "admin"
        session.commit()
    return {"id": user_id, "role": payload.role}


@app.post("/api/team/users/{user_id}/disable")
async def disable_user(request: Request, user_id: int):
    require_master_admin(request)
    with session_factory() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(404, "User not found")
        if user.is_master_admin:
            raise HTTPException(400, "The master administrator cannot be disabled")
        user.is_active = False
        session.query(SessionToken).filter(SessionToken.user_id == user_id).delete(synchronize_session=False)
        session.commit()
    return {"disabled": True}


@app.post("/api/team/users/{user_id}/reset-password")
async def reset_user_password(request: Request, user_id: int, payload: PasswordReset):
    require_master_admin(request)
    with session_factory() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(404, "User not found")
        if user.is_master_admin:
            raise HTTPException(400, "Use MASTER_ADMIN_PASSWORD to manage the master administrator")
        user.password_hash = hash_password(payload.password)
        user.is_active = True
        revoke_user_sessions(session, user_id)
        session.commit()
    return {"reset": True}


@app.delete("/api/team/users/{user_id}")
async def delete_user(request: Request, user_id: int):
    require_master_admin(request)
    with session_factory() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(404, "User not found")
        if user.is_master_admin:
            raise HTTPException(400, "The master administrator cannot be deleted")
        revoke_user_sessions(session, user_id)
        session.delete(user)
        session.commit()
    return {"deleted": True}


@app.post("/api/auth/accept-invite")
async def accept_invite(payload: InviteAccept):
    token_hash = hashlib.sha256(payload.token.encode()).hexdigest()
    with session_factory() as session:
        invite = session.scalar(select(TeamInvite).where(TeamInvite.token_hash == token_hash))
        now = datetime.now(timezone.utc)
        if invite is None or invite.used_at is not None or invite.expires_at.replace(tzinfo=timezone.utc) <= now:
            raise HTTPException(400, "Invite is invalid or expired")
        username = payload.username.strip().casefold()
        if get_user_by_username(session, username) is not None:
            raise HTTPException(409, "Username is already in use")
        user = User(username=username, password_hash=hash_password(payload.password), role="member", is_admin=False)
        session.add(user)
        invite.used_at = now
        session.commit()
        token = create_session(session, user, settings)
    response = JSONResponse({"ok": True, "redirect": "/"})
    response.set_cookie("session", token, httponly=True, samesite="lax", max_age=settings.session_max_age_hours * 3600, path="/")
    return response


@app.get("/api/inbox")
async def inbox(request: Request, account_id: int | None = None, search: str = ""):
    user = require_user(request)
    if account_id is not None:
        require_account(request, account_id)
        account_ids = [account_id]
    else:
        with session_factory() as session:
            account_ids = [account.id for account in list_accounts_for_user(session, user.id)]
    scheduled_accounts = _schedule_inbox_sync(account_ids)
    with session_factory() as session:
        query = select(ChatConversation).where(ChatConversation.account_id.in_(account_ids))
        if account_id is not None:
            query = query.where(ChatConversation.account_id == account_id)
        if search.strip():
            term = f"%{search.strip()}%"
            query = query.where(
                (ChatConversation.participant_handle.ilike(term))
                | (ChatConversation.participant_display_name.ilike(term))
                | (ChatConversation.latest_message_preview.ilike(term))
            )
        conversations = list(session.scalars(query.order_by(ChatConversation.latest_message_at.desc().nullslast())))
        account_handles = {item.id: item.handle for item in list_accounts_for_user(session, user.id)}
        return {
            "conversations": [
                {**_conversation_response(session, item), "account_handle": account_handles.get(item.account_id, "")}
                for item in conversations
            ],
            "syncing_account_ids": scheduled_accounts,
            "errors": [
                {
                    "account_id": current_account_id,
                    "account_handle": account_handles.get(current_account_id, ""),
                    "error": inbox_sync_errors[current_account_id],
                }
                for current_account_id in account_ids
                if current_account_id in inbox_sync_errors
            ],
        }


@app.get("/api/notifications")
async def notifications(request: Request):
    user = require_user(request)
    with session_factory() as session:
        account_ids = [account.id for account in list_accounts_for_user(session, user.id)]
    scheduled_accounts = _schedule_inbox_sync(account_ids)
    scheduled_notification_accounts = _schedule_account_notification_sync(account_ids)
    with session_factory() as session:
        rows = list(
            session.scalars(
                select(FollowbackNotification)
                .where(FollowbackNotification.account_id.in_(account_ids))
                .order_by(FollowbackNotification.created_at.desc())
                .limit(50)
            )
        )
        conversations = list(
            session.scalars(
                select(ChatConversation)
                .where(
                    ChatConversation.account_id.in_(account_ids),
                    ChatConversation.unread_count > 0,
                )
                .order_by(ChatConversation.latest_message_at.desc().nullslast())
                .limit(50)
            )
        )
        account_notifications = list(
            session.scalars(
                select(AccountNotification)
                .where(AccountNotification.account_id.in_(account_ids))
                .order_by(AccountNotification.created_at.desc())
                .limit(100)
            )
        )
        account_handles = {account.id: account.handle for account in list_accounts_for_user(session, user.id)}
        followback_items = [
            {
                "id": f"followback-{item.id}",
                "type": "followback",
                "account_id": item.account_id,
                "account_handle": account_handles.get(item.account_id, ""),
                "did": item.did,
                "handle": item.handle,
                "display_name": item.display_name,
                "avatar": item.avatar,
                "message": item.message,
                "created_at": item.created_at.isoformat(),
                "read": item.read_at is not None,
            }
            for item in rows
        ]
        message_items = [
            {
                "id": f"message-{item.id}",
                "type": "message",
                "account_id": item.account_id,
                "account_handle": account_handles.get(item.account_id, ""),
                "handle": item.participant_handle,
                "display_name": item.participant_display_name,
                "avatar": item.participant_avatar,
                "message": f"New message from @{item.participant_handle or 'unknown'}: {item.latest_message_preview}",
                "created_at": item.latest_message_at.isoformat() if item.latest_message_at else "",
                "read": False,
                "conversation_id": item.conversation_id,
                "unread_count": item.unread_count,
            }
            for item in conversations
        ]
        account_notification_items = [
            {
                "id": f"account-notification-{item.id}",
                "type": item.reason,
                "account_id": item.account_id,
                "account_handle": account_handles.get(item.account_id, ""),
                "handle": item.actor_handle,
                "display_name": item.actor_display_name,
                "avatar": item.actor_avatar,
                "message": item.message,
                "created_at": item.created_at.isoformat(),
                "read": item.read_at is not None,
            }
            for item in account_notifications
        ]
        items = sorted(
            followback_items + message_items + account_notification_items,
            key=lambda item: item["created_at"],
            reverse=True,
        )
        return {
            "notifications": items,
            "unread_count": sum(
                item.get("unread_count", 1) if item["type"] == "message" else 1
                for item in items
                if not item["read"]
            ),
            "syncing_account_ids": scheduled_accounts,
            "syncing_notification_account_ids": scheduled_notification_accounts,
            "notification_errors": [
                {
                    "account_id": current_account_id,
                    "account_handle": account_handles.get(current_account_id, ""),
                    "error": notification_sync_errors[current_account_id],
                }
                for current_account_id in account_ids
                if current_account_id in notification_sync_errors
            ],
        }


@app.post("/api/inbox/{account_id}/start/{member_did}")
async def start_inbox_conversation(request: Request, account_id: int, member_did: str):
    require_editor(request)
    require_account(request, account_id)
    if not member_did.startswith("did:") or len(member_did) > 255:
        raise HTTPException(400, "A valid Bluesky DID is required")
    account, client = _chat_client_for_account(account_id)
    try:
        client._require_login()
        result = await asyncio.to_thread(
            client._chat_client().invoke_query,
            "chat.bsky.convo.getConvoForMembers",
            models.ChatBskyConvoGetConvoForMembers.Params(members=[member_did]),
        )
    except Exception as exc:
        logger.exception("Could not start inbox conversation for account %s", account_id)
        raise HTTPException(502, "Bluesky could not open a conversation with this account") from exc
    response_content = getattr(result, "content", None)
    conversation_payload = response_content.get("convo") if isinstance(response_content, dict) else None
    if not isinstance(conversation_payload, dict):
        raise HTTPException(502, "Bluesky returned an invalid conversation")
    account_did = str(getattr(getattr(client.client, "me", None), "did", "") or "")
    with session_factory() as session:
        conversation = _upsert_chat_conversation(
            session,
            account_id,
            conversation_payload,
            account_did,
        )
        conversation_id = conversation.conversation_id
        session.commit()
    return {
        "account_id": account.id,
        "account_handle": account.handle,
        "conversation_id": conversation_id,
    }


@app.post("/api/notifications/read")
async def mark_notifications_read(request: Request):
    user = require_user(request)
    seen_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    with session_factory() as session:
        account_ids = [account.id for account in list_accounts_for_user(session, user.id)]
    for account_id in account_ids:
        try:
            _, client = _chat_client_for_account(account_id)
            await asyncio.to_thread(client.mark_notifications_seen, seen_at)
        except Exception:
            logger.exception("Could not mark Bluesky notifications seen for account %s", account_id)
    with session_factory() as session:
        session.query(FollowbackNotification).filter(
            FollowbackNotification.account_id.in_(account_ids),
            FollowbackNotification.read_at.is_(None),
        ).update({"read_at": datetime.now(timezone.utc)}, synchronize_session=False)
        session.query(ChatConversation).filter(
            ChatConversation.account_id.in_(account_ids),
            ChatConversation.unread_count > 0,
        ).update(
            {"unread_count": 0},
            synchronize_session=False,
        )
        session.query(AccountNotification).filter(
            AccountNotification.account_id.in_(account_ids),
            AccountNotification.read_at.is_(None)
        ).update({"read_at": datetime.now(timezone.utc)}, synchronize_session=False)
        session.commit()
    return {"read": True}


@app.get("/api/inbox/{account_id}/{conversation_id}/messages")
async def inbox_messages(
    request: Request,
    account_id: int,
    conversation_id: str,
    cursor: str | None = None,
    limit: int = 50,
):
    require_user(request)
    require_account(request, account_id)
    account, client = _chat_client_for_account(account_id)
    block_reason = _inbox_sync_block_reason(account)
    if block_reason is not None:
        raise HTTPException(423, block_reason)
    try:
        client._require_login()
        account_did = str(getattr(getattr(client.client, "me", None), "did", "") or "")
        result = await asyncio.to_thread(
            client.chat_get_messages,
            conversation_id,
            min(max(limit, 1), 100),
            cursor,
        )
    except Exception as exc:
        logger.exception("Could not load inbox messages for account %s", account_id)
        message = _record_inbox_sync_failure(account_id, exc)
        raise HTTPException(502, message) from exc
    messages = result.content.get("messages", [])
    with session_factory() as session:
        conversation = session.scalar(
            select(ChatConversation).where(
                ChatConversation.account_id == account_id,
                ChatConversation.conversation_id == conversation_id,
            )
        )
        if conversation is None:
            raise HTTPException(404, "Conversation not found for this account")
        page_message_ids: list[str] = []
        for payload in messages:
            if isinstance(payload, dict):
                _store_chat_message(session, account_id, conversation_id, payload, account_did)
                message_id = str(payload.get("id") or "")
                if message_id:
                    page_message_ids.append(message_id)
        session.commit()
        if page_message_ids:
            stored = list(session.scalars(
                select(ChatMessage).where(
                    ChatMessage.account_id == account_id,
                    ChatMessage.conversation_id == conversation_id,
                    ChatMessage.message_id.in_(page_message_ids),
                ).order_by(ChatMessage.sent_at.asc().nullslast(), ChatMessage.id.asc())
            ))
        else:
            stored = []
        return {
            "account_id": account.id,
            "account_handle": account.handle,
            "conversation_id": conversation_id,
            "messages": [
                {
                    "id": item.message_id,
                    "text": item.text,
                    "sent_at": item.sent_at.isoformat() if item.sent_at else None,
                    "is_from_account": item.is_from_account,
                }
                for item in stored
            ],
            "cursor": result.content.get("cursor"),
            "has_more": bool(result.content.get("cursor")),
        }


@app.post("/api/inbox/{account_id}/{conversation_id}/send")
async def send_inbox_message(request: Request, account_id: int, conversation_id: str, payload: ChatMessageCreate):
    require_editor(request)
    require_account(request, account_id)
    text_value = payload.text.strip()
    if not text_value:
        raise HTTPException(400, "Message cannot be empty")
    account, client = _chat_client_for_account(account_id)
    try:
        client._require_login()
    except Exception as exc:
        logger.exception("Inbox authentication failed for account %s", account_id)
        raise HTTPException(502, "Could not authenticate the selected Bluesky account for chat") from exc
    with session_factory() as session:
        conversation = session.scalar(
            select(ChatConversation).where(
                ChatConversation.account_id == account_id,
                ChatConversation.conversation_id == conversation_id,
            )
        )
        if conversation is None:
            raise HTTPException(404, "Conversation not found for this account")
    try:
        result = await asyncio.to_thread(client.chat_send_message, conversation_id, text_value)
    except Exception as exc:
        logger.exception("Could not send inbox message for account %s", account_id)
        raise HTTPException(502, "Bluesky did not accept this message") from exc
    account_did = str(getattr(getattr(client.client, "me", None), "did", "") or "")
    message_payload = result.content.get("message") if isinstance(result.content.get("message"), dict) else result.content
    with session_factory() as session:
        message = _store_chat_message(session, account_id, conversation_id, message_payload, account_did)
        conversation = session.scalar(
            select(ChatConversation).where(
                ChatConversation.account_id == account_id,
                ChatConversation.conversation_id == conversation_id,
            )
        )
        if conversation is not None and message is not None:
            conversation.latest_message_id = message.message_id
            conversation.latest_message_preview = message.text
            conversation.latest_message_at = message.sent_at
            conversation.unread_count = 0
        session.commit()
        return {"message_id": message.message_id if message else "", "account_id": account.id}


@app.post("/api/inbox/{account_id}/{conversation_id}/read")
async def mark_inbox_read(request: Request, account_id: int, conversation_id: str):
    require_editor(request)
    require_account(request, account_id)
    with session_factory() as session:
        conversation = session.scalar(
            select(ChatConversation).where(
                ChatConversation.account_id == account_id,
                ChatConversation.conversation_id == conversation_id,
            )
        )
        if conversation is None:
            raise HTTPException(404, "Conversation not found for this account")
        message_id = conversation.latest_message_id
    if message_id:
        _, client = _chat_client_for_account(account_id)
        try:
            await asyncio.to_thread(client.chat_mark_read, conversation_id, message_id)
        except Exception as exc:
            logger.exception("Could not mark inbox conversation read for account %s", account_id)
            raise HTTPException(502, "Could not mark this conversation as read") from exc
    with session_factory() as session:
        conversation = session.scalar(
            select(ChatConversation).where(
                ChatConversation.account_id == account_id,
                ChatConversation.conversation_id == conversation_id,
            )
        )
        if conversation:
            conversation.unread_count = 0
            session.commit()
    return {"read": True, "account_id": account_id, "conversation_id": conversation_id}


@app.get("/api/accounts")
async def accounts(request: Request):
    user = require_user(request)
    with session_factory() as session:
        return [
            {
                "id": account.id,
                "handle": account.handle,
                "status": worker_manager.get(account.id).status if worker_manager.get(account.id) else account.status,
                "last_error": account.last_error,
            }
            for account in list_accounts_for_user(session, user.id)
        ]


@app.get("/api/system/control")
async def system_control_status(request: Request):
    user = require_user(request)
    control = get_global_control(session_factory)
    control["can_clear"] = effective_role(user) == "admin"
    return control


@app.post("/api/system/emergency-stop")
async def activate_emergency_stop(request: Request):
    user = require_editor(request)
    state = set_global_emergency_stop(
        session_factory,
        True,
        f"Activated by {user.username}",
        actor_user_id=user.id,
        code="manual_emergency_stop",
    )
    await worker_manager.stop_all()
    return state


@app.post("/api/system/emergency-stop/clear")
async def clear_emergency_stop(request: Request):
    user = require_admin(request)
    return set_global_emergency_stop(
        session_factory,
        False,
        "",
        actor_user_id=user.id,
        code="manual_emergency_stop_clear",
    )


@app.post("/api/accounts")
async def add_account(request: Request, payload: AccountCreate):
    user = require_editor(request)
    try:
        encrypted = encrypt_secret(settings, payload.app_password.strip())
    except RuntimeError as exc:
        raise HTTPException(503, str(exc)) from exc
    handle = payload.handle.strip().lstrip("@")
    with session_factory() as session:
        account = BlueskyAccount(handle=handle, encrypted_app_password=encrypted, created_by_user_id=user.id)
        session.add(account)
        session.flush()
        session.add(
            AccountSettings(
                account_id=account.id,
                dry_run=settings.dry_run,
                max_follows_per_day=30,
                max_followers=settings.max_followers,
                follow_delay_min_seconds=60,
                follow_delay_max_seconds=180,
                recent_post_limit=settings.recent_post_limit,
                search_keywords=",".join(settings.search_keywords),
            )
        )
        if len(list_accounts(session)) == 1:
            session.query(TargetUser).filter(TargetUser.account_id.is_(None)).update({"account_id": account.id})
        session.commit()
        return {"id": account.id, "handle": account.handle, "status": account.status}


@app.post("/api/accounts/import-xlsx")
async def import_accounts_xlsx(request: Request, file: UploadFile = File(...)):
    user = require_editor(request)
    if not file.filename or not file.filename.casefold().endswith(".xlsx"):
        raise HTTPException(400, "Upload an .xlsx Excel workbook")
    try:
        workbook = load_workbook(io.BytesIO(await file.read()), read_only=True, data_only=True)
    except Exception as exc:
        raise HTTPException(400, "The uploaded file is not a valid .xlsx workbook") from exc
    try:
        sheet = workbook.active
        rows = sheet.iter_rows(values_only=True)
        headers = [str(value or "").strip().casefold() for value in next(rows, ())]
        if "handle" not in headers or "app_password" not in headers:
            raise HTTPException(400, "The workbook must contain handle and app_password columns")
        handle_index = headers.index("handle")
        password_index = headers.index("app_password")
        results = {"imported": [], "skipped": [], "invalid": []}
        with session_factory() as session:
            existing_handles = {account.handle.casefold() for account in list_accounts_for_user(session, user.id)}
            had_accounts = bool(existing_handles)
            for row_number, row in enumerate(rows, start=2):
                handle = str(row[handle_index] or "").strip().lstrip("@") if handle_index < len(row) else ""
                app_password = str(row[password_index] or "").strip() if password_index < len(row) else ""
                if not handle or not app_password:
                    results["invalid"].append({"row": row_number, "reason": "handle and app_password are required"})
                    continue
                normalized_handle = handle.casefold()
                if normalized_handle in existing_handles:
                    results["skipped"].append({"row": row_number, "handle": handle, "reason": "already connected"})
                    continue
                try:
                    encrypted = encrypt_secret(settings, app_password)
                except RuntimeError as exc:
                    raise HTTPException(503, str(exc)) from exc
                account = BlueskyAccount(handle=handle, encrypted_app_password=encrypted, created_by_user_id=user.id)
                session.add(account)
                session.flush()
                session.add(AccountSettings(
                    account_id=account.id,
                    dry_run=settings.dry_run,
                    max_follows_per_day=30,
                    max_followers=settings.max_followers,
                    follow_delay_min_seconds=60,
                    follow_delay_max_seconds=180,
                    recent_post_limit=settings.recent_post_limit,
                    search_keywords=",".join(settings.search_keywords),
                ))
                if not had_accounts:
                    session.query(TargetUser).filter(TargetUser.account_id.is_(None)).update({"account_id": account.id})
                had_accounts = True
                existing_handles.add(normalized_handle)
                results["imported"].append({"row": row_number, "handle": handle, "status": account.status})
            session.commit()
        return results
    finally:
        workbook.close()


@app.delete("/api/accounts/{account_id}")
async def delete_account(request: Request, account_id: int):
    require_editor(request)
    require_account(request, account_id)
    worker = worker_manager.get(account_id)
    if worker is not None and worker.status in {"running", "paused"}:
        await worker.stop()
    worker_manager.workers.pop(account_id, None)
    logs.pop(account_id, None)
    chat_clients.pop(account_id, None)
    inbox_sync_errors.pop(account_id, None)
    notification_sync_errors.pop(account_id, None)
    notification_sync_last_started.pop(account_id, None)
    with session_factory() as session:
        if not remove_account(session, account_id):
            raise HTTPException(404, "Account not found")
    return {"deleted": True, "account_id": account_id}


@app.get("/api/accounts/{account_id}/status")
async def account_status(request: Request, account_id: int):
    user = require_user(request)
    account = require_account(request, account_id)
    with session_factory() as session:
        account_settings = get_account_settings(session, account_id)
        if account_settings is None:
            raise HTTPException(500, "Account settings are missing")
        worker = worker_manager.get(account_id)
        return {
            "id": account.id,
            "handle": account.handle,
            "status": worker.status if worker else account.status,
            "last_error": worker.last_error if worker else account.last_error,
            "can_review_suspension": effective_role(user) == "admin",
            "dry_run": account_settings.dry_run,
            "target_counts": target_counts(session, account_id),
            "follows_today": count_follows_today(session, account_id),
            "max_follows_per_day": account_settings.max_follows_per_day,
            "keywords": [item.strip() for item in account_settings.search_keywords.split(",") if item.strip()],
            "max_followers": account_settings.max_followers,
            "follow_delay_min_seconds": account_settings.follow_delay_min_seconds,
            "follow_delay_max_seconds": account_settings.follow_delay_max_seconds,
            "logs": logs[account_id],
            "audit_events": [
                {
                    "created_at": event.created_at.isoformat(),
                    "event_type": event.event_type,
                    "severity": event.severity,
                    "category": event.category,
                    "code": event.code,
                    "http_status": event.http_status,
                    "retry_after": event.retry_after,
                    "message": event.message,
                }
                for event in session.scalars(
                    select(AuditEvent)
                    .where(
                        AuditEvent.account_id == account_id,
                        AuditEvent.account_owner_user_id == user.id,
                    )
                    .order_by(AuditEvent.created_at.desc())
                    .limit(50)
                )
            ],
            "targets": [
                {
                    "handle": item.handle,
                    "did": item.did,
                    "follower_count": item.follower_count,
                    "matched_keyword": item.matched_keyword,
                    "followed_at_timestamp": item.followed_at_timestamp.isoformat() if item.followed_at_timestamp else None,
                    "status": item.status,
                }
                for item in list_targets(session, 5000, account_id)
            ],
        }


def account_worker(request: Request, account_id: int):
    account = require_account(request, account_id)
    worker = worker_manager.get(account_id)
    if worker is None:
        with session_factory() as session:
            account_settings = get_account_settings(session, account_id)
        if account_settings is None:
            raise HTTPException(500, "Account settings are missing")
        try:
            app_password = decrypt_secret(settings, account.encrypted_app_password)
        except RuntimeError as exc:
            with session_factory() as session:
                stored_account = get_account(session, account_id)
                if stored_account is not None:
                    stored_account.status = "error"
                    stored_account.last_error = str(exc)[:500]
                    session.commit()
            raise HTTPException(503, str(exc)) from exc
        worker = worker_manager.create_worker(account, account_settings, app_password)
    return worker


@app.post("/api/accounts/{account_id}/start")
async def start_account(request: Request, account_id: int):
    user = require_editor(request)
    if is_global_emergency_stop_active(session_factory):
        raise HTTPException(423, "Global emergency stop is active; an administrator must review and clear it")
    account = require_account(request, account_id)
    if account.status == "suspended":
        raise HTTPException(
            423,
            "This account was reported as taken down and cannot be restarted; review the account with Bluesky first",
        )
    worker = account_worker(request, account_id)
    started = await worker.start()
    if started:
        record_audit_event(
            session_factory,
            "worker_started",
            "Worker started by team member",
            account_id=worker.account_id,
            account_handle=worker.client.handle,
            actor_user_id=user.id,
        )
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/pause")
async def pause_account(request: Request, account_id: int):
    user = require_editor(request)
    worker = account_worker(request, account_id)
    worker.pause()
    record_audit_event(
        session_factory,
        "worker_paused",
        "Worker paused by team member",
        account_id=worker.account_id,
        account_handle=worker.client.handle,
        actor_user_id=user.id,
    )
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/resume")
async def resume_account(request: Request, account_id: int):
    user = require_editor(request)
    if is_global_emergency_stop_active(session_factory):
        raise HTTPException(423, "Global emergency stop is active; an administrator must review and clear it")
    account = require_account(request, account_id)
    if account.status == "suspended":
        raise HTTPException(
            423,
            "This account was reported as taken down and cannot be resumed; review the account with Bluesky first",
        )
    worker = account_worker(request, account_id)
    worker.resume()
    record_audit_event(
        session_factory,
        "worker_resumed",
        "Worker resume requested by team member",
        account_id=worker.account_id,
        account_handle=worker.client.handle,
        actor_user_id=user.id,
    )
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/stop")
async def stop_account(request: Request, account_id: int):
    user = require_editor(request)
    worker = account_worker(request, account_id)
    await worker.stop()
    record_audit_event(
        session_factory,
        "worker_stopped",
        "Worker stopped by team member",
        account_id=worker.account_id,
        account_handle=worker.client.handle,
        actor_user_id=user.id,
    )
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/review-suspension")
async def review_account_suspension(request: Request, account_id: int, payload: AccountSuspensionReview):
    user = require_admin(request)
    account = require_account(request, account_id)
    if account.status != "suspended":
        raise HTTPException(409, "Account is not awaiting suspension review")
    review_note = payload.review_note.strip()
    if len(review_note) < 10:
        raise HTTPException(400, "Review note must contain at least 10 non-whitespace characters")
    worker = worker_manager.get(account_id)
    if worker is not None:
        await worker.stop()
        worker_manager.workers.pop(account_id, None)
    with session_factory() as session:
        stored_account = get_account(session, account_id)
        if stored_account is None:
            raise HTTPException(404, "Account not found")
        stored_account.status = "stopped"
        stored_account.last_error = None
        stored_account.chat_sync_status = "ok"
        stored_account.chat_sync_error = None
        stored_account.chat_sync_retry_at = None
        session.commit()
    record_audit_event(
        session_factory,
        "suspension_reviewed",
        review_note,
        account_id=account_id,
        account_handle=account.handle,
        actor_user_id=user.id,
        severity="warning",
        code="manual_review",
    )
    return {"status": "stopped", "requires_manual_start": True}


@app.post("/api/accounts/{account_id}/settings")
async def update_account_settings(request: Request, account_id: int, payload: AccountSettingsUpdate):
    require_editor(request)
    account = require_account(request, account_id)
    if payload.follow_delay_max_seconds < payload.follow_delay_min_seconds:
        raise HTTPException(400, "Maximum delay must be greater than or equal to minimum delay")
    with session_factory() as session:
        account_settings = get_account_settings(session, account_id)
        if account_settings is None:
            raise HTTPException(500, "Account settings are missing")
        account_settings.search_keywords = ",".join(item.strip() for item in payload.keywords if item.strip())
        account_settings.max_followers = payload.max_followers
        account_settings.dry_run = payload.dry_run
        account_settings.max_follows_per_day = payload.max_follows_per_day
        account_settings.follow_delay_min_seconds = payload.follow_delay_min_seconds
        account_settings.follow_delay_max_seconds = payload.follow_delay_max_seconds
        session.commit()
    if worker_manager.get(account_id) is not None:
        await worker_manager.get(account_id).stop()
        worker_manager.workers.pop(account_id, None)
    return {"account_id": account.id, "status": "updated"}


@app.post("/api/accounts/{account_id}/import-likers")
async def import_likers(request: Request, account_id: int, payload: PostImport):
    require_editor(request)
    if is_global_emergency_stop_active(session_factory):
        raise HTTPException(423, "Post import is blocked while the global emergency stop is active")
    account = require_account(request, account_id)
    if account.status == "suspended":
        raise HTTPException(423, "Post import is blocked pending administrator suspension review")
    worker = account_worker(request, account_id)
    was_running = worker.status == "running"
    if was_running:
        await worker.stop()
    try:
        matched = await worker.import_post_likers(payload.post_url)
        return {"matched": matched, "source": "post", "worker_resumed": was_running}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        logger.exception("Bluesky post import failed")
        raise HTTPException(502, f"Could not import likers: {exc}") from exc
    finally:
        if was_running:
            await worker.start()


@app.get("/api/accounts/{account_id}/export.csv")
async def export_csv(request: Request, account_id: int):
    require_account(request, account_id)
    with session_factory() as session:
        targets = list_targets(session, 5000, account_id)
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow(["handle", "did", "follower_count", "matched_keyword", "followed_at_timestamp", "status"])
    for item in targets:
        writer.writerow([item.handle, item.did, item.follower_count, item.matched_keyword, item.followed_at_timestamp, item.status])
    return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=account-{account_id}-targets.csv"})
