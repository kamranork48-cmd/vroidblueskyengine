import csv
import asyncio
import hashlib
import io
import logging
import secrets
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from openpyxl import load_workbook

from auth import create_session, current_user, decrypt_secret, encrypt_secret, hash_password, revoke_session, revoke_user_sessions, verify_password
from bsky_client import BlueSkyClient
from config import get_settings
from database import (
    AccountSettings,
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
    list_targets,
    target_counts,
    count_follows_today,
    TargetUser,
    remove_account,
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
inbox_sync_cooldown_until: dict[int, float] = {}
chat_clients: dict[int, BlueSkyClient] = {}


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
    logger.info("BlueSky Signal Desk ready")
    yield
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


class ChatMessageCreate(BaseModel):
    text: str = Field(min_length=1, max_length=10000)


def _chat_client_for_account(account_id: int) -> tuple[BlueskyAccount, BlueSkyClient]:
    with session_factory() as session:
        account = get_account(session, account_id)
        if account is None:
            raise HTTPException(404, "Account not found")
        client = chat_clients.get(account_id)
        if client is None:
            try:
                app_password = decrypt_secret(settings, account.encrypted_app_password)
            except RuntimeError as exc:
                raise HTTPException(503, str(exc)) from exc
            client = BlueSkyClient(account.handle, app_password)
            chat_clients[account_id] = client
    return account, client


def _parse_chat_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _chat_profile(profile: object) -> dict[str, str]:
    if not isinstance(profile, dict):
        return {"did": "", "handle": "", "display_name": "", "avatar": ""}
    return {
        "did": str(profile.get("did") or ""),
        "handle": str(profile.get("handle") or ""),
        "display_name": str(profile.get("displayName") or profile.get("display_name") or ""),
        "avatar": str(profile.get("avatar") or ""),
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
    if time.monotonic() < inbox_sync_cooldown_until.get(account_id, 0):
        return
    account, client = _chat_client_for_account(account_id)
    try:
        client._require_login()
    except Exception as exc:
        chat_clients.pop(account_id, None)
        if "429" in str(exc):
            inbox_sync_cooldown_until[account_id] = time.monotonic() + 60
        logger.exception("Inbox authentication failed for account %s", account_id)
        raise HTTPException(502, "Could not authenticate the selected Bluesky account for chat") from exc
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


async def _sync_account_in_background(account_id: int) -> None:
    try:
        await asyncio.to_thread(_sync_account_conversations, account_id)
        inbox_sync_errors.pop(account_id, None)
    except Exception as exc:
        message = "Could not synchronize this account's inbox"
        if "429" in str(exc):
            inbox_sync_cooldown_until[account_id] = time.monotonic() + 60
        logger.exception("Inbox synchronization failed for account %s", account_id)
        inbox_sync_errors[account_id] = message
    finally:
        inbox_sync_tasks.pop(account_id, None)


def _schedule_inbox_sync(account_ids: list[int]) -> list[int]:
    scheduled: list[int] = []
    for account_id in account_ids:
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
    require_user(request)
    with session_factory() as session:
        account = get_account(session, account_id)
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
    require_user(request)
    if account_id is not None:
        require_account(request, account_id)
        account_ids = [account_id]
    else:
        with session_factory() as session:
            account_ids = [account.id for account in list_accounts(session)]
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
        account_handles = {item.id: item.handle for item in list_accounts(session)}
        return {
            "conversations": [
                {**_conversation_response(session, item), "account_handle": account_handles.get(item.account_id, "")}
                for item in conversations
            ],
            "syncing_account_ids": scheduled_accounts,
            "errors": [
                {"account_id": current_account_id, "error": inbox_sync_errors[current_account_id]}
                for current_account_id in account_ids
                if current_account_id in inbox_sync_errors
            ],
        }


@app.get("/api/inbox/{account_id}/{conversation_id}/messages")
async def inbox_messages(request: Request, account_id: int, conversation_id: str):
    require_user(request)
    require_account(request, account_id)
    account, client = _chat_client_for_account(account_id)
    try:
        client._require_login()
    except Exception as exc:
        logger.exception("Inbox authentication failed for account %s", account_id)
        raise HTTPException(502, "Could not authenticate the selected Bluesky account for chat") from exc
    account_did = str(getattr(getattr(client.client, "me", None), "did", "") or "")
    try:
        result = await asyncio.to_thread(client.chat_get_messages, conversation_id)
    except Exception as exc:
        logger.exception("Could not load inbox messages for account %s", account_id)
        raise HTTPException(502, "Could not load this Bluesky conversation") from exc
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
        for payload in messages:
            if isinstance(payload, dict):
                _store_chat_message(session, account_id, conversation_id, payload, account_did)
        session.commit()
        stored = list(session.scalars(
            select(ChatMessage).where(
                ChatMessage.account_id == account_id,
                ChatMessage.conversation_id == conversation_id,
            ).order_by(ChatMessage.sent_at.asc().nullslast(), ChatMessage.id.asc())
        ))
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
    require_user(request)
    with session_factory() as session:
        return [
            {
                "id": account.id,
                "handle": account.handle,
                "status": worker_manager.get(account.id).status if worker_manager.get(account.id) else account.status,
                "last_error": account.last_error,
            }
            for account in list_accounts(session)
        ]


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
            existing_handles = {account.handle.casefold() for account in list_accounts(session)}
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
    inbox_sync_cooldown_until.pop(account_id, None)
    inbox_sync_errors.pop(account_id, None)
    with session_factory() as session:
        if not remove_account(session, account_id):
            raise HTTPException(404, "Account not found")
    return {"deleted": True, "account_id": account_id}


@app.get("/api/accounts/{account_id}/status")
async def account_status(request: Request, account_id: int):
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
            "dry_run": account_settings.dry_run,
            "target_counts": target_counts(session, account_id),
            "follows_today": count_follows_today(session, account_id),
            "max_follows_per_day": account_settings.max_follows_per_day,
            "keywords": [item.strip() for item in account_settings.search_keywords.split(",") if item.strip()],
            "max_followers": account_settings.max_followers,
            "follow_delay_min_seconds": account_settings.follow_delay_min_seconds,
            "follow_delay_max_seconds": account_settings.follow_delay_max_seconds,
            "logs": logs[account_id],
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
    require_editor(request)
    worker = account_worker(request, account_id)
    await worker.start()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/pause")
async def pause_account(request: Request, account_id: int):
    require_editor(request)
    worker = account_worker(request, account_id)
    worker.pause()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/resume")
async def resume_account(request: Request, account_id: int):
    require_editor(request)
    worker = account_worker(request, account_id)
    worker.resume()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/stop")
async def stop_account(request: Request, account_id: int):
    require_editor(request)
    worker = account_worker(request, account_id)
    await worker.stop()
    return {"status": worker.status}


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
