import csv
import io
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from sqlalchemy import func, select

from auth import create_session, current_user, decrypt_secret, encrypt_secret, hash_password, verify_password
from config import get_settings
from database import (
    AccountSettings,
    BlueskyAccount,
    User,
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


def require_user(request: Request) -> User:
    with session_factory() as session:
        return current_user(request, session)


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
        user = User(username=payload.username.strip().casefold(), password_hash=hash_password(payload.password), is_admin=True)
        session.add(user)
        session.commit()
        token = create_session(session, user, settings)
    response = RedirectResponse("/", status_code=303)
    response.set_cookie("session", token, httponly=True, samesite="lax", max_age=settings.session_max_age_hours * 3600)
    return response


@app.post("/api/auth/login")
async def login(payload: LoginRequest):
    with session_factory() as session:
        user = get_user_by_username(session, payload.username.strip())
        if user is None or not verify_password(payload.password, user.password_hash):
            raise HTTPException(401, "Invalid username or password")
        token = create_session(session, user, settings)
    response = RedirectResponse("/", status_code=303)
    response.set_cookie("session", token, httponly=True, samesite="lax", max_age=settings.session_max_age_hours * 3600)
    return response


@app.post("/api/auth/logout")
async def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session")
    return response


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
    user = require_user(request)
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


@app.delete("/api/accounts/{account_id}")
async def delete_account(request: Request, account_id: int):
    require_account(request, account_id)
    worker = worker_manager.get(account_id)
    if worker is not None and worker.status in {"running", "paused"}:
        await worker.stop()
    worker_manager.workers.pop(account_id, None)
    logs.pop(account_id, None)
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
    worker = account_worker(request, account_id)
    await worker.start()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/pause")
async def pause_account(request: Request, account_id: int):
    worker = account_worker(request, account_id)
    worker.pause()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/resume")
async def resume_account(request: Request, account_id: int):
    worker = account_worker(request, account_id)
    worker.resume()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/stop")
async def stop_account(request: Request, account_id: int):
    worker = account_worker(request, account_id)
    await worker.stop()
    return {"status": worker.status}


@app.post("/api/accounts/{account_id}/settings")
async def update_account_settings(request: Request, account_id: int, payload: AccountSettingsUpdate):
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
    worker = account_worker(request, account_id)
    try:
        return {"matched": await worker.import_post_likers(payload.post_url)}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:
        logger.exception("Bluesky post import failed")
        raise HTTPException(502, f"Could not import likers: {exc}") from exc


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
