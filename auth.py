import base64
import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet, InvalidToken
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from config import Settings
from database import SessionToken, User, get_user_by_session_hash


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
    return f"pbkdf2_sha256$310000${base64.urlsafe_b64encode(salt).decode()}${base64.urlsafe_b64encode(digest).decode()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algorithm, rounds, salt, expected = stored.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        actual = hashlib.pbkdf2_hmac("sha256", password.encode(), base64.urlsafe_b64decode(salt), int(rounds))
        return hmac.compare_digest(base64.urlsafe_b64decode(expected), actual)
    except (ValueError, TypeError):
        return False


def _fernet(settings: Settings) -> Fernet:
    if not settings.app_encryption_key:
        raise RuntimeError("APP_ENCRYPTION_KEY must be configured before connecting a Bluesky account")
    try:
        return Fernet(settings.app_encryption_key.encode())
    except (ValueError, TypeError) as exc:
        raise RuntimeError("APP_ENCRYPTION_KEY must be a valid Fernet key") from exc


def encrypt_secret(settings: Settings, value: str) -> str:
    return _fernet(settings).encrypt(value.encode()).decode()


def decrypt_secret(settings: Settings, value: str) -> str:
    try:
        return _fernet(settings).decrypt(value.encode()).decode()
    except InvalidToken as exc:
        raise RuntimeError("Stored account credential could not be decrypted") from exc


def create_session(session: Session, user: User, settings: Settings) -> str:
    raw_token = secrets.token_urlsafe(48)
    token = SessionToken(
        token_hash=hashlib.sha256(raw_token.encode()).hexdigest(),
        user_id=user.id,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=settings.session_max_age_hours),
    )
    session.add(token)
    session.commit()
    return raw_token


def revoke_session(session: Session, raw_token: str | None) -> None:
    if not raw_token:
        return
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    session.query(SessionToken).filter(SessionToken.token_hash == token_hash).delete(synchronize_session=False)
    session.commit()


def revoke_user_sessions(session: Session, user_id: int) -> None:
    session.query(SessionToken).filter(SessionToken.user_id == user_id).delete(synchronize_session=False)
    session.commit()


def current_user(request: Request, session: Session) -> User:
    raw_token = request.cookies.get("session")
    if not raw_token:
        raise HTTPException(401, "Please log in")
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    user = get_user_by_session_hash(session, token_hash)
    if user is None or user.is_active is False:
        raise HTTPException(401, "Session expired; please log in again")
    return user
