from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, func, inspect, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from config import Settings


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(120), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(512))
    is_admin: Mapped[bool] = mapped_column(default=False)
    role: Mapped[str] = mapped_column(String(32), default="member", index=True)
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SessionToken(Base):
    __tablename__ = "session_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class TeamInvite(Base):
    __tablename__ = "team_invites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    created_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class BlueskyAccount(Base):
    __tablename__ = "bluesky_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    handle: Mapped[str] = mapped_column(String(255), index=True)
    did: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    encrypted_app_password: Mapped[str] = mapped_column(String(1024))
    status: Mapped[str] = mapped_column(String(32), default="stopped", index=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_by_user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class AccountSettings(Base):
    __tablename__ = "account_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("bluesky_accounts.id"), unique=True, index=True)
    dry_run: Mapped[bool] = mapped_column(default=True)
    max_follows_per_day: Mapped[int] = mapped_column(Integer, default=30)
    max_followers: Mapped[int] = mapped_column(Integer, default=300)
    follow_delay_min_seconds: Mapped[int] = mapped_column(Integer, default=60)
    follow_delay_max_seconds: Mapped[int] = mapped_column(Integer, default=180)
    recent_post_limit: Mapped[int] = mapped_column(Integer, default=10)
    search_keywords: Mapped[str] = mapped_column(String(4000), default="furry art,3d model,2d model,vtuber")


class TargetUser(Base):
    __tablename__ = "target_users"
    __table_args__ = (UniqueConstraint("account_id", "did", name="uq_target_account_did"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int | None] = mapped_column(ForeignKey("bluesky_accounts.id"), nullable=True, index=True)
    handle: Mapped[str] = mapped_column(String(255), index=True)
    did: Mapped[str] = mapped_column(String(255), index=True)
    follower_count: Mapped[int] = mapped_column(Integer, default=0)
    matched_keyword: Mapped[str] = mapped_column(String(255), default="")
    followed_at_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="discovered", index=True)


def create_database(settings: Settings) -> tuple[Any, sessionmaker[Session]]:
    if settings.require_persistent_database and settings.database_url.startswith("sqlite"):
        raise RuntimeError("REQUIRE_PERSISTENT_DATABASE is enabled; configure Railway DATABASE_URL with PostgreSQL")
    if settings.database_url.startswith("sqlite"):
        settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
    engine = create_engine(settings.database_url, connect_args=connect_args)
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        user_columns = {column["name"] for column in inspect(engine).get_columns("users")}
        if "role" not in user_columns:
            connection.execute(text("ALTER TABLE users ADD COLUMN role VARCHAR(32) DEFAULT 'member'"))
        if "is_active" not in user_columns:
            connection.execute(text("ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT TRUE"))
        connection.execute(text("UPDATE users SET role = 'admin' WHERE is_admin = TRUE AND (role IS NULL OR role = 'member')"))
        connection.execute(text("UPDATE users SET is_active = TRUE WHERE is_active IS NULL"))
    if settings.database_url.startswith("sqlite"):
        with engine.begin() as connection:
            columns = {column["name"] for column in inspect(engine).get_columns("target_users")}
            if "account_id" not in columns:
                connection.execute(text("ALTER TABLE target_users ADD COLUMN account_id INTEGER"))
    return engine, sessionmaker(bind=engine, expire_on_commit=False)


def get_user_by_username(session: Session, username: str) -> User | None:
    return session.scalar(select(User).where(User.username == username.casefold()))


def get_user_by_session_hash(session: Session, token_hash: str) -> User | None:
    token = session.scalar(select(SessionToken).where(SessionToken.token_hash == token_hash))
    if token is None:
        return None
    expires_at = token.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= datetime.now(timezone.utc):
        return None
    return session.get(User, token.user_id)


def list_accounts(session: Session) -> list[BlueskyAccount]:
    return list(session.scalars(select(BlueskyAccount).order_by(BlueskyAccount.id)))


def get_account(session: Session, account_id: int) -> BlueskyAccount | None:
    return session.get(BlueskyAccount, account_id)


def get_account_settings(session: Session, account_id: int) -> AccountSettings | None:
    return session.scalar(select(AccountSettings).where(AccountSettings.account_id == account_id))


def update_account_status(session: Session, account_id: int, status: str, last_error: str | None = None) -> None:
    account = session.get(BlueskyAccount, account_id)
    if account is not None:
        account.status = status
        account.last_error = last_error[:500] if last_error else None
        session.commit()


def remove_account(session: Session, account_id: int) -> bool:
    account = session.get(BlueskyAccount, account_id)
    if account is None:
        return False
    session.query(TargetUser).filter(TargetUser.account_id == account_id).delete(synchronize_session=False)
    session.query(AccountSettings).filter(AccountSettings.account_id == account_id).delete(synchronize_session=False)
    session.delete(account)
    session.commit()
    return True


def target_exists(session: Session, did: str, handle: str, account_id: int | None = None) -> bool:
    query = select(TargetUser.id).where((TargetUser.did == did) | (TargetUser.handle == handle))
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    return session.scalar(query) is not None


def save_discovered_user(
    session: Session,
    *,
    account_id: int | None,
    handle: str,
    did: str,
    follower_count: int,
    matched_keyword: str,
) -> TargetUser:
    query = select(TargetUser).where(TargetUser.did == did)
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    user = session.scalar(query)
    if user is None:
        user = TargetUser(account_id=account_id, handle=handle, did=did, follower_count=follower_count, matched_keyword=matched_keyword)
        session.add(user)
    else:
        user.follower_count = follower_count
        user.matched_keyword = matched_keyword
    session.commit()
    return user


def mark_followed(session: Session, did: str, account_id: int | None = None) -> None:
    query = select(TargetUser).where(TargetUser.did == did)
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    user = session.scalar(query)
    if user:
        user.status = "followed"
        user.followed_at_timestamp = datetime.now(timezone.utc)
        session.commit()


def mark_failed(session: Session, did: str, reason: str = "failed", account_id: int | None = None) -> None:
    query = select(TargetUser).where(TargetUser.did == did)
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    user = session.scalar(query)
    if user:
        user.status = reason[:32]
        session.commit()


def list_targets(session: Session, limit: int | None = None, account_id: int | None = None) -> list[TargetUser]:
    query = select(TargetUser).order_by(TargetUser.id.desc())
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    if limit is not None:
        query = query.limit(limit)
    return list(session.scalars(query))


def target_counts(session: Session, account_id: int | None = None) -> dict[str, int]:
    query = select(TargetUser.status, func.count(TargetUser.id)).group_by(TargetUser.status)
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    rows = session.execute(query).all()
    counts = {status: int(total) for status, total in rows}
    counts["total"] = sum(counts.values())
    counts["followed"] = counts.get("followed", 0)
    counts["pending"] = counts["total"] - counts["followed"]
    return counts


def count_follows_today(session: Session, account_id: int | None = None) -> int:
    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    query = select(func.count(TargetUser.id)).where(TargetUser.followed_at_timestamp >= start)
    if account_id is not None:
        query = query.where(TargetUser.account_id == account_id)
    return int(session.scalar(query) or 0)
