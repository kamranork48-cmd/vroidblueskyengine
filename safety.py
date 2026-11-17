from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from database import AuditEvent, BlueskyAccount, SystemControl


@dataclass(frozen=True)
class BlueskyErrorInfo:
    category: str
    code: str
    status_code: int | None
    retry_after: str
    rate_limit_reset: str
    message: str


def classify_bluesky_error(error: Exception) -> BlueskyErrorInfo:
    current: BaseException | None = error
    response = None
    while current is not None:
        response = getattr(current, "response", None)
        if response is not None:
            break
        current = current.__cause__ or current.__context__
    status_code = getattr(response, "status_code", None)
    content = getattr(response, "content", None)
    code = str(getattr(content, "error", "") or "")
    message = str(getattr(content, "message", "") or (str(current) if current else str(error)))
    headers: Any = getattr(response, "headers", {}) or {}
    retry_after = str(headers.get("retry-after", headers.get("Retry-After", "")) or "")
    rate_limit_reset = str(headers.get("ratelimit-reset", headers.get("RateLimit-Reset", "")) or "")
    normalized_code = code.casefold()
    normalized_message = message.casefold()

    if normalized_code == "accounttakedown" or "account has been taken down" in normalized_message:
        category = "suspended"
    elif status_code == 429 or "ratelimitexceeded" in normalized_code or "rate limit" in normalized_message:
        category = "rate_limited"
    elif status_code == 404:
        category = "not_found"
    elif status_code == 401 or normalized_code in {"authenticationrequired", "unauthorized"}:
        category = "authentication"
    elif status_code == 403 or "forbidden" in normalized_message:
        category = "restricted"
    else:
        category = "transient"

    return BlueskyErrorInfo(category, code, status_code, retry_after, rate_limit_reset, message[:1000])


def record_audit_event(
    session_factory: sessionmaker[Session],
    event_type: str,
    message: str,
    *,
    account_id: int | None = None,
    account_handle: str = "",
    actor_user_id: int | None = None,
    severity: str = "info",
    category: str = "",
    code: str = "",
    http_status: int | None = None,
    retry_after: str = "",
) -> None:
    with session_factory() as session:
        account = session.get(BlueskyAccount, account_id) if account_id is not None else None
        session.add(
            AuditEvent(
                account_id=account_id,
                account_owner_user_id=account.created_by_user_id if account else None,
                account_handle=account_handle,
                actor_user_id=actor_user_id,
                event_type=event_type,
                severity=severity,
                category=category[:40],
                code=code[:100],
                http_status=http_status,
                retry_after=retry_after[:100],
                message=message[:1000],
            )
        )
        session.commit()


def is_global_emergency_stop_active(session_factory: sessionmaker[Session]) -> bool:
    with session_factory() as session:
        control = session.get(SystemControl, 1)
        return bool(control and control.emergency_stop_active)


def get_global_control(session_factory: sessionmaker[Session]) -> dict[str, Any]:
    with session_factory() as session:
        control = session.get(SystemControl, 1)
        if control is None:
            return {"active": False, "reason": "", "updated_at": None}
        return {
            "active": control.emergency_stop_active,
            "reason": control.emergency_stop_reason,
            "updated_at": control.updated_at.isoformat() if control.updated_at else None,
        }


def set_global_emergency_stop(
    session_factory: sessionmaker[Session],
    active: bool,
    reason: str,
    *,
    actor_user_id: int | None = None,
    account_id: int | None = None,
    account_handle: str = "",
    code: str = "",
    category: str = "",
    http_status: int | None = None,
    retry_after: str = "",
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    with session_factory() as session:
        account = session.get(BlueskyAccount, account_id) if account_id is not None else None
        control = session.get(SystemControl, 1)
        if control is None:
            control = SystemControl(id=1)
            session.add(control)
        control.emergency_stop_active = active
        control.emergency_stop_reason = reason[:500] if active else ""
        control.updated_by_user_id = actor_user_id
        control.updated_at = now
        session.add(
            AuditEvent(
                account_id=account_id,
                account_owner_user_id=account.created_by_user_id if account else None,
                account_handle=account_handle,
                actor_user_id=actor_user_id,
                event_type="emergency_stop_activated" if active else "emergency_stop_cleared",
                severity="critical" if active else "warning",
                category=category[:40],
                code=code[:100],
                http_status=http_status,
                retry_after=retry_after[:100],
                message=(reason or ("Emergency stop cleared" if not active else "Emergency stop activated"))[:1000],
            )
        )
        session.commit()
        return {"active": active, "reason": control.emergency_stop_reason, "updated_at": now.isoformat()}
