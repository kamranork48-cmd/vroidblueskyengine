import logging
import re
from dataclasses import dataclass
from typing import Any

from atproto import Client, models

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Profile:
    handle: str
    did: str
    display_name: str
    bio: str
    followers_count: int


@dataclass(slots=True)
class ChatResult:
    content: dict[str, Any]


class BlueSkyClient:
    def __init__(self, handle: str, app_password: str) -> None:
        self.handle = handle.strip().lstrip("@")
        self.app_password = app_password
        self.client = Client()
        self.logged_in = False

    def login(self) -> None:
        if not self.handle:
            raise ValueError("BSKY_HANDLE is empty; use a handle such as name.bsky.social")
        if self.handle in {"your-handle.bsky.social", "example.bsky.social"}:
            raise ValueError("BSKY_HANDLE is still a placeholder; replace it with your real BlueSky handle")
        if not self.app_password:
            raise ValueError("BSKY_APP_PASSWORD is empty; create and use a BlueSky App Password")
        try:
            self.client.login(self.handle, self.app_password)
        except Exception as exc:
            raise RuntimeError(
                "BlueSky login failed. Check that BSKY_HANDLE is like name.bsky.social "
                "without an @ prefix and that BSKY_APP_PASSWORD is a valid App Password."
            ) from exc
        self.logged_in = True
        logger.info("Authenticated as %s", self.handle)

    def _require_login(self) -> None:
        if not self.logged_in:
            self.login()

    def search_posts(self, query: str, limit: int = 25) -> list[Any]:
        self._require_login()
        response = self.client.app.bsky.feed.search_posts({'q': query, 'limit': limit})
        return list(getattr(response, "posts", []))

    def post_likers(self, post_uri: str, limit: int = 100) -> list[Any]:
        self._require_login()
        response = self.client.app.bsky.feed.get_likes({'uri': post_uri, 'limit': limit})
        return list(getattr(response, "likes", []))

    def post_uri_from_url(self, post_url: str) -> str:
        match = re.fullmatch(r"https?://bsky\.app/profile/([^/]+)/post/([^/?#]+)", post_url.strip())
        if not match:
            raise ValueError("Use a Bluesky post URL like https://bsky.app/profile/name.bsky.social/post/abc123")
        handle, rkey = match.groups()
        self._require_login()
        response = self.client.com.atproto.identity.resolve_handle({'handle': handle})
        did = getattr(response, "did", None)
        if not did:
            raise ValueError("Could not resolve the Bluesky profile in that URL")
        return f"at://{did}/app.bsky.feed.post/{rkey}"

    def get_profile(self, actor: str) -> Profile:
        self._require_login()
        profile = self.client.app.bsky.actor.get_profile({'actor': actor})
        return Profile(
            handle=profile.handle,
            did=profile.did,
            display_name=getattr(profile, "display_name", "") or "",
            bio=getattr(profile, "description", "") or "",
            followers_count=int(getattr(profile, "followers_count", 0) or 0),
        )

    def recent_post_text(self, actor: str, limit: int = 10) -> str:
        self._require_login()
        response = self.client.app.bsky.feed.get_author_feed({'actor': actor, 'limit': limit})
        texts: list[str] = []
        for item in getattr(response, "feed", []):
            record = getattr(getattr(item, "post", None), "record", None)
            text = getattr(record, "text", "") if record else ""
            if text:
                texts.append(text)
        return " ".join(texts)

    def follow(self, did: str) -> str:
        self._require_login()
        record = self.client.follow(did)
        return str(getattr(record, "uri", record))

    def _chat_client(self) -> Any:
        self._require_login()
        return self.client.with_bsky_chat_proxy()

    def chat_list_conversations(self, limit: int = 50, cursor: str | None = None) -> ChatResult:
        params = models.ChatBskyConvoListConvos.Params(
            limit=min(max(limit, 1), 100),
            cursor=cursor,
        )
        response = self._chat_client().invoke_query("chat.bsky.convo.listConvos", params=params)
        if not isinstance(response.content, dict):
            raise RuntimeError("Bluesky returned an invalid conversation response")
        return ChatResult(response.content)

    def chat_get_messages(self, conversation_id: str, limit: int = 100, cursor: str | None = None) -> ChatResult:
        params = models.ChatBskyConvoGetMessages.Params(
            convoId=conversation_id,
            limit=min(max(limit, 1), 100),
            cursor=cursor,
        )
        response = self._chat_client().invoke_query("chat.bsky.convo.getMessages", params=params)
        if not isinstance(response.content, dict):
            raise RuntimeError("Bluesky returned an invalid message response")
        return ChatResult(response.content)

    def chat_send_message(self, conversation_id: str, text: str) -> ChatResult:
        response = self._chat_client().invoke_procedure(
            "chat.bsky.convo.sendMessage",
            data=models.ChatBskyConvoSendMessage.Data(
                convoId=conversation_id,
                message=models.ChatBskyConvoDefs.MessageInput(text=text),
            ),
        )
        if not isinstance(response.content, dict):
            raise RuntimeError("Bluesky returned an invalid sent-message response")
        return ChatResult(response.content)

    def chat_mark_read(self, conversation_id: str, message_id: str) -> None:
        response = self._chat_client().invoke_procedure(
            "chat.bsky.convo.updateRead",
            data=models.ChatBskyConvoUpdateRead.Data(convoId=conversation_id, messageId=message_id),
        )
        if not response.success:
            raise RuntimeError("Bluesky could not mark the conversation as read")
