import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from atproto import models

from bsky_client import BlueSkyClient


class ChatClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = BlueSkyClient("example.bsky.social", "app-password")
        self.proxy = SimpleNamespace(
            invoke_query=Mock(
                side_effect=[
                    SimpleNamespace(content={"convos": []}),
                    SimpleNamespace(content={"messages": []}),
                ]
            ),
            invoke_procedure=Mock(
                side_effect=[
                    SimpleNamespace(content={"message": {}}),
                    SimpleNamespace(success=True),
                ]
            ),
        )
        self.chat_client_patch = patch.object(self.client, "_chat_client", return_value=self.proxy)
        self.chat_client_patch.start()
        self.addCleanup(self.chat_client_patch.stop)

    def test_chat_calls_use_generated_request_models(self) -> None:
        self.client.chat_list_conversations(limit=200, cursor="page-2")
        self.client.chat_get_messages("convo-1", limit=120, cursor="messages-2")
        self.client.chat_send_message("convo-1", "Hello")
        self.client.chat_mark_read("convo-1", "message-1")

        list_params = self.proxy.invoke_query.call_args_list[0].kwargs["params"]
        message_params = self.proxy.invoke_query.call_args_list[1].kwargs["params"]
        send_data = self.proxy.invoke_procedure.call_args_list[0].kwargs["data"]
        read_data = self.proxy.invoke_procedure.call_args_list[1].kwargs["data"]

        self.assertIsInstance(list_params, models.ChatBskyConvoListConvos.Params)
        self.assertEqual(list_params.limit, 100)
        self.assertEqual(list_params.cursor, "page-2")
        self.assertIsInstance(message_params, models.ChatBskyConvoGetMessages.Params)
        self.assertEqual(message_params.convo_id, "convo-1")
        self.assertEqual(message_params.limit, 100)
        self.assertEqual(message_params.cursor, "messages-2")
        self.assertIsInstance(send_data, models.ChatBskyConvoSendMessage.Data)
        self.assertEqual(send_data.convo_id, "convo-1")
        self.assertEqual(send_data.message.text, "Hello")
        self.assertEqual(
            self.proxy.invoke_procedure.call_args_list[0].kwargs["input_encoding"],
            "application/json",
        )
        self.assertEqual(
            self.proxy.invoke_procedure.call_args_list[0].kwargs["output_encoding"],
            "application/json",
        )
        self.assertIsInstance(read_data, models.ChatBskyConvoUpdateRead.Data)
        self.assertEqual(read_data.convo_id, "convo-1")
        self.assertEqual(read_data.message_id, "message-1")
        self.assertEqual(
            self.proxy.invoke_procedure.call_args_list[1].kwargs["input_encoding"],
            "application/json",
        )
        self.assertEqual(
            self.proxy.invoke_procedure.call_args_list[1].kwargs["output_encoding"],
            "application/json",
        )

    def test_notification_calls_use_generated_models(self) -> None:
        response = SimpleNamespace(notifications=[], cursor="next-page")
        notification_api = SimpleNamespace(
            list_notifications=Mock(return_value=response),
            update_seen=Mock(return_value=True),
        )
        with patch.object(self.client, "_notification_client", return_value=notification_api):
            result = self.client.list_notifications(limit=200, cursor="page-2")
            self.client.mark_notifications_seen("2026-11-17T18:00:00Z")

        params = notification_api.list_notifications.call_args.args[0]
        seen_data = notification_api.update_seen.call_args.args[0]
        self.assertIs(result, response)
        self.assertIsInstance(params, models.AppBskyNotificationListNotifications.Params)
        self.assertEqual(params.limit, 100)
        self.assertEqual(params.cursor, "page-2")
        self.assertIsInstance(seen_data, models.AppBskyNotificationUpdateSeen.Data)
        self.assertEqual(seen_data.seen_at, "2026-11-17T18:00:00Z")


if __name__ == "__main__":
    unittest.main()
