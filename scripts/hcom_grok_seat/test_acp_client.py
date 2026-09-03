from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from .acp_client import (
    AcpProtocolError,
    AcpRemoteError,
    AcpTransportClosed,
    AsyncAcpClient,
    cancelled_permission_result,
)


FAKE_SERVER = r'''
import json, sys
held = []
def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    msg = json.loads(line)
    method = msg.get("method")
    rid = msg.get("id")
    if method == "hold":
        held.append(rid)
        if len(held) == 2:
            send({"jsonrpc":"2.0","id":held[1],"result":{"value":2}})
            send({"jsonrpc":"2.0","method":"session/update","params":{"text":"secret body"}})
            send({"jsonrpc":"2.0","id":held[0],"result":{"value":1}})
    elif method == "permission":
        send({"jsonrpc":"2.0","id":"reverse-1","method":"session/request_permission","params":{"sessionId":"s","options":[]}})
    elif rid == "reverse-1":
        send({"jsonrpc":"2.0","id":1,"result":{"reverse":msg}})
    elif method == "unknown_reverse":
        send({"jsonrpc":"2.0","id":"reverse-2","method":"x/unknown","params":{}})
    elif rid == "reverse-2":
        send({"jsonrpc":"2.0","id":1,"result":{"reverse":msg}})
    elif method == "hang":
        pass
    elif method == "exit":
        sys.exit(7)
    else:
        send({"jsonrpc":"2.0","id":rid,"result":{"method":method}})
'''


def server_sending(frame: object) -> str:
    encoded = repr(json.dumps(frame, separators=(",", ":")) + "\n")
    return (
        "import sys; sys.stdin.readline(); "
        f"sys.stdout.write({encoded}); sys.stdout.flush(); sys.stdin.read()"
    )


class AsyncAcpClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.clients: list[AsyncAcpClient] = []

    async def asyncTearDown(self) -> None:
        for client in self.clients:
            await client.close_transport()
        self.temp.cleanup()

    async def spawn(self, source: str = FAKE_SERVER, **kwargs: object) -> AsyncAcpClient:
        reverse_handlers = kwargs.pop(
            "reverse_handlers",
            {"session/request_permission": cancelled_permission_result},
        )
        client = await AsyncAcpClient.spawn(
            [sys.executable, "-u", "-c", source],
            cwd=self.root,
            env=os.environ.copy(),
            log_path=self.root / f"acp-{len(self.clients)}.jsonl",
            reverse_handlers=reverse_handlers,  # type: ignore[arg-type]
            **kwargs,
        )
        self.clients.append(client)
        return client

    async def test_concurrent_responses_can_arrive_out_of_order(self) -> None:
        client = await self.spawn()
        first = await client.begin_request("hold", {"text": "first secret"})
        second = await client.begin_request("hold", {"text": "second secret"})
        second_result = await asyncio.wait_for(second.response, 2)
        first_result = await asyncio.wait_for(first.response, 2)
        self.assertEqual(second_result["value"], 2)
        self.assertEqual(first_result["value"], 1)
        notice = await asyncio.wait_for(client.notifications.get(), 2)
        self.assertEqual(notice["method"], "session/update")

    async def test_permission_reverse_request_receives_cancelled(self) -> None:
        client = await self.spawn()
        result = await client.call("permission", {}, timeout=2)
        reverse = result["reverse"]
        self.assertEqual(reverse["id"], "reverse-1")
        self.assertEqual(reverse["result"]["outcome"]["outcome"], "cancelled")

    async def test_unknown_reverse_request_gets_method_not_found(self) -> None:
        client = await self.spawn()
        result = await client.call("unknown_reverse", {}, timeout=2)
        self.assertEqual(result["reverse"]["error"]["code"], -32601)

    async def test_eof_fails_every_pending_request(self) -> None:
        client = await self.spawn()
        pending = await client.begin_request("hang", {})
        exiting = await client.begin_request("exit", {})
        with self.assertRaises(AcpTransportClosed):
            await asyncio.wait_for(pending.response, 2)
        with self.assertRaises(AcpTransportClosed):
            await asyncio.wait_for(exiting.response, 2)

    async def test_malformed_json_closes_transport(self) -> None:
        source = "import sys; sys.stdout.write('not-json\\n'); sys.stdout.flush(); sys.stdin.read()"
        client = await self.spawn(source)
        error = await asyncio.wait_for(client.wait_closed(), 2)
        self.assertIsInstance(error, AcpProtocolError)
        self.assertIsNotNone(client.proc.returncode)
        self.assertTrue(client.proc._transport.is_closing())
        self.assertTrue(client.proc.stdout._transport.is_closing())

    async def test_oversized_frame_closes_transport(self) -> None:
        source = (
            "import json,sys; sys.stdout.write(json.dumps({'jsonrpc':'2.0','method':'x','params':"
            "{'blob':'x'*5000}})+'\\n'); sys.stdout.flush(); sys.stdin.read()"
        )
        client = await self.spawn(source, max_frame_bytes=1024)
        error = await asyncio.wait_for(client.wait_closed(), 2)
        self.assertIsInstance(error, AcpProtocolError)

    async def test_timeout_closes_connection_without_retry(self) -> None:
        client = await self.spawn()
        with self.assertRaises(AcpTransportClosed):
            await client.call("hang", {}, timeout=0.05)
        self.assertIsInstance(client.closed_exception, AcpTransportClosed)

    async def test_logs_are_valid_jsonl_and_redact_bodies(self) -> None:
        client = await self.spawn()
        await client.call(
            "echo", {"text": "TOP_SECRET_BODY", "token": "TOP_TOKEN"}, timeout=2
        )
        log_path = client.log_path
        await client.close_transport()
        lines = [json.loads(line) for line in log_path.read_text().splitlines()]
        serialized = json.dumps(lines)
        self.assertNotIn("TOP_SECRET_BODY", serialized)
        self.assertNotIn("TOP_TOKEN", serialized)
        self.assertTrue(lines)

    async def test_slow_notification_sink_does_not_block_response_reader(self) -> None:
        source = r'''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    sys.stdout.write(json.dumps({"jsonrpc":"2.0","method":"session/update","params":{"n":1}}) + "\n")
    sys.stdout.write(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":True}}) + "\n")
    sys.stdout.flush()
'''
        sink_started = asyncio.Event()
        release_sink = asyncio.Event()

        async def slow_sink(_notice: dict[str, object]) -> None:
            sink_started.set()
            await release_sink.wait()

        client = await self.spawn(
            source,
            notification_sink=slow_sink,
            notification_capacity=1,
        )
        result = await asyncio.wait_for(client.call("go", {}, timeout=2), 0.5)
        self.assertEqual(result, {"ok": True})
        await asyncio.wait_for(sink_started.wait(), 0.5)
        release_sink.set()
        await asyncio.wait_for(client.notifications.join(), 0.5)

    async def test_flush_notifications_waits_for_exact_fifo_sink_cut(self) -> None:
        source = r'''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    for value in (1, 2):
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","method":"session/update","params":{"value":value}}) + "\n")
    sys.stdout.write(json.dumps({"jsonrpc":"2.0","id":msg["id"],"result":{}}) + "\n")
    sys.stdout.flush()
'''
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        seen: list[int] = []

        async def ordered_sink(notice: dict[str, object]) -> None:
            params = notice["params"]
            assert isinstance(params, dict)
            value = params["value"]
            assert isinstance(value, int)
            if value == 1:
                first_started.set()
                await release_first.wait()
            seen.append(value)

        client = await self.spawn(source, notification_sink=ordered_sink)
        await client.call("emit", {}, timeout=2)
        await asyncio.wait_for(first_started.wait(), 1)
        flush = asyncio.create_task(client.flush_notifications())
        await asyncio.sleep(0)
        self.assertFalse(flush.done())
        release_first.set()
        await asyncio.wait_for(flush, 1)
        self.assertEqual(seen, [1, 2])

    async def test_flush_notifications_requires_sink(self) -> None:
        client = await self.spawn()
        with self.assertRaisesRegex(ValueError, "requires a notification sink"):
            await client.flush_notifications()

    async def test_notification_sink_property_preserves_identity_and_is_read_only(self) -> None:
        async def replay_fence(_notice: dict[str, object]) -> None:
            return None

        client = await self.spawn(notification_sink=replay_fence)
        self.assertIs(client.notification_sink, replay_fence)
        with self.assertRaises(AttributeError):
            client.notification_sink = None  # type: ignore[misc]

    async def test_json_rpc_result_null_is_valid(self) -> None:
        client = await self.spawn(
            server_sending({"jsonrpc": "2.0", "id": 1, "result": None})
        )
        self.assertIsNone(await client.call("null-result", {}, timeout=2))

    async def test_present_non_string_method_is_protocol_error(self) -> None:
        client = await self.spawn(
            server_sending(
                {"jsonrpc": "2.0", "id": 1, "method": 7, "result": {}}
            )
        )
        with self.assertRaises(AcpProtocolError):
            await client.call("bad-method", {}, timeout=2)

    async def test_reverse_request_ids_are_strict_json_ids(self) -> None:
        invalid_ids: list[object] = [True, None, 1.5, {"nested": 1}]
        for invalid_id in invalid_ids:
            with self.subTest(request_id=invalid_id):
                client = await self.spawn(
                    server_sending(
                        {
                            "jsonrpc": "2.0",
                            "id": invalid_id,
                            "method": "session/request_permission",
                            "params": {},
                        }
                    )
                )
                with self.assertRaises(AcpProtocolError):
                    await client.call("invalid-reverse-id", {}, timeout=2)

    async def test_boolean_remote_error_code_is_protocol_error(self) -> None:
        client = await self.spawn(
            server_sending(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {"code": True, "message": "not an integer code"},
                }
            )
        )
        with self.assertRaises(AcpProtocolError):
            await client.call("bad-error", {}, timeout=2)

    async def test_fatal_reader_error_cancels_reverse_and_sink_tasks(self) -> None:
        source = r'''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    if msg.get("method") == "start":
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","id":"r","method":"x/block","params":{}}) + "\n")
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","method":"session/update","params":{}}) + "\n")
        sys.stdout.flush()
    elif msg.get("method") == "break-reader":
        sys.stdout.write("not-json\n")
        sys.stdout.flush()
'''
        reverse_started = asyncio.Event()
        reverse_cancelled = asyncio.Event()
        sink_started = asyncio.Event()
        sink_cancelled = asyncio.Event()

        async def blocked_reverse(_params: dict[str, object]) -> dict[str, object]:
            reverse_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                reverse_cancelled.set()
            return {}

        async def blocked_sink(_notice: dict[str, object]) -> None:
            sink_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                sink_cancelled.set()

        client = await self.spawn(
            source,
            reverse_handlers={"x/block": blocked_reverse},
            notification_sink=blocked_sink,
        )
        pending = await client.begin_request("start", {})
        await asyncio.wait_for(reverse_started.wait(), 1)
        await asyncio.wait_for(sink_started.wait(), 1)
        await client.notify("break-reader", {})
        error = await asyncio.wait_for(client.wait_closed(), 1)
        self.assertIsInstance(error, AcpProtocolError)
        with self.assertRaises(AcpProtocolError):
            await pending.response
        await asyncio.wait_for(reverse_cancelled.wait(), 1)
        await asyncio.wait_for(sink_cancelled.wait(), 1)
        self.assertFalse(client._reverse_tasks)
        self.assertTrue(client._notification_task.done())

    async def test_explicit_close_cancels_reverse_task_after_process_exit(self) -> None:
        source = r'''
import json, sys
for line in sys.stdin:
    msg = json.loads(line)
    if msg.get("method") == "start":
        sys.stdout.write(json.dumps({"jsonrpc":"2.0","id":"r","method":"x/block","params":{}}) + "\n")
        sys.stdout.flush()
'''
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked_reverse(_params: dict[str, object]) -> dict[str, object]:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return {}

        client = await self.spawn(
            source, reverse_handlers={"x/block": blocked_reverse}
        )
        pending = await client.begin_request("start", {})
        await asyncio.wait_for(started.wait(), 1)
        client.proc.terminate()
        await asyncio.wait_for(client.proc.wait(), 1)
        await client.close_transport()
        await asyncio.wait_for(cancelled.wait(), 1)
        self.assertFalse(client._reverse_tasks)
        with self.assertRaises(AcpTransportClosed):
            await pending.response

    async def test_cancelled_reverse_handler_sends_no_internal_error(self) -> None:
        source = r'''
import json, sys
saw_reverse_reply = False
def send(obj):
    sys.stdout.write(json.dumps(obj, separators=(",", ":")) + "\n")
    sys.stdout.flush()
for line in sys.stdin:
    msg = json.loads(line)
    if msg.get("method") == "trigger":
        send({"jsonrpc":"2.0","id":"r","method":"x/cancel","params":{}})
        send({"jsonrpc":"2.0","id":msg["id"],"result":{}})
    elif msg.get("id") == "r":
        saw_reverse_reply = True
    elif msg.get("method") == "probe":
        send({"jsonrpc":"2.0","id":msg["id"],"result":{"sawReverseReply":saw_reverse_reply}})
'''
        handler_started = asyncio.Event()

        async def cancel_handler(_params: dict[str, object]) -> dict[str, object]:
            handler_started.set()
            raise asyncio.CancelledError

        client = await self.spawn(
            source, reverse_handlers={"x/cancel": cancel_handler}
        )
        await client.call("trigger", {}, timeout=2)
        await asyncio.wait_for(handler_started.wait(), 1)
        await asyncio.sleep(0)
        result = await client.call("probe", {}, timeout=2)
        self.assertEqual(result, {"sawReverseReply": False})

    async def test_capacities_must_be_positive_non_boolean_integers(self) -> None:
        invalid_options = (
            {"notification_capacity": 0},
            {"notification_capacity": True},
            {"max_reverse_tasks": 0},
            {"max_reverse_tasks": True},
        )
        for options in invalid_options:
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    await self.spawn(**options)

    async def test_remote_error_message_and_data_are_redacted_from_log(self) -> None:
        client = await self.spawn(
            server_sending(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "error": {
                        "code": -32001,
                        "message": "REMOTE_MESSAGE_SECRET",
                        "data": {"diagnostic": "REMOTE_DATA_SECRET"},
                    },
                }
            )
        )
        with self.assertRaises(AcpRemoteError) as raised:
            await client.call("remote-error", {}, timeout=2)
        self.assertNotIn("REMOTE_MESSAGE_SECRET", str(raised.exception))
        self.assertNotIn("REMOTE_DATA_SECRET", str(raised.exception))
        await client.close_transport()
        serialized = client.log_path.read_text(encoding="utf-8")
        self.assertNotIn("REMOTE_MESSAGE_SECRET", serialized)
        self.assertNotIn("REMOTE_DATA_SECRET", serialized)

    async def test_outbound_methods_must_be_non_empty_strings(self) -> None:
        client = await self.spawn()
        for invalid in (None, 1, True, ""):
            with self.subTest(method=invalid):
                with self.assertRaises(ValueError):
                    await client.begin_request(invalid, {})  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    await client.notify(invalid, {})  # type: ignore[arg-type]

    async def test_spawn_does_not_change_process_umask_across_await(self) -> None:
        with mock.patch(
            "scripts.hcom_grok_seat.acp_client.os.umask",
            side_effect=AssertionError("process umask must not be touched"),
        ):
            client = await self.spawn()
            result = await client.call("echo", {}, timeout=2)
        self.assertEqual(result, {"method": "echo"})


if __name__ == "__main__":
    unittest.main()
