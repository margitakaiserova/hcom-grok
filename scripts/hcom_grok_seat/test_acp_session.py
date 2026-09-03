from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from typing import Any

from .acp_session import (
    AcpCompatibilityError,
    AcpHandshake,
    PermissionBroker,
    ResumeReplayFence,
    cancel_prompt,
    initialize_authenticated,
    resume_session,
)


class FakeClient:
    def __init__(
        self,
        initialize: dict[str, Any] | None = None,
        *,
        session_result: Any = None,
    ) -> None:
        self.initialize = initialize or {}
        self.session_result = session_result
        self.calls: list[tuple[str, dict[str, Any], float | None]] = []
        self.notifications: list[tuple[str, dict[str, Any]]] = []
        self.flush_count = 0
        self.notification_sink: Any = None

    async def call(
        self, method: str, params: dict[str, Any], *, timeout: float | None
    ) -> Any:
        self.calls.append((method, params, timeout))
        if method == "initialize":
            return self.initialize
        if method == "authenticate":
            return {}
        return self.session_result

    async def notify(self, method: str, params: dict[str, Any]) -> None:
        self.notifications.append((method, params))

    async def flush_notifications(self) -> None:
        self.flush_count += 1


def valid_initialize(cwd: Path) -> dict[str, Any]:
    return {
        "protocolVersion": 1,
        "agentCapabilities": {
            "loadSession": True,
            "sessionCapabilities": {"resume": {}, "close": {}},
        },
        "authMethods": [{"id": "cached_token"}],
        "_meta": {"agentVersion": "1.0.13", "currentWorkingDirectory": str(cwd)},
    }


def valid_resume_result(
    cwd: Path, session_id: str = "session-1", model_id: str = "grok-4.6"
) -> dict[str, Any]:
    return {
        "models": {
            "currentModelId": model_id,
            "availableModels": [{"modelId": model_id, "name": "Grok"}],
        },
        "_meta": {
            "sessionId": session_id,
            "x.ai/sessionDetail": {
                "sessionId": session_id,
                "cwd": str(cwd),
                "currentModelId": model_id,
            },
        },
    }


def session_update(
    event_id: str,
    correlated_prompt_id: str,
    kind: str,
    *,
    replay: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "eventId": event_id,
        "promptId": correlated_prompt_id,
    }
    if replay:
        meta["isReplay"] = True
    return {
        "jsonrpc": "2.0",
        "method": "_x.ai/session/update",
        "params": {
            "sessionId": "session-1",
            "_meta": meta,
            "update": {"sessionUpdate": kind, **extra},
        },
    }


def persisted(notification: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": notification["method"],
        "params": notification["params"],
        "timestamp": 1,
    }


def permission_params(tool_call_id: str = "tool-1") -> dict[str, Any]:
    return {
        "sessionId": "session-1",
        "toolCall": {"toolCallId": tool_call_id, "title": "read"},
        "options": [
            {"kind": "allow_always", "optionId": "forever"},
            {"kind": "allow_once", "optionId": "once"},
            {"kind": "reject_once", "optionId": "reject"},
        ],
    }


class AcpSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_initialize_resume_validates_and_flushes_before_sealing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td).resolve()
            fake = FakeClient(
                valid_initialize(cwd), session_result=valid_resume_result(cwd)
            )
            handshake = await initialize_authenticated(fake, cwd)  # type: ignore[arg-type]
            fence = ResumeReplayFence("session-1", "resume")
            fake.notification_sink = fence
            mode, result = await resume_session(
                fake,  # type: ignore[arg-type]
                handshake,
                "session-1",
                cwd,
                replay_fence=fence,
                expected_model_id="grok-4.6",
            )
            self.assertEqual(mode, "resume")
            self.assertEqual(result["_meta"]["sessionId"], "session-1")
            self.assertEqual(fake.flush_count, 1)
            self.assertTrue(fence.sealed)
            self.assertEqual(
                [call[0] for call in fake.calls],
                ["initialize", "authenticate", "session/resume"],
            )

    async def test_unknown_version_and_wrong_initialize_cwd_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td).resolve()
            wrong = valid_initialize(cwd)
            wrong["_meta"]["agentVersion"] = "9.9.9"
            with self.assertRaises(AcpCompatibilityError):
                await initialize_authenticated(FakeClient(wrong), cwd)  # type: ignore[arg-type]
            wrong = valid_initialize(cwd)
            wrong["_meta"]["currentWorkingDirectory"] = "/tmp/wrong"
            with self.assertRaises(AcpCompatibilityError):
                await initialize_authenticated(FakeClient(wrong), cwd)  # type: ignore[arg-type]

    async def test_load_is_used_only_when_resume_is_absent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td).resolve()
            handshake = AcpHandshake(
                1,
                "1.0.13",
                "cached_token",
                {"loadSession": True, "sessionCapabilities": {}},
                None,
            )
            fake = FakeClient(session_result=valid_resume_result(cwd))
            fence = ResumeReplayFence("session-1", "load")
            fake.notification_sink = fence
            mode, _ = await resume_session(
                fake,  # type: ignore[arg-type]
                handshake,
                "session-1",
                cwd,
                replay_fence=fence,
            )
            self.assertEqual(mode, "load")
            self.assertEqual(fake.calls[-1][0], "session/load")

    async def test_resume_result_identity_cwd_and_model_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td).resolve()
            handshake = AcpHandshake(
                1, "1.0.13", "cached_token", {"sessionCapabilities": {"resume": {}}}, None
            )
            mutations = []
            wrong_session = valid_resume_result(cwd)
            wrong_session["_meta"]["sessionId"] = "other"
            mutations.append(wrong_session)
            wrong_cwd = valid_resume_result(cwd)
            wrong_cwd["_meta"]["x.ai/sessionDetail"]["cwd"] = "/tmp/wrong"
            mutations.append(wrong_cwd)
            wrong_model = valid_resume_result(cwd)
            wrong_model["_meta"]["x.ai/sessionDetail"]["currentModelId"] = "grok-other"
            mutations.append(wrong_model)
            missing_identity = valid_resume_result(cwd)
            del missing_identity["_meta"]["sessionId"]
            del missing_identity["_meta"]["x.ai/sessionDetail"]["sessionId"]
            mutations.append(missing_identity)
            for result in mutations:
                with self.subTest(result=result):
                    fake = FakeClient(session_result=result)
                    fence = ResumeReplayFence("session-1", "resume")
                    fake.notification_sink = fence
                    with self.assertRaises(AcpCompatibilityError):
                        await resume_session(
                            fake,  # type: ignore[arg-type]
                            handshake,
                            "session-1",
                            cwd,
                            replay_fence=fence,
                            expected_model_id="grok-4.6",
                        )

    async def test_resume_requires_the_exact_open_fence_as_transport_sink(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cwd = Path(td).resolve()
            handshake = AcpHandshake(
                1, "1.0.13", "cached_token", {"sessionCapabilities": {"resume": {}}}, None
            )
            fake = FakeClient(session_result=valid_resume_result(cwd))
            fence = ResumeReplayFence("session-1", "resume")
            with self.assertRaisesRegex(AcpCompatibilityError, "configured ACP"):
                await resume_session(
                    fake,  # type: ignore[arg-type]
                    handshake,
                    "session-1",
                    cwd,
                    replay_fence=fence,
                )
            self.assertEqual(fake.calls, [])

            fake.notification_sink = fence
            await fence.seal()
            with self.assertRaisesRegex(AcpCompatibilityError, "sealed before"):
                await resume_session(
                    fake,  # type: ignore[arg-type]
                    handshake,
                    "session-1",
                    cwd,
                    replay_fence=fence,
                )
            self.assertEqual(fake.calls, [])

    async def test_load_fence_discards_replay_and_retains_live(self) -> None:
        fence = ResumeReplayFence("session-1", "load")
        replay = session_update(
            "event-1",
            "prompt-1",
            "hook_execution",
            replay=True,
            event_name="user_prompt_submit",
            prompt_id="prompt-1",
        )
        live = session_update(
            "event-2",
            "prompt-1",
            "agent_message_chunk",
            content={"type": "text", "text": "live"},
        )
        await fence(replay)
        await fence(live)
        await fence.seal()
        self.assertEqual(fence.replay_event_ids, ("event-1",))
        self.assertEqual(await fence.drain_live(), (live,))

    async def test_load_fence_forwards_only_non_replay_notifications(self) -> None:
        forwarded: list[dict[str, Any]] = []

        async def live_sink(notification: dict[str, Any]) -> None:
            forwarded.append(notification)

        fence = ResumeReplayFence("session-1", "load", live_sink=live_sink)
        replay = session_update(
            "event-1",
            "prompt-1",
            "hook_execution",
            replay=True,
            event_name="user_prompt_submit",
            prompt_id="prompt-1",
        )
        live = session_update(
            "event-2",
            "prompt-1",
            "agent_message_chunk",
            content={"type": "text", "text": "live"},
        )
        unrelated = {"jsonrpc": "2.0", "method": "custom/notice", "params": {}}
        await fence(replay)
        await fence(live)
        await fence(unrelated)
        self.assertIs(fence.live_sink, live_sink)
        self.assertEqual(forwarded, [live, unrelated])

    async def test_resume_fence_rejects_replay(self) -> None:
        fence = ResumeReplayFence("session-1", "resume")
        with self.assertRaises(AcpCompatibilityError):
            await fence(
                session_update(
                    "event-1",
                    "prompt-1",
                    "hook_execution",
                    replay=True,
                    event_name="user_prompt_submit",
                    prompt_id="prompt-1",
                )
            )

    async def test_replay_fence_is_bounded_and_rejects_non_finite_json(self) -> None:
        first = session_update(
            "event-1",
            "prompt-1",
            "agent_message_chunk",
            content={"type": "text", "text": "one"},
        )
        second = session_update(
            "event-2",
            "prompt-1",
            "agent_message_chunk",
            content={"type": "text", "text": "two"},
        )
        fence = ResumeReplayFence("session-1", "load", max_live_events=1)
        await fence(first)
        with self.assertRaisesRegex(AcpCompatibilityError, "buffer is full"):
            await fence(second)

        tiny = ResumeReplayFence("session-1", "load", max_live_bytes=1)
        with self.assertRaisesRegex(AcpCompatibilityError, "byte limit"):
            await tiny(first)

        non_finite = session_update(
            "event-3",
            "prompt-1",
            "agent_message_chunk",
            content={"type": "text", "text": "bad", "score": float("nan")},
        )
        with self.assertRaisesRegex(AcpCompatibilityError, "finite JSON"):
            await ResumeReplayFence("session-1", "load")(non_finite)
        with self.assertRaises(ValueError):
            ResumeReplayFence("session-1", "load", max_live_events=True)
        with self.assertRaises(ValueError):
            ResumeReplayFence("session-1", "load", live_sink="bad")  # type: ignore[arg-type]

    async def test_reconcile_combines_disk_with_buffered_live(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            admission = session_update(
                "event-1",
                "prompt-1",
                "hook_execution",
                event_name="user_prompt_submit",
                prompt_id="prompt-1",
            )
            path.write_text(json.dumps(persisted(admission)) + "\n")
            fence = ResumeReplayFence("session-1", "load")
            await fence.seal()
            completion = session_update(
                "event-2",
                "prompt-1",
                "turn_completed",
                prompt_id="prompt-1",
                stop_reason="end_turn",
            )
            await fence(completion)
            fake = FakeClient()
            evidence = await fence.reconcile_prompt(
                fake, path, "prompt-1"  # type: ignore[arg-type]
            )
            self.assertTrue(evidence.completed)
            self.assertEqual(evidence.live_event_count, 1)
            self.assertGreaterEqual(fake.flush_count, 1)

    async def test_permission_allow_once_requires_exact_tool_call(self) -> None:
        broker = PermissionBroker("session-1", decision=lambda _params: "allow_once")
        params = permission_params()
        self.assertEqual((await broker(params))["outcome"]["optionId"], "once")
        del params["toolCall"]
        self.assertEqual(
            (await broker(params))["outcome"]["outcome"], "cancelled"
        )
        mismatched = await broker({"sessionId": "other", "options": []})
        self.assertEqual(mismatched["outcome"]["outcome"], "cancelled")
        duplicate_ids = permission_params()
        duplicate_ids["options"][2]["optionId"] = "once"
        self.assertEqual(
            (await broker(duplicate_ids))["outcome"]["outcome"], "cancelled"
        )

    async def test_permission_timeout_returns_cancelled(self) -> None:
        blocker = asyncio.Event()

        async def decide(_params: dict[str, Any]) -> str:
            await blocker.wait()
            return "allow_once"

        broker = PermissionBroker("session-1", decision=decide, timeout=0.01)
        result = await broker(permission_params())
        self.assertEqual(result["outcome"]["outcome"], "cancelled")
        self.assertEqual(broker.pending_tool_calls, ())

    async def test_blocking_synchronous_permission_decision_is_timed_out(self) -> None:
        release = threading.Event()

        def blocking_decision(_params: dict[str, Any]) -> str:
            release.wait(2)
            return "allow_once"

        broker = PermissionBroker(
            "session-1", decision=blocking_decision, timeout=0.01
        )
        started = time.monotonic()
        try:
            result = await broker(permission_params())
        finally:
            release.set()
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(result["outcome"]["outcome"], "cancelled")
        self.assertEqual(broker.pending_tool_calls, ())

    async def test_cancel_pending_interrupts_permission_decision(self) -> None:
        started = asyncio.Event()
        blocker = asyncio.Event()

        async def decide(_params: dict[str, Any]) -> str:
            started.set()
            await blocker.wait()
            return "allow_once"

        broker = PermissionBroker("session-1", decision=decide)
        task = asyncio.create_task(broker(permission_params()))
        await started.wait()
        self.assertEqual(broker.pending_tool_calls, ("tool-1",))
        self.assertEqual(await broker.cancel_pending(), 1)
        result = await task
        self.assertEqual(result["outcome"]["outcome"], "cancelled")
        self.assertEqual(broker.pending_tool_calls, ())

    async def test_cancel_prompt_cancels_permissions_and_requires_stop_reason(self) -> None:
        started = asyncio.Event()
        blocker = asyncio.Event()

        async def decide(_params: dict[str, Any]) -> str:
            started.set()
            await blocker.wait()
            return "allow_once"

        broker = PermissionBroker("session-1", decision=decide)
        permission = asyncio.create_task(broker(permission_params()))
        await started.wait()
        response = asyncio.get_running_loop().create_future()
        response.set_result(
            {
                "stopReason": "cancelled",
                "_meta": {"sessionId": "session-1", "promptId": "prompt-1"},
            }
        )
        fake = FakeClient()
        result = await cancel_prompt(
            fake,
            broker,
            "session-1",
            "prompt-1",
            response,  # type: ignore[arg-type]
        )
        self.assertEqual(result["stopReason"], "cancelled")
        self.assertEqual(
            fake.notifications,
            [("session/cancel", {"sessionId": "session-1"})],
        )
        self.assertEqual((await permission)["outcome"]["outcome"], "cancelled")

        wrong = asyncio.get_running_loop().create_future()
        wrong.set_result({"stopReason": "end_turn"})
        with self.assertRaises(AcpCompatibilityError):
            await cancel_prompt(
                fake,
                broker,
                "session-1",
                "prompt-1",
                wrong,  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
