"""Fail-closed ACP negotiation, resume, replay, and permission handling."""
from __future__ import annotations

import asyncio
import copy
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from .acp_client import AsyncAcpClient, JsonObject


ACP_PROTOCOL_VERSION = 1
TESTED_GROK_VERSIONS = {"1.0.13"}
BRIDGE_VERSION = "1"
_SESSION_UPDATE_METHODS = {
    "session/update",
    "_x.ai/session/update",
    "_x.ai/session_notification",
}
DEFAULT_MAX_BUFFERED_LIVE_EVENTS = 4096
DEFAULT_MAX_BUFFERED_LIVE_BYTES = 64 * 1024 * 1024


class AcpCompatibilityError(RuntimeError):
    pass


@dataclass(frozen=True)
class AcpHandshake:
    protocol_version: int
    agent_version: str
    auth_method: str
    capabilities: JsonObject
    current_working_directory: str | None


async def initialize_authenticated(
    client: AsyncAcpClient,
    project: Path,
    *,
    tested_versions: set[str] | None = None,
) -> AcpHandshake:
    project = project.expanduser().resolve()
    result = await client.call(
        "initialize",
        {
            "protocolVersion": ACP_PROTOCOL_VERSION,
            "clientCapabilities": {
                "fs": {"readTextFile": False, "writeTextFile": False},
                "terminal": False,
            },
            "clientInfo": {"name": "hcom-grok-bridge", "version": BRIDGE_VERSION},
        },
        timeout=15,
    )
    if not isinstance(result, dict):
        raise AcpCompatibilityError("ACP initialize returned a non-object result")
    if result.get("protocolVersion") != ACP_PROTOCOL_VERSION:
        raise AcpCompatibilityError(
            f"ACP protocol mismatch: {result.get('protocolVersion')!r}"
        )
    capabilities = result.get("agentCapabilities")
    if not isinstance(capabilities, dict):
        raise AcpCompatibilityError("ACP initialize omitted agentCapabilities")
    meta = result.get("_meta")
    if not isinstance(meta, dict):
        raise AcpCompatibilityError("ACP initialize omitted metadata")
    version = meta.get("agentVersion")
    if not isinstance(version, str) or version not in (
        tested_versions or TESTED_GROK_VERSIONS
    ):
        raise AcpCompatibilityError(f"untested Grok agent version: {version!r}")
    cwd = meta.get("currentWorkingDirectory")
    if cwd is not None:
        if not isinstance(cwd, str) or Path(cwd).expanduser().resolve() != project:
            raise AcpCompatibilityError(
                f"Grok working directory mismatch: {cwd!r} != {str(project)!r}"
            )
    methods = result.get("authMethods")
    if not isinstance(methods, list):
        raise AcpCompatibilityError("ACP initialize omitted auth methods")
    method_ids = {
        item.get("id")
        for item in methods
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    }
    if "cached_token" not in method_ids:
        raise AcpCompatibilityError("cached_token authentication is unavailable")
    auth = await client.call("authenticate", {"methodId": "cached_token"}, timeout=15)
    if not isinstance(auth, dict):
        raise AcpCompatibilityError("ACP authenticate returned a non-object result")
    return AcpHandshake(
        ACP_PROTOCOL_VERSION,
        version,
        "cached_token",
        capabilities,
        cwd if isinstance(cwd, str) else None,
    )


ResumeMode = Literal["resume", "load"]


def select_resume_mode(handshake: AcpHandshake) -> ResumeMode:
    session_caps = handshake.capabilities.get("sessionCapabilities")
    if not isinstance(session_caps, dict):
        raise AcpCompatibilityError("sessionCapabilities are unavailable")
    if "resume" in session_caps:
        return "resume"
    if handshake.capabilities.get("loadSession") is True:
        return "load"
    raise AcpCompatibilityError("Grok supports neither session resume nor session load")


def _replay_marker(notification: JsonObject) -> bool:
    values: list[bool] = []
    containers: list[Any] = [notification.get("_meta")]
    params = notification.get("params")
    if isinstance(params, dict):
        containers.append(params.get("_meta"))
        update = params.get("update")
        if isinstance(update, dict):
            containers.append(update.get("_meta"))
    for container in containers:
        if not isinstance(container, dict) or "isReplay" not in container:
            continue
        marker = container["isReplay"]
        if type(marker) is not bool:
            raise AcpCompatibilityError("ACP isReplay marker is not boolean")
        values.append(marker)
    if values and any(value != values[0] for value in values[1:]):
        raise AcpCompatibilityError("ACP notification has conflicting replay markers")
    return values[0] if values else False


class ResumeReplayFence:
    """Discard tagged load replay and retain only session-matched live updates.

    Configure this object as the ACP notification sink before issuing the
    resume/load call. The transport's ``flush_notifications`` method is the
    ordering barrier that makes sealing and reconciliation race-free.
    """

    def __init__(
        self,
        session_id: str,
        mode: ResumeMode,
        *,
        live_sink: Callable[[JsonObject], Awaitable[None] | None] | None = None,
        max_live_events: int = DEFAULT_MAX_BUFFERED_LIVE_EVENTS,
        max_live_bytes: int = DEFAULT_MAX_BUFFERED_LIVE_BYTES,
    ) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session ID must be a non-empty string")
        if mode not in {"resume", "load"}:
            raise ValueError("replay fence mode is invalid")
        if live_sink is not None and not callable(live_sink):
            raise ValueError("live_sink must be callable")
        for label, value in (
            ("max_live_events", max_live_events),
            ("max_live_bytes", max_live_bytes),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{label} must be a positive integer")
        self.session_id = session_id
        self.mode = mode
        self._live_sink = live_sink
        self._max_live_events = max_live_events
        self._max_live_bytes = max_live_bytes
        self._live: list[JsonObject] = []
        self._live_bytes = 0
        self._replay_event_ids: list[str] = []
        self._replay_count = 0
        self._sealed = False
        self._lock = asyncio.Lock()

    @property
    def sealed(self) -> bool:
        return self._sealed

    @property
    def replay_event_ids(self) -> tuple[str, ...]:
        return tuple(self._replay_event_ids)

    @property
    def live_sink(self) -> Callable[[JsonObject], Awaitable[None] | None] | None:
        """Return the downstream consumer for non-replay notifications."""

        return self._live_sink

    async def _forward_live(self, notification: JsonObject) -> None:
        if self._live_sink is None:
            return
        result = self._live_sink(notification)
        if inspect.isawaitable(result):
            await result

    async def __call__(self, notification: JsonObject) -> None:
        if not isinstance(notification, dict):
            raise AcpCompatibilityError("ACP notification is not an object")
        method = notification.get("method")
        if method not in _SESSION_UPDATE_METHODS:
            await self._forward_live(notification)
            return
        params = notification.get("params")
        if not isinstance(params, dict):
            raise AcpCompatibilityError("ACP session update omitted params")
        observed_session = params.get("sessionId")
        if observed_session != self.session_id:
            raise AcpCompatibilityError(
                f"ACP session update mismatch: {observed_session!r}"
            )
        replay = _replay_marker(notification)
        try:
            notification_bytes = len(
                json.dumps(
                    notification,
                    ensure_ascii=False,
                    sort_keys=True,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8", "strict")
            )
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise AcpCompatibilityError(
                "ACP session update is not exact finite JSON"
            ) from exc
        async with self._lock:
            if replay:
                if self.mode != "load" or self._sealed:
                    raise AcpCompatibilityError(
                        "ACP replay arrived on a resume or after replay fencing closed"
                    )
                self._replay_count += 1
                if self._replay_count > self._max_live_events:
                    raise AcpCompatibilityError("ACP replay exceeds the bounded event limit")
                meta = params.get("_meta")
                event_id = meta.get("eventId") if isinstance(meta, dict) else None
                if isinstance(event_id, str) and event_id:
                    self._replay_event_ids.append(event_id)
                return
            if len(self._live) >= self._max_live_events:
                raise AcpCompatibilityError("ACP live recovery buffer is full")
            if self._live_bytes + notification_bytes > self._max_live_bytes:
                raise AcpCompatibilityError("ACP live recovery buffer exceeds its byte limit")
            self._live.append(copy.deepcopy(notification))
            self._live_bytes += notification_bytes
        await self._forward_live(notification)

    async def seal(self) -> None:
        async with self._lock:
            self._sealed = True

    async def drain_live(self) -> tuple[JsonObject, ...]:
        async with self._lock:
            if not self._sealed:
                raise AcpCompatibilityError("replay fence has not been sealed")
            result = tuple(self._live)
            self._live.clear()
            self._live_bytes = 0
            return result

    async def reconcile_prompt(
        self,
        client: AsyncAcpClient,
        updates_path: Path,
        prompt_id: str,
    ) -> Any:
        """Reconcile an append-only disk snapshot and an exact FIFO live cut."""

        if not self._sealed:
            raise AcpCompatibilityError("cannot reconcile before replay fence sealing")
        from .grok_updates import scan_prompt_updates

        await client.flush_notifications()
        live_cut = await self.drain_live()
        return await asyncio.to_thread(
            scan_prompt_updates,
            updates_path,
            self.session_id,
            prompt_id,
            buffered_live=live_cut,
        )


def _metadata_values(result: JsonObject, field: str) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    if field in result:
        values.append((f"result.{field}", result[field]))
    meta = result.get("_meta")
    if isinstance(meta, dict):
        if field in meta:
            values.append((f"result._meta.{field}", meta[field]))
        detail = meta.get("x.ai/sessionDetail")
        if isinstance(detail, dict) and field in detail:
            values.append((f"result._meta.x.ai/sessionDetail.{field}", detail[field]))
    return values


def _validate_resume_result(
    result: Any,
    session_id: str,
    project: Path,
    expected_model_id: str | None,
) -> JsonObject:
    if not isinstance(result, dict):
        raise AcpCompatibilityError("ACP resume/load returned a non-object result")
    if "_meta" in result and not isinstance(result["_meta"], dict):
        raise AcpCompatibilityError("ACP resume/load metadata is not an object")
    meta = result.get("_meta")
    if isinstance(meta, dict):
        detail = meta.get("x.ai/sessionDetail")
        if detail is not None and not isinstance(detail, dict):
            raise AcpCompatibilityError("ACP session detail metadata is not an object")
    session_values = _metadata_values(result, "sessionId")
    if not session_values:
        raise AcpCompatibilityError("ACP resume/load result omitted session identity")
    for label, value in session_values:
        if value != session_id:
            raise AcpCompatibilityError(
                f"ACP resume/load session mismatch at {label}: {value!r}"
            )

    project = project.expanduser().resolve()
    cwd_values = _metadata_values(result, "cwd")
    cwd_values.extend(_metadata_values(result, "currentWorkingDirectory"))
    if not cwd_values:
        raise AcpCompatibilityError("ACP resume/load result omitted working directory")
    for label, value in cwd_values:
        if not isinstance(value, str) or Path(value).expanduser().resolve() != project:
            raise AcpCompatibilityError(
                f"ACP resume/load cwd mismatch at {label}: {value!r}"
            )

    model_values = _metadata_values(result, "currentModelId")
    model_values.extend(_metadata_values(result, "modelId"))
    models = result.get("models")
    if models is not None:
        if not isinstance(models, dict):
            raise AcpCompatibilityError("ACP resume/load models metadata is not an object")
        if "currentModelId" in models:
            model_values.append(("result.models.currentModelId", models["currentModelId"]))
        available = models.get("availableModels")
        if available is not None:
            if not isinstance(available, list):
                raise AcpCompatibilityError("availableModels is not a list")
            available_ids: list[str] = []
            for item in available:
                if not isinstance(item, dict):
                    raise AcpCompatibilityError("availableModels contains a non-object")
                model_id = item.get("modelId")
                if not isinstance(model_id, str) or not model_id:
                    raise AcpCompatibilityError("availableModels contains an invalid modelId")
                available_ids.append(model_id)
            if len(set(available_ids)) != len(available_ids):
                raise AcpCompatibilityError("availableModels contains duplicate model IDs")
            current = models.get("currentModelId")
            if current is not None and current not in available_ids:
                raise AcpCompatibilityError("currentModelId is absent from availableModels")

    observed_models: set[str] = set()
    for label, value in model_values:
        if not isinstance(value, str) or not value:
            raise AcpCompatibilityError(f"invalid model metadata at {label}: {value!r}")
        observed_models.add(value)
    if len(observed_models) > 1:
        raise AcpCompatibilityError(
            f"ACP resume/load returned conflicting models: {sorted(observed_models)!r}"
        )
    if expected_model_id is not None:
        if not isinstance(expected_model_id, str) or not expected_model_id:
            raise ValueError("expected_model_id must be a non-empty string")
        if not observed_models or observed_models != {expected_model_id}:
            raise AcpCompatibilityError(
                f"ACP resume/load model mismatch: {sorted(observed_models)!r}"
            )
    return result


async def resume_session(
    client: AsyncAcpClient,
    handshake: AcpHandshake,
    session_id: str,
    project: Path,
    *,
    replay_fence: ResumeReplayFence,
    expected_model_id: str | None = None,
) -> tuple[ResumeMode, JsonObject]:
    if not isinstance(session_id, str) or not session_id.strip():
        raise AcpCompatibilityError("session ID is empty")
    if expected_model_id is not None and (
        not isinstance(expected_model_id, str) or not expected_model_id
    ):
        raise ValueError("expected_model_id must be a non-empty string")
    mode = select_resume_mode(handshake)
    if replay_fence.session_id != session_id or replay_fence.mode != mode:
        raise AcpCompatibilityError("resume replay fence does not match the session mode")
    if replay_fence.sealed:
        raise AcpCompatibilityError("resume replay fence was sealed before resume/load")
    if getattr(client, "notification_sink", None) is not replay_fence:
        raise AcpCompatibilityError(
            "resume replay fence is not the configured ACP notification sink"
        )
    project = project.expanduser().resolve()
    params = {"sessionId": session_id, "cwd": str(project), "mcpServers": []}
    method = "session/resume" if mode == "resume" else "session/load"
    timeout = 60 if mode == "resume" else 120
    result = await client.call(method, params, timeout=timeout)
    validated = _validate_resume_result(
        result, session_id, project, expected_model_id
    )
    try:
        await client.flush_notifications()
    except (AttributeError, ValueError) as exc:
        raise AcpCompatibilityError(
            "ACP transport lacks an active notification-sink barrier"
        ) from exc
    await replay_fence.seal()
    return mode, validated


PermissionDecision = Callable[[JsonObject], Awaitable[str] | str]


def _cancelled_permission() -> JsonObject:
    return {"outcome": {"outcome": "cancelled"}}


class PermissionBroker:
    """Validate permission requests and never grant persistent access."""

    def __init__(
        self,
        session_id: str,
        decision: PermissionDecision | None = None,
        timeout: float = 300,
    ) -> None:
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session ID must be a non-empty string")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError("permission timeout must be positive")
        self.session_id = session_id
        self.decision = decision
        self.timeout = float(timeout)
        self._pending: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._cancelling = False

    @property
    def pending_tool_calls(self) -> tuple[str, ...]:
        return tuple(sorted(self._pending))

    async def cancel_pending(self) -> int:
        async with self._lock:
            self._cancelling = True
            pending = tuple(self._pending.values())
            for event in pending:
                event.set()
            return len(pending)

    async def wait_pending_cancelled(self) -> None:
        await self._idle.wait()

    async def finish_cancellation(self) -> None:
        async with self._lock:
            if self._pending:
                raise AcpCompatibilityError(
                    "cannot finish cancellation while permissions are pending"
                )
            self._cancelling = False

    async def __call__(self, params: JsonObject) -> JsonObject:
        if not isinstance(params, dict) or params.get("sessionId") != self.session_id:
            return _cancelled_permission()
        tool_call = params.get("toolCall")
        if not isinstance(tool_call, dict):
            return _cancelled_permission()
        tool_call_id = tool_call.get("toolCallId")
        if not isinstance(tool_call_id, str) or not tool_call_id.strip():
            return _cancelled_permission()
        options = params.get("options")
        if not isinstance(options, list):
            return _cancelled_permission()
        candidates: dict[str, str] = {}
        option_ids: set[str] = set()
        for item in options:
            if not isinstance(item, dict):
                return _cancelled_permission()
            kind = item.get("kind")
            option_id = item.get("optionId")
            if not isinstance(kind, str) or not isinstance(option_id, str) or not option_id:
                return _cancelled_permission()
            if kind in candidates or option_id in option_ids:
                return _cancelled_permission()
            candidates[kind] = option_id
            option_ids.add(option_id)

        cancelled = asyncio.Event()
        async with self._lock:
            if self._cancelling or tool_call_id in self._pending:
                return _cancelled_permission()
            self._pending[tool_call_id] = cancelled
            self._idle.clear()

        decision_task: asyncio.Future[Any] | None = None
        cancel_task: asyncio.Task[bool] | None = None
        try:
            if self.decision is None:
                choice: Any = "reject_once"
            else:
                async def decide() -> Any:
                    result = await asyncio.to_thread(self.decision, params)
                    if inspect.isawaitable(result):
                        return await result
                    return result

                decision_task = asyncio.create_task(decide())
                cancel_task = asyncio.create_task(cancelled.wait())
                done, _ = await asyncio.wait(
                    {decision_task, cancel_task},
                    timeout=self.timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cancel_task in done or decision_task not in done:
                    return _cancelled_permission()
                try:
                    choice = decision_task.result()
                except TimeoutError:
                    return _cancelled_permission()
            if cancelled.is_set() or choice == "cancelled":
                return _cancelled_permission()
            if choice not in {"allow_once", "reject_once"}:
                return _cancelled_permission()
            option_id = candidates.get(choice)
            if option_id is None:
                return _cancelled_permission()
            return {"outcome": {"outcome": "selected", "optionId": option_id}}
        finally:
            for task in (decision_task, cancel_task):
                if task is not None and not task.done():
                    task.cancel()
            if decision_task is not None or cancel_task is not None:
                await asyncio.gather(
                    *(task for task in (decision_task, cancel_task) if task is not None),
                    return_exceptions=True,
                )
            async with self._lock:
                if self._pending.get(tool_call_id) is cancelled:
                    self._pending.pop(tool_call_id, None)
                if not self._pending:
                    self._idle.set()


async def cancel_prompt(
    client: AsyncAcpClient,
    broker: PermissionBroker,
    session_id: str,
    prompt_id: str,
    prompt_response: Awaitable[Any],
    *,
    timeout: float = 15,
) -> JsonObject:
    """Cancel permissions, notify Grok, and prove the prompt stopped cancelled."""

    if session_id != broker.session_id:
        raise AcpCompatibilityError("permission broker belongs to another session")
    if not isinstance(prompt_id, str) or not prompt_id:
        raise ValueError("prompt_id must be a non-empty string")
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("cancellation timeout must be positive")
    try:
        await broker.cancel_pending()
        await broker.wait_pending_cancelled()
        await client.notify("session/cancel", {"sessionId": session_id})
        try:
            async with asyncio.timeout(float(timeout)):
                result = await asyncio.shield(prompt_response)
        except TimeoutError as exc:
            raise AcpCompatibilityError(
                "ACP prompt did not stop after session/cancel"
            ) from exc
    finally:
        await broker.finish_cancellation()
    meta = result.get("_meta") if isinstance(result, dict) else None
    if (
        not isinstance(result, dict)
        or result.get("stopReason") != "cancelled"
        or not isinstance(meta, dict)
        or meta.get("sessionId") != session_id
        or meta.get("promptId") != prompt_id
    ):
        raise AcpCompatibilityError("ACP cancellation was not confirmed")
    return result
