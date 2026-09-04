"""Asynchronous, bounded JSON-RPC client for ``grok agent stdio``."""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import inspect
import json
import os
import signal
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeAlias


JsonId: TypeAlias = int | str
JsonObject: TypeAlias = dict[str, Any]
ReverseHandler: TypeAlias = Callable[[JsonObject], Awaitable[JsonObject] | JsonObject]
NotificationSink: TypeAlias = Callable[[JsonObject], Awaitable[None] | None]


class AcpError(RuntimeError):
    pass


class AcpTransportClosed(AcpError):
    pass


class AcpProtocolError(AcpError):
    pass


class AcpRemoteError(AcpError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"ACP remote error {code}; message and data redacted")
        self.code = code
        self.message = message
        self.data = data


@dataclass(frozen=True)
class RpcHandle:
    request_id: JsonId
    method: str
    response: asyncio.Future[Any]


@dataclass
class _PendingRpc:
    method: str
    future: asyncio.Future[Any]


@dataclass(frozen=True)
class _NotificationBarrier:
    reached: asyncio.Future[None]


_SECRET_KEYS = {
    "authorization",
    "cookie",
    "password",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
}
_BODY_KEYS = {
    "body",
    "content",
    "data",
    "exact_body",
    "message",
    "prompt",
    "text",
}


def _summary(value: Any) -> dict[str, Any]:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode(
        "utf-8", "surrogatepass"
    )
    return {"redacted": True, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def _redact(value: Any, parent_key: str = "") -> Any:
    lowered = parent_key.lower()
    if any(secret in lowered for secret in _SECRET_KEYS):
        return "<redacted>"
    if lowered in _BODY_KEYS:
        return _summary(value)
    if isinstance(value, dict):
        return {str(key): _redact(item, str(key)) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


class AsyncAcpClient:
    """One-reader, multi-request ACP transport.

    A successful ``begin_request`` means only that a complete frame drained to
    the child pipe. Prompt admission must be proven separately.
    """

    def __init__(
        self,
        proc: asyncio.subprocess.Process,
        *,
        log_path: Path,
        log_file: Any,
        stderr_file: Any,
        reverse_handlers: Mapping[str, ReverseHandler],
        notification_sink: NotificationSink | None,
        max_frame_bytes: int,
        notification_capacity: int,
        max_reverse_tasks: int,
        process_group_owned: bool,
    ) -> None:
        self.proc = proc
        self.log_path = log_path
        self._log_file = log_file
        self._stderr_file = stderr_file
        self._reverse_handlers = dict(reverse_handlers)
        self._notification_sink = notification_sink
        self._max_frame_bytes = max_frame_bytes
        self._max_reverse_tasks = max_reverse_tasks
        self._process_group_owned = process_group_owned
        self._next_id = 0
        self._pending: dict[JsonId, _PendingRpc] = {}
        self._tombstones: set[JsonId] = set()
        self._write_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._reverse_tasks: set[asyncio.Task[None]] = set()
        self.notifications: asyncio.Queue[JsonObject | _NotificationBarrier] = asyncio.Queue(
            maxsize=notification_capacity
        )
        loop = asyncio.get_running_loop()
        self._closed: asyncio.Future[BaseException | None] = loop.create_future()
        self._closed_exc: BaseException | None = None
        self._closing = False
        self._files_closed = False
        self._notification_task: asyncio.Task[None] | None = None
        if notification_sink is not None:
            self._notification_task = asyncio.create_task(
                self._notification_loop(), name="acp-notification-sink"
            )
        self._reader_task = asyncio.create_task(self._reader_loop(), name="acp-reader")

    @classmethod
    async def spawn(
        cls,
        argv: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str],
        log_path: Path,
        reverse_handlers: Mapping[str, ReverseHandler] | None = None,
        notification_sink: NotificationSink | None = None,
        max_frame_bytes: int = 16 * 1024 * 1024,
        notification_capacity: int = 4096,
        max_reverse_tasks: int = 32,
        start_new_session: bool = False,
    ) -> "AsyncAcpClient":
        if not argv:
            raise ValueError("ACP argv cannot be empty")
        if type(max_frame_bytes) is not int or max_frame_bytes < 1024:
            raise ValueError("max_frame_bytes must be at least 1024")
        if type(notification_capacity) is not int or notification_capacity <= 0:
            raise ValueError("notification_capacity must be a positive integer")
        if type(max_reverse_tasks) is not int or max_reverse_tasks <= 0:
            raise ValueError("max_reverse_tasks must be a positive integer")
        log_path = log_path.expanduser().absolute()
        log_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(log_path.parent, 0o700)
        stderr_path = log_path.with_name(log_path.stem + ".stderr.log")
        log_file = None
        stderr_file = None
        try:
            log_fd = _open_private_append(log_path)
            try:
                log_file = os.fdopen(log_fd, "a", encoding="utf-8", buffering=1)
            except BaseException:
                os.close(log_fd)
                raise
            stderr_fd = _open_private_append(stderr_path)
            try:
                stderr_file = os.fdopen(stderr_fd, "ab", buffering=0)
            except BaseException:
                os.close(stderr_fd)
                raise
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(cwd),
                env=dict(env),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=stderr_file,
                limit=max_frame_bytes + 1,
                start_new_session=start_new_session,
            )
        except BaseException:
            if log_file is not None:
                log_file.close()
            if stderr_file is not None:
                stderr_file.close()
            raise
        return cls(
            proc,
            log_path=log_path,
            log_file=log_file,
            stderr_file=stderr_file,
            reverse_handlers=reverse_handlers or {},
            notification_sink=notification_sink,
            max_frame_bytes=max_frame_bytes,
            notification_capacity=notification_capacity,
            max_reverse_tasks=max_reverse_tasks,
            process_group_owned=start_new_session,
        )

    @property
    def closed_exception(self) -> BaseException | None:
        return self._closed_exc

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def notification_sink(self) -> NotificationSink | None:
        """Return the exact configured sink so recovery can prove its fence."""
        return self._notification_sink

    def _log(self, direction: str, payload: Any, byte_count: int) -> None:
        redacted = _redact(payload)
        encoded = json.dumps(redacted, ensure_ascii=False, sort_keys=True, default=str).encode(
            "utf-8"
        )
        if len(encoded) > 64 * 1024:
            redacted = _summary(redacted)
        record = {
            "direction": direction,
            "monotonic_ns": time.monotonic_ns(),
            "wall_time_ns": time.time_ns(),
            "bytes": int(byte_count),
            "payload": redacted,
        }
        self._log_file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    async def _send_frame(self, payload: JsonObject) -> None:
        if self._closing or self.proc.stdin is None:
            raise AcpTransportClosed("ACP transport is closed")
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"
        if len(raw) > self._max_frame_bytes:
            raise AcpProtocolError(
                f"outbound ACP frame is {len(raw)} bytes; maximum is {self._max_frame_bytes}"
            )
        async with self._write_lock:
            if self._closing or self.proc.stdin is None:
                raise AcpTransportClosed("ACP transport is closed")
            self._log("out", payload, len(raw))
            self.proc.stdin.write(raw)
            try:
                await self.proc.stdin.drain()
            except (BrokenPipeError, ConnectionError) as exc:
                error = AcpTransportClosed(f"ACP pipe write failed: {exc}")
                await self._fatal(error)
                raise error from exc

    async def begin_request(
        self, method: str, params: JsonObject | None = None
    ) -> RpcHandle:
        if not isinstance(method, str) or not method:
            raise ValueError("ACP method must be a non-empty string")
        loop = asyncio.get_running_loop()
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = _PendingRpc(method, future)
        payload: JsonObject = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            payload["params"] = params
        try:
            await self._send_frame(payload)
        except BaseException:
            self._pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            raise
        return RpcHandle(request_id, method, future)

    async def call(
        self,
        method: str,
        params: JsonObject | None = None,
        *,
        timeout: float | None,
    ) -> Any:
        handle = await self.begin_request(method, params)
        return await self.await_response(handle, timeout=timeout)

    async def await_response(
        self,
        handle: RpcHandle,
        *,
        timeout: float | None,
    ) -> Any:
        """Finish a request whose frame has already drained to the child."""

        try:
            if timeout is None:
                return await handle.response
            async with asyncio.timeout(timeout):
                return await asyncio.shield(handle.response)
        except TimeoutError as exc:
            self._pending.pop(handle.request_id, None)
            self._tombstones.add(handle.request_id)
            error = AcpTransportClosed(
                f"timeout waiting for ACP method {handle.method}"
            )
            if not handle.response.done():
                handle.response.set_exception(error)
                with contextlib.suppress(AcpError):
                    handle.response.exception()
            await self._fatal(error)
            raise error from exc

    async def notify(self, method: str, params: JsonObject | None = None) -> None:
        if not isinstance(method, str) or not method:
            raise ValueError("ACP method must be a non-empty string")
        payload: JsonObject = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        await self._send_frame(payload)

    async def flush_notifications(self) -> None:
        """Wait until the configured sink has processed a precise FIFO cut."""
        if self._notification_task is None:
            raise ValueError("flush_notifications requires a notification sink")
        if self._closing:
            raise self._transport_closed_error()
        loop = asyncio.get_running_loop()
        barrier = _NotificationBarrier(loop.create_future())
        await self._await_or_transport_close(self.notifications.put(barrier))
        await self._await_or_transport_close(asyncio.shield(barrier.reached))

    async def _reader_loop(self) -> None:
        assert self.proc.stdout is not None
        try:
            while not self._closing:
                try:
                    raw = await self.proc.stdout.readline()
                except (ValueError, asyncio.LimitOverrunError) as exc:
                    raise AcpProtocolError("ACP frame exceeded configured limit") from exc
                if not raw:
                    return_code = await self.proc.wait()
                    raise AcpTransportClosed(f"ACP sidecar exited with status {return_code}")
                if len(raw) > self._max_frame_bytes:
                    raise AcpProtocolError(
                        f"inbound ACP frame is {len(raw)} bytes; maximum is {self._max_frame_bytes}"
                    )
                if not raw.endswith(b"\n"):
                    raise AcpProtocolError("ACP stream ended with a partial frame")
                try:
                    obj = json.loads(raw.decode("utf-8", "strict"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise AcpProtocolError(f"malformed ACP JSON: {exc}") from exc
                if not isinstance(obj, dict):
                    raise AcpProtocolError("ACP frame is not a JSON object")
                if obj.get("jsonrpc") != "2.0":
                    raise AcpProtocolError("ACP frame has an invalid JSON-RPC version")
                self._log("in", obj, len(raw))
                await self._dispatch(obj)
        except asyncio.CancelledError:
            if not self._closing:
                await self._fatal(AcpTransportClosed("ACP reader was cancelled"))
            raise
        except BaseException as exc:
            await self._fatal(exc if isinstance(exc, AcpError) else AcpProtocolError(str(exc)))

    async def _notification_loop(self) -> None:
        assert self._notification_sink is not None
        try:
            while True:
                item = await self.notifications.get()
                try:
                    if isinstance(item, _NotificationBarrier):
                        if not item.reached.done():
                            item.reached.set_result(None)
                    else:
                        result = self._notification_sink(item)
                        if inspect.isawaitable(result):
                            await result
                finally:
                    self.notifications.task_done()
        except asyncio.CancelledError:
            if not self._closing:
                await self._fatal(
                    AcpTransportClosed("ACP notification sink was cancelled")
                )
            raise
        except BaseException as exc:
            error = AcpProtocolError(
                f"ACP notification sink failed: {type(exc).__name__}"
            )
            await self._fatal(error)

    async def _dispatch(self, obj: JsonObject) -> None:
        has_method = "method" in obj
        has_id = "id" in obj
        if has_method and not isinstance(obj["method"], str):
            raise AcpProtocolError("ACP method has an invalid type")
        if has_method and has_id:
            request_id = obj["id"]
            if not _is_json_id(request_id):
                raise AcpProtocolError("ACP reverse request ID has an invalid type")
            if len(self._reverse_tasks) >= self._max_reverse_tasks:
                await self._send_error(
                    request_id, -32603, "Too many concurrent reverse requests"
                )
                return
            task = asyncio.create_task(self._run_reverse(obj), name="acp-reverse")
            self._reverse_tasks.add(task)
            task.add_done_callback(self._reverse_done)
            return
        if has_method and not has_id:
            if self.notifications.full():
                raise AcpProtocolError("ACP notification queue overflow")
            self.notifications.put_nowait(obj)
            return
        if has_id and not has_method:
            await self._handle_response(obj)
            return
        raise AcpProtocolError("invalid JSON-RPC envelope")

    async def _handle_response(self, obj: JsonObject) -> None:
        request_id = obj.get("id")
        if not _is_json_id(request_id):
            raise AcpProtocolError("ACP response ID has an invalid type")
        if request_id in self._tombstones:
            self._tombstones.remove(request_id)
            return
        pending = self._pending.get(request_id)
        if pending is None:
            raise AcpProtocolError(f"unknown or duplicate ACP response ID {request_id!r}")
        has_result = "result" in obj
        has_error = "error" in obj
        if has_result == has_error:
            raise AcpProtocolError("ACP response must contain exactly one of result or error")
        if has_error:
            error = obj.get("error")
            if not isinstance(error, dict):
                raise AcpProtocolError("ACP error response is malformed")
            code = error.get("code")
            message = error.get("message")
            if type(code) is not int or not isinstance(message, str):
                raise AcpProtocolError("ACP error response lacks code or message")
            self._pending.pop(request_id, None)
            if not pending.future.done():
                pending.future.set_exception(AcpRemoteError(code, message, error.get("data")))
            return
        result = obj["result"]
        self._pending.pop(request_id, None)
        if not pending.future.done():
            pending.future.set_result(result)

    async def _run_reverse(self, obj: JsonObject) -> None:
        try:
            await self._handle_reverse(obj)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            error = exc if isinstance(exc, AcpError) else AcpProtocolError(str(exc))
            await self._fatal(error)

    def _reverse_done(self, task: asyncio.Task[None]) -> None:
        self._reverse_tasks.discard(task)
        with contextlib.suppress(asyncio.CancelledError):
            task.exception()

    async def _handle_reverse(self, obj: JsonObject) -> None:
        request_id = obj["id"]
        method = str(obj["method"])
        params = obj.get("params", {})
        if not isinstance(params, dict):
            await self._send_error(request_id, -32602, "Invalid params")
            return
        handler = self._reverse_handlers.get(method)
        if handler is None:
            await self._send_error(request_id, -32601, "Method not found")
            return
        try:
            result = handler(params)
            if inspect.isawaitable(result):
                result = await result
            if not isinstance(result, dict):
                raise TypeError("reverse handler did not return an object")
        except asyncio.CancelledError:
            raise
        except (ValueError, TypeError) as exc:
            await self._send_error(request_id, -32602, f"Invalid params: {exc}")
            return
        except Exception as exc:
            await self._send_error(request_id, -32603, f"Internal error: {type(exc).__name__}")
            return
        await self._send_frame({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def _send_error(self, request_id: Any, code: int, message: str) -> None:
        await self._send_frame(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": code, "message": message},
            }
        )

    async def _fatal(self, exc: BaseException) -> None:
        async with self._lifecycle_lock:
            if self._closed_exc is None:
                self._closed_exc = exc
            self._closing = True
            self._fail_pending(self._closed_exc)
            await self._cancel_background_tasks()
            await self._stop_process()
            self._close_files()
            if not self._closed.done():
                self._closed.set_result(self._closed_exc)

    async def _await_or_transport_close(self, awaitable: Awaitable[Any]) -> Any:
        operation = asyncio.ensure_future(awaitable)
        closed_wait = asyncio.ensure_future(asyncio.shield(self._closed))
        try:
            done, _pending = await asyncio.wait(
                (operation, closed_wait), return_when=asyncio.FIRST_COMPLETED
            )
            if operation in done:
                return await operation
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            raise self._transport_closed_error()
        finally:
            if not closed_wait.done():
                closed_wait.cancel()
            await asyncio.gather(closed_wait, return_exceptions=True)

    def _transport_closed_error(self) -> AcpTransportClosed:
        if self._closed_exc is None:
            return AcpTransportClosed("ACP transport is closed")
        return AcpTransportClosed(
            f"ACP transport is closed: {type(self._closed_exc).__name__}"
        )

    async def wait_closed(self) -> BaseException | None:
        return await self._closed

    async def close_transport(self) -> None:
        async with self._lifecycle_lock:
            self._closing = True
            self._fail_pending(AcpTransportClosed("ACP transport closed by caller"))
            await self._cancel_background_tasks()
            await self._stop_process()
            if not self._closed.done():
                self._closed.set_result(self._closed_exc)
            self._close_files()

    def _fail_pending(self, exc: BaseException) -> None:
        for pending in list(self._pending.values()):
            if not pending.future.done():
                pending.future.set_exception(exc)
        self._pending.clear()

    async def _cancel_background_tasks(self) -> None:
        current = asyncio.current_task()
        tasks: set[asyncio.Task[Any]] = set(self._reverse_tasks)
        tasks.add(self._reader_task)
        if self._notification_task is not None:
            tasks.add(self._notification_task)
        tasks.discard(current)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._reverse_tasks.clear()

    async def _stop_process(self) -> None:
        if self.proc.stdin is not None:
            self.proc.stdin.close()
        stdout_transport = None
        if self.proc.stdout is not None:
            stdout_transport = getattr(self.proc.stdout, "_transport", None)
        if stdout_transport is not None and not stdout_transport.is_closing():
            stdout_transport.close()
        if self._process_group_owned:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGTERM)
        elif self.proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                self.proc.terminate()
        if self.proc.returncode is None:
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=2)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    if self._process_group_owned:
                        os.killpg(self.proc.pid, signal.SIGKILL)
                    else:
                        self.proc.kill()
                await self.proc.wait()
        else:
            await self.proc.wait()
        if self._process_group_owned:
            # The stdio parent can exit before a helper it spawned. A group
            # created by this client remains ours until its last member exits.
            # Signal immediately after reap rather than widening the PGID-reuse
            # window with an arbitrary delay.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.proc.pid, signal.SIGKILL)

        # asyncio exposes no public close method for a Process or its stdout
        # StreamReader. Closing the owning transport is required when our sole
        # reader was cancelled before the pipe's EOF callback ran.
        process_transport = getattr(self.proc, "_transport", None)
        if process_transport is not None and not process_transport.is_closing():
            process_transport.close()
        if self.proc.stdin is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.proc.stdin.wait_closed(), timeout=1)
        await asyncio.sleep(0)

    def _close_files(self) -> None:
        if self._files_closed:
            return
        self._files_closed = True
        if not self._log_file.closed:
            self._log_file.close()
        if not self._stderr_file.closed:
            self._stderr_file.close()


def cancelled_permission_result(_params: JsonObject) -> JsonObject:
    """Safe default for unattended ACP permission requests."""
    return {"outcome": {"outcome": "cancelled"}}


def _is_json_id(value: Any) -> bool:
    return isinstance(value, str) or type(value) is int


def _open_private_append(path: Path) -> int:
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
    except BaseException:
        os.close(fd)
        raise
    return fd
