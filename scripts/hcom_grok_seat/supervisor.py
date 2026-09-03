#!/usr/bin/env python3
"""Production HCOM to Grok bridge with one visible persistent Grok session."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import os
import pty
import shlex
import signal
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .acp_client import AcpRemoteError, AsyncAcpClient, JsonObject
from .acp_session import PermissionBroker, initialize_authenticated
from .envelope import Envelope, EventRow, HcomReader, classify_event, prompt_text


CLIENT_ID = "hcom-grok-bridge"
STATE_SCHEMA = 1
POLL_SECONDS = 0.10
HEARTBEAT_SECONDS = 3.0
SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)


def _resolved(value: str | None, default: Path) -> Path:
    return Path(value).expanduser().resolve() if value else default.expanduser().resolve()


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _private_open(path: Path, mode: str) -> Any:
    _private_dir(path.parent)
    flags = os.O_CREAT | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= os.O_APPEND if "a" in mode else os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    os.fchmod(fd, 0o600)
    return os.fdopen(
        fd,
        mode,
        encoding=None if "b" in mode else "utf-8",
        buffering=0 if "b" in mode else 1,
    )


def _fsync_parent(path: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    _private_dir(path.parent)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(
        tmp,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()
        os.write(fd, raw)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    os.chmod(path, 0o600)
    _fsync_parent(path)


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass(frozen=True)
class Config:
    state_root: Path
    log_root: Path
    project: Path
    hcom_dir: Path
    hcom_db: Path
    grok_bin: str
    hcom_bin: str
    seat: str
    socket_path: str
    background_tui: bool
    session_mode: str = "resume"

    @classmethod
    def from_env(cls, background_tui: bool) -> "Config":
        home = Path.home()
        state_root = _resolved(
            os.environ.get("HCOM_GROK_STATE_ROOT"),
            home / ".local/state/hcom-grok/current",
        )
        log_root = _resolved(
            os.environ.get("HCOM_GROK_LOG_DIR"), home / "Library/Logs/hcom-grok"
        )
        project = _resolved(os.environ.get("HCOM_GROK_PROJECT"), Path.cwd())
        hcom_dir = _resolved(os.environ.get("HCOM_DIR"), home / ".hcom")
        seat = os.environ.get("HCOM_GROK_SEAT", "gsea").strip() or "gsea"
        seed = hashlib.sha256(f"{seat}\0{project}".encode()).hexdigest()[:8]
        socket_path = os.environ.get(
            "HCOM_GROK_SOCKET", f"/tmp/hg-{os.getuid()}-{seed}.sock"
        )
        if len(socket_path.encode()) >= 100:
            raise ValueError("Grok leader socket path is too long")
        session_mode = os.environ.get("HCOM_GROK_SESSION_MODE", "resume")
        if session_mode not in {"new", "resume"}:
            raise ValueError(f"unsupported Grok session mode: {session_mode}")
        return cls(
            state_root=state_root,
            log_root=log_root,
            project=project,
            hcom_dir=hcom_dir,
            hcom_db=hcom_dir / "hcom.db",
            grok_bin=os.environ.get("GROK_BIN", "grok"),
            hcom_bin=os.environ.get("HCOM_BIN", "hcom"),
            seat=seat,
            socket_path=socket_path,
            background_tui=background_tui,
            session_mode=session_mode,
        )

    @property
    def run_path(self) -> Path:
        return self.state_root / "run.json"

    @property
    def cursor_path(self) -> Path:
        return self.state_root / "cursor.json"

    @property
    def session_path(self) -> Path:
        return self.state_root / "session.json"

    @property
    def lock_path(self) -> Path:
        return self.state_root / "supervisor.lock"


def clean_child_env(config: Config, run_token: str) -> dict[str, str]:
    env = os.environ.copy()
    allowed_hcom = {
        "HCOM_DIR",
        "HCOM_GROK_STATE_ROOT",
        "HCOM_GROK_LOG_DIR",
        "HCOM_GROK_PROJECT",
        "HCOM_GROK_SEAT",
        "HCOM_GROK_SOCKET",
    }
    for key in tuple(env):
        if key.startswith("HCOM_") and key not in allowed_hcom:
            env.pop(key, None)
        if key.startswith(
            ("ANTIGRAVITY_", "CLAUDE_", "CODEX_", "CURSOR_", "GEMINI_", "KIMI_")
        ):
            env.pop(key, None)
        if key in {"CLAUDECODE", "GEMINI_CLI", "OPENCODE", "KILO"}:
            env.pop(key, None)
    env.update(
        {
            "HCOM_DIR": str(config.hcom_dir),
            "HCOM_INSTANCE_NAME": config.seat,
            "HCOM_PROCESS_ID": run_token,
            "GROK_FOLDER_TRUST": "0",
            "GROK_OAUTH2_REFERRER": "hcom",
            # This bridge is the sole HCOM delivery owner. Grok otherwise
            # imports the global Claude/Cursor HCOM hooks and receives every
            # letter a second time inside the same conversation.
            "GROK_CLAUDE_HOOKS_ENABLED": "false",
            "GROK_CURSOR_HOOKS_ENABLED": "false",
        }
    )
    return env


def live_process_executable(pid: int | None = None) -> str:
    """Record the executable path the OS exposes for safe operator signaling."""
    target_pid = os.getpid() if pid is None else pid
    try:
        result = subprocess.run(
            ["ps", "-ww", "-p", str(target_pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        command = result.stdout.strip()
        if result.returncode == 0 and command:
            argv = shlex.split(command)
            if argv:
                return str(Path(argv[0]).expanduser().resolve())
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return str(Path(sys.executable).expanduser().resolve())


def session_directory(project: Path, session_id: str) -> Path:
    encoded = quote(str(project), safe="")
    return Path.home() / ".grok" / "sessions" / encoded / session_id


def rules_text(config: Config) -> str:
    return (
        f"You are HCOM instance {config.seat}. HCOM_DIR is {config.hcom_dir}.\n"
        "HCOM letters arrive as ordinary user turns headed [HCOM DELIVERY].\n"
        "Never run hcom listen, poll for mail, or wait for mail.\n"
        "For an HCOM request, answer normally in your final response. "
        "The bridge returns that response automatically. Do not send a duplicate HCOM reply. "
        "Keep the completion summary concise unless the sender requests detail.\n"
        "For an HCOM inform, absorb it. Use hcom send only when a useful reply is needed.\n"
        f"If a request explicitly asks you to initiate a separate message, run hcom send --name {config.seat} "
        "with the recipient, intent, reply reference, and thread supplied by the user. "
        "That separate message does not replace your normal final response.\n"
        "Incoming ack messages are transport acknowledgements and never become model turns.\n"
    )


class TurnCollector:
    """Collect assistant text for the current prompt from ordered ACP updates."""

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self._chunks: dict[str, list[str]] = {}
        self._completed: dict[str, str] = {}
        self._stream_start_ms: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def begin(self, prompt_id: str) -> None:
        async with self._lock:
            self._chunks[prompt_id] = []
            self._completed.pop(prompt_id, None)
            self._stream_start_ms.pop(prompt_id, None)

    async def __call__(self, notification: JsonObject) -> None:
        if notification.get("method") not in {
            "session/update",
            "_x.ai/session/update",
            "_x.ai/session_notification",
        }:
            return
        params = notification.get("params")
        if not isinstance(params, dict) or params.get("sessionId") != self.session_id:
            return
        update = params.get("update")
        if not isinstance(update, dict):
            return
        meta = params.get("_meta")
        prompt_id = meta.get("promptId") if isinstance(meta, dict) else None
        if not isinstance(prompt_id, str) or not prompt_id:
            candidate = update.get("prompt_id")
            prompt_id = candidate if isinstance(candidate, str) else None
        if not prompt_id:
            return
        kind = update.get("sessionUpdate")
        async with self._lock:
            if prompt_id not in self._chunks:
                return
            if kind == "agent_message_chunk":
                content = update.get("content")
                if (
                    isinstance(content, dict)
                    and content.get("type") == "text"
                    and isinstance(content.get("text"), str)
                ):
                    stream_start = meta.get("streamStartMs") if isinstance(meta, dict) else None
                    if type(stream_start) is int:
                        previous = self._stream_start_ms.get(prompt_id)
                        if previous is not None and previous != stream_start:
                            self._chunks[prompt_id] = []
                        self._stream_start_ms[prompt_id] = stream_start
                    self._chunks[prompt_id].append(content["text"])
            elif kind == "turn_completed":
                reason = update.get("stop_reason")
                if isinstance(reason, str):
                    self._completed[prompt_id] = reason

    async def finish(self, prompt_id: str) -> tuple[str, str | None]:
        async with self._lock:
            text = "".join(self._chunks.pop(prompt_id, [])).strip()
            reason = self._completed.pop(prompt_id, None)
            self._stream_start_ms.pop(prompt_id, None)
            return text, reason


@dataclass
class TuiChild:
    pid: int
    process: asyncio.subprocess.Process | None = None
    pty_fd: int | None = None
    drain_task: asyncio.Task[None] | None = None

    def exited(self) -> bool:
        if self.process is not None:
            return self.process.returncode is not None
        try:
            child, _status = os.waitpid(self.pid, os.WNOHANG)
        except ChildProcessError:
            return True
        return child == self.pid


class Supervisor:
    def __init__(self, config: Config, operator_marker: str) -> None:
        self.config = config
        if not operator_marker.startswith("hcom-grok:"):
            raise ValueError("operator marker must start with hcom-grok:")
        self.operator_marker = operator_marker
        self.run_token = str(uuid.uuid4())
        self.stop_event = asyncio.Event()
        self.leader: asyncio.subprocess.Process | None = None
        self.tui: TuiChild | None = None
        self.client: AsyncAcpClient | None = None
        self.session_id = ""
        self.cursor: dict[str, Any] = {}
        self._lock_file: Any = None
        self._last_heartbeat = 0.0
        self.started_ns = 0

    def log(self, message: str, **fields: Any) -> None:
        record = {"time_ns": time.time_ns(), "message": message, **fields}
        with _private_open(self.config.log_root / "bridge.jsonl", "a") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    def harden_runtime_files(self) -> None:
        for directory in (self.config.state_root, self.config.log_root):
            if not directory.exists():
                continue
            os.chmod(directory, 0o700)
            for child in directory.iterdir():
                if child.is_file() and not child.is_symlink():
                    os.chmod(child, 0o600)

    def acquire_lock(self) -> None:
        _private_dir(self.config.state_root)
        handle = _private_open(self.config.lock_path, "a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise RuntimeError("another hcom-grok supervisor owns this state root")
        self._lock_file = handle

    def load_session(self) -> tuple[str, bool]:
        saved = load_json(self.config.session_path)
        if self.config.session_mode == "resume":
            if saved is None:
                raise RuntimeError(
                    "No resumable Grok session. Run hcom-grok to start fresh."
                )
            session_id = saved.get("session_id")
            project = saved.get("project")
            if not isinstance(session_id, str) or not session_id:
                raise RuntimeError(
                    "No resumable Grok session. Run hcom-grok to start fresh."
                )
            if project != str(self.config.project):
                raise RuntimeError(
                    f"Saved Grok session belongs to {project}, not {self.config.project}. "
                    "Run hcom-grok to start fresh."
                )
            if not session_directory(self.config.project, session_id).is_dir():
                raise RuntimeError(
                    f"Saved Grok session is no longer available: {session_id}. "
                    "Run hcom-grok to start fresh."
                )
            return session_id, True
        session_id = str(uuid.uuid4())
        atomic_json(
            self.config.session_path,
            {
                "schema": STATE_SCHEMA,
                "session_id": session_id,
                "project": str(self.config.project),
                "created_ns": time.time_ns(),
            },
        )
        return session_id, False

    def load_cursor(self) -> dict[str, Any]:
        if not self.config.hcom_db.is_file():
            raise RuntimeError(f"HCOM database does not exist: {self.config.hcom_db}")
        db_stat = self.config.hcom_db.stat()
        saved = load_json(self.config.cursor_path)
        if saved is not None:
            if saved.get("db_path") != str(self.config.hcom_db):
                raise RuntimeError("saved cursor belongs to another HCOM database")
            if saved.get("db_device") != db_stat.st_dev or saved.get("db_inode") != db_stat.st_ino:
                raise RuntimeError("HCOM database was replaced; clear pending explicitly")
            event_id = saved.get("last_event_id")
            if type(event_id) is int and event_id >= 0:
                return saved
        legacy = self.config.project / "scripts/hcom_grok_seat/runtime-main/gseat.last_id"
        event_id: int | None = None
        if legacy.is_file():
            with contextlib.suppress(ValueError, OSError):
                event_id = int(legacy.read_text().strip())
        reader = HcomReader(self.config.hcom_db)
        maximum = reader.max_event_id()
        if event_id is None or event_id < 0 or event_id > maximum:
            event_id = maximum
        state = {
            "schema": STATE_SCHEMA,
            "db_path": str(self.config.hcom_db),
            "db_device": db_stat.st_dev,
            "db_inode": db_stat.st_ino,
            "last_event_id": event_id,
            "last_event_sha256": reader.event_digest(event_id),
            "pending_reply": None,
            "updated_ns": time.time_ns(),
        }
        atomic_json(self.config.cursor_path, state)
        return state

    def save_cursor(
        self,
        event: EventRow,
        *,
        pending_reply: dict[str, Any] | None = None,
    ) -> None:
        self.cursor.update(
            {
                "last_event_id": event.event_id,
                "last_event_sha256": event.sha256,
                "pending_reply": pending_reply,
                "updated_ns": time.time_ns(),
            }
        )
        atomic_json(self.config.cursor_path, self.cursor)

    def clear_pending_reply(self) -> None:
        self.cursor["pending_reply"] = None
        self.cursor["updated_ns"] = time.time_ns()
        atomic_json(self.config.cursor_path, self.cursor)

    async def spawn_leader(self) -> None:
        socket = Path(self.config.socket_path)
        if socket.exists():
            socket.unlink()
        leader_log = _private_open(self.config.log_root / "leader.stdout.log", "ab")
        leader_err = _private_open(self.config.log_root / "leader.stderr.log", "ab")
        argv = [
            self.config.grok_bin,
            "agent",
            "leader",
            "--no-exit-on-disconnect",
            "--relay-on-demand",
            "--no-auto-update",
            "--leader-socket",
            self.config.socket_path,
            "--debug-file",
            str(self.config.log_root / "leader.debug.log"),
        ]
        self.leader = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(self.config.project),
            env=clean_child_env(self.config, self.run_token),
            stdout=leader_log,
            stderr=leader_err,
            start_new_session=True,
        )
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.leader.returncode is not None:
                raise RuntimeError(f"Grok leader exited with {self.leader.returncode}")
            if socket.exists() and stat.S_ISSOCK(socket.stat().st_mode):
                return
            await asyncio.sleep(0.05)
        raise RuntimeError("Grok leader socket did not become ready")

    def tui_argv(self, resume: bool) -> list[str]:
        session_args = ["--resume", self.session_id] if resume else ["--session-id", self.session_id]
        return [
            self.config.grok_bin,
            "--leader",
            "--leader-socket",
            self.config.socket_path,
            *session_args,
            "--always-approve",
            "--rules",
            rules_text(self.config),
            "--debug-file",
            str(self.config.log_root / "tui.debug.log"),
            "--no-auto-update",
        ]

    async def spawn_tui(self, resume: bool) -> None:
        argv = self.tui_argv(resume)
        env = clean_child_env(self.config, self.run_token)
        if not self.config.background_tui:
            proc = await asyncio.create_subprocess_exec(
                *argv, cwd=str(self.config.project), env=env
            )
            self.tui = TuiChild(proc.pid, process=proc)
            return
        pid, fd = pty.fork()
        if pid == 0:
            os.chdir(self.config.project)
            os.execvpe(self.config.grok_bin, argv, env)
        os.set_blocking(fd, False)
        child = TuiChild(pid, pty_fd=fd)
        child.drain_task = asyncio.create_task(
            self._drain_background_tui(fd), name="grok-tui-drain"
        )
        self.tui = child

    async def _drain_background_tui(self, fd: int) -> None:
        with _private_open(self.config.log_root / "tui.pty.log", "ab") as handle:
            while not self.stop_event.is_set():
                try:
                    chunk = os.read(fd, 65536)
                except BlockingIOError:
                    await asyncio.sleep(0.05)
                    continue
                except OSError:
                    return
                if not chunk:
                    return
                handle.write(chunk)

    async def wait_session(self) -> None:
        target = session_directory(self.config.project, self.session_id)
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if target.is_dir():
                return
            if self.tui is not None and self.tui.exited():
                raise RuntimeError("Grok TUI exited before creating its session")
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Grok session directory did not appear: {target}")

    async def connect_sidecar(self) -> TurnCollector:
        collector = TurnCollector(self.session_id)
        permission = PermissionBroker(
            self.session_id, decision=lambda _params: "allow_once", timeout=60
        )
        argv = [
            self.config.grok_bin,
            "agent",
            "--leader",
            "--leader-socket",
            self.config.socket_path,
            "--debug-file",
            str(self.config.log_root / "sidecar.debug.log"),
            "stdio",
        ]
        self.client = await AsyncAcpClient.spawn(
            argv,
            cwd=self.config.project,
            env=clean_child_env(self.config, self.run_token),
            log_path=self.config.log_root / "sidecar.jsonl",
            reverse_handlers={"session/request_permission": permission},
            notification_sink=collector,
        )
        await initialize_authenticated(self.client, self.config.project)
        params = {
            "sessionId": self.session_id,
            "cwd": str(self.config.project),
            "mcpServers": [],
        }
        last_error: BaseException | None = None
        for _ in range(8):
            try:
                result = await self.client.call("session/load", params, timeout=20)
                if not isinstance(result, dict):
                    raise RuntimeError("Grok session/load returned a non-object")
                await self.client.flush_notifications()
                return collector
            except AcpRemoteError as exc:
                last_error = exc
                await asyncio.sleep(0.4)
        raise RuntimeError(f"Grok session/load failed: {type(last_error).__name__}")

    async def hcom_command(
        self, args: list[str], timeout: float = 20
    ) -> subprocess.CompletedProcess[str]:
        def run() -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [self.config.hcom_bin, *args],
                cwd=self.config.project,
                env=clean_child_env(self.config, self.run_token),
                capture_output=True,
                text=True,
                timeout=timeout,
            )

        return await asyncio.to_thread(run)

    async def register_hcom(self) -> None:
        result = await self.hcom_command(["start", "--as", self.config.seat])
        if result.returncode != 0:
            raise RuntimeError(f"hcom start failed: {result.stderr.strip()[:300]}")
        self.heartbeat("listening", "ready")

    def heartbeat(self, status: str, detail: str = "") -> None:
        if self.tui is None or not self.config.hcom_db.is_file():
            return
        now = time.time()
        con = sqlite3.connect(self.config.hcom_db, timeout=5)
        try:
            con.execute("PRAGMA busy_timeout=5000")
            con.execute(
                "UPDATE instances SET pid=?,status=?,status_context=?,status_detail=?,"
                "status_time=?,last_seen=?,session_id=?,directory=? WHERE name=?",
                (
                    self.tui.pid,
                    status,
                    "hcom-grok",
                    detail,
                    int(now),
                    int(now),
                    self.session_id,
                    str(self.config.project),
                    self.config.seat,
                ),
            )
            con.execute(
                "INSERT OR REPLACE INTO process_bindings"
                "(process_id,session_id,instance_name,updated_at) VALUES(?,?,?,?)",
                (self.run_token, self.session_id, self.config.seat, now),
            )
            con.execute(
                "INSERT OR REPLACE INTO session_bindings"
                "(session_id,instance_name,created_at) VALUES(?,?,?)",
                (self.session_id, self.config.seat, now),
            )
            con.commit()
        finally:
            con.close()
        self._last_heartbeat = time.monotonic()
        self.harden_runtime_files()

    def unregister_hcom_fallback(self) -> None:
        if not self.config.hcom_db.is_file():
            return
        con = sqlite3.connect(self.config.hcom_db, timeout=5)
        try:
            con.execute("PRAGMA busy_timeout=5000")
            con.execute(
                "DELETE FROM process_bindings WHERE process_id=? AND instance_name=?",
                (self.run_token, self.config.seat),
            )
            con.execute(
                "UPDATE instances SET status='inactive',status_context='hcom-grok',"
                "status_detail='stopped',last_seen=? WHERE name=? AND session_id=?",
                (int(time.time()), self.config.seat, self.session_id),
            )
            con.commit()
        finally:
            con.close()

    async def unregister_hcom(self) -> None:
        """Publish native HCOM stop lifecycle, with a fail-safe local fallback."""
        try:
            result = await self.hcom_command(["stop", self.config.seat])
        except (OSError, subprocess.SubprocessError) as exc:
            self.log(
                "hcom_stop_failed",
                seat=self.config.seat,
                error_type=type(exc).__name__,
                stderr=str(exc)[:300],
            )
            self.unregister_hcom_fallback()
            return
        if result.returncode == 0:
            self.log("hcom_stopped", seat=self.config.seat)
            return
        self.log(
            "hcom_stop_failed",
            seat=self.config.seat,
            returncode=result.returncode,
            stderr=result.stderr.strip()[:300],
        )
        self.unregister_hcom_fallback()

    def write_run_state(self, ready: bool, **extra: Any) -> None:
        sidecar_proc = getattr(self.client, "proc", None)
        data = {
            "schema": STATE_SCHEMA,
            "supervisor_pid": os.getpid(),
            "tui_pid": self.tui.pid if self.tui else None,
            "leader_pid": self.leader.pid if self.leader else None,
            "sidecar_pid": getattr(sidecar_proc, "pid", None),
            "run_token": self.run_token,
            "socket_path": self.config.socket_path,
            "session_id": self.session_id,
            "launch_mode": "resumed" if self.config.session_mode == "resume" else "new",
            "project": str(self.config.project),
            "seat": self.config.seat,
            "background_tui": self.config.background_tui,
            "supervisor_executable": live_process_executable(),
            "argv_marker": self.operator_marker,
            "release": os.environ.get("HCOM_GROK_RELEASE", "development"),
            "started_ns": self.started_ns,
            "ready": ready,
            "busy": False,
            "updated_ns": time.time_ns(),
            **extra,
        }
        atomic_json(self.config.run_path, data)

    async def send_reply(self, pending: dict[str, Any]) -> None:
        args = [
            "send",
            "--name",
            self.config.seat,
            "--intent",
            "inform",
            "--reply-to",
            pending["reply_ref"],
        ]
        thread = pending.get("thread")
        if isinstance(thread, str) and thread:
            args.extend(["--thread", thread])
        args.extend([f"@{pending['sender']}", "--", pending["body"]])
        result = await self.hcom_command(args)
        if result.returncode != 0:
            raise RuntimeError(f"hcom reply failed: {result.stderr.strip()[:300]}")
        self.clear_pending_reply()
        self.log(
            "reply_sent",
            sender=pending["sender"],
            reply_ref=pending["reply_ref"],
        )

    async def retry_pending_reply(self) -> None:
        pending = self.cursor.get("pending_reply")
        if isinstance(pending, dict):
            await self.send_reply(pending)

    @staticmethod
    def prompt_id(envelope: Envelope) -> str:
        raw = f"{envelope.event.event_id}\0{envelope.event.sha256}".encode()
        return "hcom-grok:" + hashlib.sha256(raw).hexdigest()

    async def deliver(self, envelope: Envelope, collector: TurnCollector) -> None:
        assert self.client is not None
        delivery_id = f"local:{envelope.event.event_id}:{envelope.event.sha256[:16]}"
        body = prompt_text(envelope, self.config.seat, delivery_id)
        prompt_id = self.prompt_id(envelope)
        params = {
            "sessionId": self.session_id,
            "prompt": [{"type": "text", "text": body}],
            "_meta": {
                "promptId": prompt_id,
                "sendNow": False,
                "clientIdentifier": CLIENT_ID,
            },
        }
        await collector.begin(prompt_id)
        started_ns = time.time_ns()
        self.heartbeat("active", f"event:{envelope.event.event_id}")
        self.write_run_state(True, busy=True, active_event=envelope.event.event_id)
        result = await self.client.call("session/prompt", params, timeout=900)
        if not isinstance(result, dict) or not isinstance(result.get("stopReason"), str):
            raise RuntimeError("Grok session/prompt returned an invalid completion")
        await self.client.flush_notifications()
        assistant_text, observed_reason = await collector.finish(prompt_id)
        stop_reason = str(result["stopReason"])
        if observed_reason is not None and observed_reason != stop_reason:
            raise RuntimeError("Grok completion reason disagrees with the live update")
        pending: dict[str, Any] | None = None
        if envelope.intent == "request":
            pending = {
                "sender": envelope.sender,
                "reply_ref": envelope.reply_ref,
                "thread": envelope.thread,
                "body": assistant_text or "Grok completed the request without a text response.",
                "source_event_id": envelope.event.event_id,
            }
        self.save_cursor(envelope.event, pending_reply=pending)
        self.log(
            "delivery_completed",
            event_id=envelope.event.event_id,
            prompt_id=prompt_id,
            elapsed_ms=round((time.time_ns() - started_ns) / 1_000_000, 3),
            stop_reason=stop_reason,
            response_bytes=len(assistant_text.encode()),
        )
        if pending is not None:
            await self.send_reply(pending)
        self.heartbeat("listening", "ready")
        self.write_run_state(True, busy=False)

    async def process_mail(self, collector: TurnCollector) -> bool:
        reader = HcomReader(self.config.hcom_db)
        last_id = int(self.cursor["last_event_id"])
        rows = reader.rows_after(last_id, limit=256)
        if not rows:
            return False
        for row in rows:
            classified = classify_event(row, self.config.seat)
            if classified.disposition in {"request", "inform"}:
                assert classified.envelope is not None
                await self.deliver(classified.envelope, collector)
            else:
                self.save_cursor(row)
                if classified.disposition == "quarantine":
                    self.log(
                        "message_quarantined",
                        event_id=row.event_id,
                        reason=classified.reason,
                    )
        return True

    async def terminate_child(
        self, proc: asyncio.subprocess.Process | None, label: str
    ) -> None:
        if proc is None or proc.returncode is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), timeout=4)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            await proc.wait()
        self.log("child_stopped", role=label, returncode=proc.returncode)

    async def stop_tui(self) -> None:
        if self.tui is None:
            return
        if self.tui.drain_task is not None:
            self.tui.drain_task.cancel()
            await asyncio.gather(self.tui.drain_task, return_exceptions=True)
        if self.tui.exited():
            return
        with contextlib.suppress(ProcessLookupError):
            os.kill(self.tui.pid, signal.SIGTERM)
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline and not self.tui.exited():
            await asyncio.sleep(0.05)
        if not self.tui.exited():
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.tui.pid, signal.SIGKILL)

    async def cleanup(self) -> None:
        self.write_run_state(False, stopping=True)
        if self.client is not None:
            await self.client.close_transport()
        await self.stop_tui()
        await self.terminate_child(self.leader, "leader")
        with contextlib.suppress(OSError):
            Path(self.config.socket_path).unlink()
        await self.unregister_hcom()
        self.write_run_state(False, stopped=True)
        self.log("supervisor_stopped")

    def announce_ready(self, resume: bool) -> None:
        """Write a readiness line only when no visible TUI owns stdout."""
        if not self.config.background_tui:
            return
        print(
            "hcom-grok ready: "
            f"mode={'RESUMED' if resume else 'NEW'} seat={self.config.seat} "
            f"session={self.session_id} project={self.config.project}",
            flush=True,
        )

    async def run(self) -> int:
        self.acquire_lock()
        _private_dir(self.config.log_root)
        self.cursor = self.load_cursor()
        self.session_id, resume = self.load_session()
        started_ns = time.time_ns()
        self.started_ns = started_ns
        self.write_run_state(False, starting=True)
        try:
            await self.spawn_leader()
            await self.spawn_tui(resume)
            await self.wait_session()
            collector = await self.connect_sidecar()
            await self.register_hcom()
            await self.retry_pending_reply()
            self.write_run_state(True)
            self.announce_ready(resume)
            self.log(
                "supervisor_ready",
                session_id=self.session_id,
                start_ms=round((time.time_ns() - started_ns) / 1_000_000, 3),
                cursor=self.cursor["last_event_id"],
            )
            while not self.stop_event.is_set():
                if self.tui is not None and self.tui.exited():
                    self.log("tui_exited")
                    break
                processed = await self.process_mail(collector)
                now = time.monotonic()
                if now - self._last_heartbeat >= HEARTBEAT_SECONDS:
                    self.heartbeat("listening", "ready")
                    self.write_run_state(True)
                await asyncio.sleep(0 if processed else POLL_SECONDS)
            return 0
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
                return 0
            self.log(
                "supervisor_failed",
                error_type=type(exc).__name__,
                error=str(exc)[:500],
            )
            self.write_run_state(
                False, error_type=type(exc).__name__, error=str(exc)[:500]
            )
            return 1
        finally:
            await self.cleanup()


def install_shutdown_handlers(
    loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event
) -> None:
    """Route terminal close and interactive stop signals through normal cleanup."""
    for signum in SHUTDOWN_SIGNALS:
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop_event.set)


async def async_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hcom-grok-supervisor")
    sub = parser.add_subparsers(dest="command", required=True)
    run_parser = sub.add_parser("run")
    run_parser.add_argument("--background-tui", action="store_true")
    run_parser.add_argument("--operator-marker", required=True)
    args = parser.parse_args(argv)
    config = Config.from_env(args.background_tui)
    supervisor = Supervisor(config, args.operator_marker)
    loop = asyncio.get_running_loop()
    install_shutdown_handlers(loop, supervisor.stop_event)
    return await supervisor.run()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
