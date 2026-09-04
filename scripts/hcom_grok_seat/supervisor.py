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
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .acp_client import AsyncAcpClient, JsonObject
from .acp_session import (
    AcpHandshake,
    NewSessionFence,
    PermissionBroker,
    ResumeReplayFence,
    create_session as create_acp_session,
    initialize_authenticated,
    load_session as load_acp_session,
)
from .envelope import Envelope, EventRow, HcomReader, classify_event, prompt_text
from .pager_status import (
    MAX_STATUS_INPUT_BYTES,
    PagerStatusSetup,
    cleanup_pager_status,
    disabled_setup,
    prepare_pager_status,
    recover_pager_config,
    record_authenticated_pager_payload,
    stage_pager_status,
)
from .visible_session import (
    VisibleSessionObservation,
    observe_pager_session,
    observe_visible_session,
    session_directory_for,
)


CLIENT_ID = "hcom-grok-bridge"
CURSOR_STATE_SCHEMA = 1
SESSION_STATE_SCHEMA = 2
RUN_STATE_SCHEMA = 2
POLL_SECONDS = 0.10
HEARTBEAT_SECONDS = 3.0
OBSERVATION_SECONDS = 0.50
TRANSIENT_GRACE_SECONDS = 2.0
DELIVERY_OBSERVATION_TIMEOUT_SECONDS = 3.0
DELIVERY_OBSERVATION_POLL_SECONDS = 0.05
FOCUS_SETTLE_NS = 350_000_000
SHUTDOWN_SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)


def _resolved(value: str | None, default: Path) -> Path:
    return Path(value).expanduser().resolve() if value else default.expanduser().resolve()


def _private_dir(path: Path) -> None:
    created = False
    try:
        path.lstat()
    except FileNotFoundError:
        try:
            path.mkdir(mode=0o700, parents=True, exist_ok=False)
            created = True
        except FileExistsError:
            pass
    if created:
        os.chmod(path, 0o700)
    try:
        details = path.lstat()
    except OSError as exc:
        raise RuntimeError(f"private directory is unavailable: {path}") from exc
    if (
        not stat.S_ISDIR(details.st_mode)
        or stat.S_ISLNK(details.st_mode)
        or details.st_uid != os.getuid()
        or details.st_mode & 0o077
    ):
        raise RuntimeError(
            f"private directory must be real, user-owned, and mode 0700: {path}"
        )


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
        view = memoryview(raw)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write while publishing JSON state")
            view = view[written:]
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    finally:
        os.close(fd)
    os.replace(tmp, path)
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
    grok_home: Path
    hcom_dir: Path
    hcom_db: Path
    grok_bin: str
    hcom_bin: str
    seat: str
    socket_path: str
    background_tui: bool
    session_mode: str = "resume"
    sidecar_handoff: str = "close-first"
    launch_home: Path | None = None
    isolated_home: bool = False

    @classmethod
    def from_env(cls, background_tui: bool) -> "Config":
        home = Path.home().expanduser().resolve()
        state_root = _resolved(
            os.environ.get("HCOM_GROK_STATE_ROOT"),
            home / ".local/state/hcom-grok/current",
        )
        log_root = _resolved(
            os.environ.get("HCOM_GROK_LOG_DIR"), home / "Library/Logs/hcom-grok"
        )
        project = _resolved(os.environ.get("HCOM_GROK_PROJECT"), Path.cwd())
        grok_home = _resolved(os.environ.get("GROK_HOME"), home / ".grok")
        hcom_dir = _resolved(os.environ.get("HCOM_DIR"), home / ".hcom")
        seat = os.environ.get("HCOM_GROK_SEAT", "gsea").strip() or "gsea"
        seed = hashlib.sha256(f"{os.getuid()}\0{state_root}".encode()).hexdigest()[:8]
        socket_path = os.environ.get(
            "HCOM_GROK_SOCKET", f"/tmp/hg-{os.getuid()}-{seed}.sock"
        )
        if len(socket_path.encode()) >= 100:
            raise ValueError("Grok leader socket path is too long")
        session_mode = os.environ.get("HCOM_GROK_SESSION_MODE", "resume")
        if session_mode not in {"new", "resume"}:
            raise ValueError(f"unsupported Grok session mode: {session_mode}")
        sidecar_handoff = os.environ.get(
            "HCOM_GROK_SIDECAR_HANDOFF", "close-first"
        )
        if sidecar_handoff not in {"concurrent", "close-first"}:
            raise ValueError(
                f"unsupported Grok sidecar handoff mode: {sidecar_handoff}"
            )
        return cls(
            state_root=state_root,
            log_root=log_root,
            project=project,
            grok_home=grok_home,
            hcom_dir=hcom_dir,
            hcom_db=hcom_dir / "hcom.db",
            grok_bin=os.environ.get("GROK_BIN", "grok"),
            hcom_bin=os.environ.get("HCOM_BIN", "hcom"),
            seat=seat,
            socket_path=socket_path,
            background_tui=background_tui,
            session_mode=session_mode,
            sidecar_handoff=sidecar_handoff,
            launch_home=home,
            isolated_home=os.environ.get("HCOM_GROK_ISOLATED_HOME") == "1",
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
            "GROK_HOME": str(config.grok_home),
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


def session_directory(grok_home: Path, project: Path, session_id: str) -> Path:
    return session_directory_for(grok_home, project, session_id)


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


@dataclass(frozen=True)
class PagerPeerProof:
    """Stable process facts proven for one connected pager helper."""

    pid: int
    start_identity: str
    tui_pid: int
    tui_start_identity: str


@dataclass(frozen=True)
class SidecarBinding:
    """All session-specific state owned by one hidden ACP sidecar."""

    session_id: str
    client: AsyncAcpClient
    collector: TurnCollector
    permission: PermissionBroker
    handshake: AcpHandshake
    fence: ResumeReplayFence | NewSessionFence
    generation: int
    process_start: str | None

    @property
    def pid(self) -> int | None:
        value = getattr(getattr(self.client, "proc", None), "pid", None)
        return value if type(value) is int else None


def process_start_identity(pid: int) -> str | None:
    """Return a kernel start token with enough precision to reject PID reuse."""

    if sys.platform == "darwin":
        try:
            import ctypes

            class ProcBsdInfo(ctypes.Structure):
                _fields_ = [
                    ("pbi_flags", ctypes.c_uint32),
                    ("pbi_status", ctypes.c_uint32),
                    ("pbi_xstatus", ctypes.c_uint32),
                    ("pbi_pid", ctypes.c_uint32),
                    ("pbi_ppid", ctypes.c_uint32),
                    ("pbi_uid", ctypes.c_uint32),
                    ("pbi_gid", ctypes.c_uint32),
                    ("pbi_ruid", ctypes.c_uint32),
                    ("pbi_rgid", ctypes.c_uint32),
                    ("pbi_svuid", ctypes.c_uint32),
                    ("pbi_svgid", ctypes.c_uint32),
                    ("rfu_1", ctypes.c_uint32),
                    ("pbi_comm", ctypes.c_char * 16),
                    ("pbi_name", ctypes.c_char * 32),
                    ("pbi_nfiles", ctypes.c_uint32),
                    ("pbi_pgid", ctypes.c_uint32),
                    ("pbi_pjobc", ctypes.c_uint32),
                    ("e_tdev", ctypes.c_uint32),
                    ("e_tpgid", ctypes.c_uint32),
                    ("pbi_nice", ctypes.c_int32),
                    ("pbi_start_tvsec", ctypes.c_uint64),
                    ("pbi_start_tvusec", ctypes.c_uint64),
                ]

            info = ProcBsdInfo()
            libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
            proc_pidinfo = libproc.proc_pidinfo
            proc_pidinfo.argtypes = [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_uint64,
                ctypes.c_void_p,
                ctypes.c_int,
            ]
            proc_pidinfo.restype = ctypes.c_int
            size = ctypes.sizeof(info)
            if proc_pidinfo(pid, 3, 0, ctypes.byref(info), size) == size:
                return (
                    f"darwin:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}:"
                    f"uid={info.pbi_uid}"
                )
        except (AttributeError, OSError, ValueError):
            return None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        raw = proc_stat.read_text(encoding="ascii")
        closing = raw.rfind(")")
        fields = raw[closing + 2 :].split()
        start_ticks = fields[19]
        uid = proc_stat.stat().st_uid
    except (IndexError, OSError, ValueError):
        return None
    return f"procfs:{start_ticks}:uid={uid}"


def pid_alive(pid: int) -> bool:
    if type(pid) is not int or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def process_group_alive(group_id: int) -> bool:
    try:
        os.killpg(group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def process_argv(pid: int) -> list[str] | None:
    try:
        result = subprocess.run(
            ["ps", "-ww", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    command = result.stdout.strip()
    if result.returncode != 0 or not command:
        return None
    try:
        return shlex.split(command)
    except ValueError:
        return command.split()


def process_uid(pid: int) -> int | None:
    try:
        return Path(f"/proc/{pid}").stat().st_uid
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "uid="],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return int(result.stdout.strip()) if result.returncode == 0 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def process_parent_pid(pid: int) -> int | None:
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        raw = proc_stat.read_text(encoding="ascii")
        closing = raw.rfind(")")
        return int(raw[closing + 2 :].split()[1])
    except (IndexError, OSError, ValueError):
        pass
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "ppid="],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        return int(result.stdout.strip()) if result.returncode == 0 else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def unix_peer_identity(peer: Any) -> tuple[int, int] | None:
    """Return kernel-supplied ``(pid, uid)`` for one Unix stream peer."""

    try:
        if sys.platform == "darwin":
            # sys/un.h: SOL_LOCAL=0, LOCAL_PEERPID=2. Python does not expose
            # LOCAL_PEERPID on all supported macOS builds.
            raw = peer.getsockopt(0, 2, struct.calcsize("i"))
            pid = struct.unpack("i", raw)[0]
            uid = process_uid(pid)
            return (pid, uid) if uid is not None else None
        peercred = getattr(socket, "SO_PEERCRED", None)
        if peercred is not None:
            raw = peer.getsockopt(
                socket.SOL_SOCKET,
                peercred,
                struct.calcsize("3i"),
            )
            pid, uid, _gid = struct.unpack("3i", raw)
            return pid, uid
    except (OSError, struct.error):
        return None
    return None


def filesystem_socket_identity(path: Path) -> tuple[int, int] | None:
    try:
        details = path.lstat()
    except OSError:
        return None
    if (
        not stat.S_ISSOCK(details.st_mode)
        or stat.S_ISLNK(details.st_mode)
        or details.st_uid != os.getuid()
    ):
        return None
    return details.st_dev, details.st_ino


def remove_owned_socket(path: Path, expected: tuple[int, int] | None) -> bool:
    """Detach and delete only the exact socket inode created by this process."""

    if expected is None:
        return False
    current = filesystem_socket_identity(path)
    if current != expected:
        return False
    quarantine = path.with_name(f".{path.name}.retired-{uuid.uuid4().hex}")
    try:
        os.rename(path, quarantine)
        moved = filesystem_socket_identity(quarantine)
        if moved != expected:
            try:
                os.link(quarantine, path, follow_symlinks=False)
                quarantine.unlink()
            except OSError:
                pass
            return False
        quarantine.unlink()
        return not (path.exists() or path.is_symlink())
    except OSError:
        return False


def sidecar_process_matches(
    pid: int,
    *,
    expected_uid: int,
    expected_start: str,
    leader_socket: str,
) -> bool:
    """Prove a recorded PID is the exact adapter sidecar before signaling it."""

    if (
        process_uid(pid) != expected_uid
        or process_start_identity(pid) != expected_start
    ):
        return False
    argv = process_argv(pid)
    if not argv or "agent" not in argv or "--leader" not in argv:
        return False
    if argv[-1] != "stdio":
        return False
    positions = [index for index, value in enumerate(argv) if value == "--leader-socket"]
    if len(positions) != 1:
        return False
    socket_index = positions[0] + 1
    if socket_index >= len(argv) or argv[socket_index] != leader_socket:
        return False
    # Re-probe the non-reusable kernel token after argv inspection.
    return process_start_identity(pid) == expected_start


def _argv_option_matches(argv: list[str], option: str, expected: str) -> bool:
    positions = [index for index, value in enumerate(argv) if value == option]
    return (
        len(positions) == 1
        and positions[0] + 1 < len(argv)
        and argv[positions[0] + 1] == expected
    )


def leader_process_matches(
    pid: int,
    *,
    expected_uid: int,
    expected_start: str,
    leader_socket: str,
) -> bool:
    """Prove a recorded process is this adapter's no-exit Grok leader."""

    if (
        process_uid(pid) != expected_uid
        or process_start_identity(pid) != expected_start
    ):
        return False
    argv = process_argv(pid)
    if not (
        argv
        and "agent" in argv
        and "leader" in argv
        and "--no-exit-on-disconnect" in argv
        and _argv_option_matches(argv, "--leader-socket", leader_socket)
    ):
        return False
    return process_start_identity(pid) == expected_start


def tui_process_matches(
    pid: int,
    *,
    expected_uid: int,
    expected_start: str,
    leader_socket: str,
    session_id: str,
) -> bool:
    """Prove a recorded process is this adapter's visible Grok TUI."""

    if (
        process_uid(pid) != expected_uid
        or process_start_identity(pid) != expected_start
    ):
        return False
    argv = process_argv(pid)
    if not argv:
        return False
    session_matches = _argv_option_matches(
        argv, "--resume", session_id
    ) or _argv_option_matches(argv, "--session-id", session_id)
    if not (
        "--leader" in argv
        and _argv_option_matches(argv, "--leader-socket", leader_socket)
        and session_matches
    ):
        return False
    return process_start_identity(pid) == expected_start


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
        self.binding: SidecarBinding | None = None
        self.session_id = ""
        self.launch_session_id = ""
        self.previous_session_id: str | None = None
        self.binding_generation = 0
        self.transition_reason: str | None = None
        self.visible_session_id: str | None = None
        self.focus_source: str | None = None
        self.status_trigger: str | None = None
        self.status_sample_monotonic_ns: int | None = None
        self.focus_state_monotonic_ns: int | None = None
        self.registry_session_id: str | None = None
        self.registry_observation_reason: str | None = None
        self.pager_status: PagerStatusSetup = disabled_setup(
            "pager status admission has not run",
            state_root=config.state_root,
            grok_home=config.grok_home,
        )
        self._pager_launch_gate_ns: int | None = None
        self._pager_server: asyncio.AbstractServer | None = None
        self._pager_socket_identity: tuple[int, int] | None = None
        self._pager_focus_floor_ns = 0
        self._pager_publication_floor_ns = 0
        self._tui_start_identity: str | None = None
        self._tui_argv_session_id: str | None = None
        self._tui_process_group: int | None = None
        self._leader_start_identity: str | None = None
        self._leader_socket_identity: tuple[int, int] | None = None
        self.pager_bootstrap_retained: str | None = None
        self.bridge_state = "STARTING"
        self.degraded_reason: str | None = None
        self._degraded_recoverable = False
        self.cursor: dict[str, Any] = {}
        self._lock_file: Any = None
        self._binding_lock = asyncio.Lock()
        # Serializes a pager focus commit with only the brief ACP request-frame
        # admission boundary. It is deliberately not held while a model turn
        # runs, so pager updates continue throughout long prompts.
        self._focus_admission_lock = asyncio.Lock()
        self._publication_lock = asyncio.Lock()
        self._binding_transition: dict[str, Any] | None = None
        self._last_transition: dict[str, Any] | None = None
        self._orphan_report: str | None = None
        self._obsolete_binding: SidecarBinding | None = None
        self._repair_hcom = False
        self._delivery_active = False
        self._active_event_id: int | None = None
        self._transient_since: float | None = None
        self._pending_observation: VisibleSessionObservation | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._needs_bridge_context = False
        self._hcom_registered = False
        self._last_heartbeat = 0.0
        self.started_ns = 0

    def log(self, message: str, **fields: Any) -> None:
        record = {"time_ns": time.time_ns(), "message": message, **fields}
        with _private_open(self.config.log_root / "bridge.jsonl", "a") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    def harden_runtime_files(self) -> None:
        paths = {
            self.config.lock_path,
            self.config.cursor_path,
            self.config.session_path,
            self.config.run_path,
            self.config.log_root / "bridge.jsonl",
            self.config.log_root / "leader.stdout.log",
            self.config.log_root / "leader.stderr.log",
            self.config.log_root / "leader.debug.log",
            self.config.log_root / "tui.debug.log",
            self.config.log_root / "tui.pty.log",
        }
        paths.add(self.pager_status.status_path)
        for child in paths:
            if not child.exists() or child.is_symlink():
                continue
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            if nofollow:
                flags |= nofollow
            try:
                fd = os.open(child, flags)
            except OSError:
                continue
            try:
                details = os.fstat(fd)
                if stat.S_ISREG(details.st_mode):
                    os.fchmod(fd, 0o600)
            finally:
                os.close(fd)

    def acquire_lock(self) -> None:
        _private_dir(self.config.state_root)
        handle = _private_open(self.config.lock_path, "a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            raise RuntimeError("another hcom-grok supervisor owns this state root")
        self._lock_file = handle

    async def recover_stale_sidecars(self) -> None:
        state = load_json(self.config.run_path)
        if not state:
            return
        transition = state.get("binding_transition")
        if not isinstance(transition, dict):
            return
        prior_supervisor = state.get("supervisor_pid")
        prior_start = state.get("supervisor_process_start")
        if type(prior_supervisor) is int and pid_alive(prior_supervisor):
            if not isinstance(prior_start, str) or (
                process_start_identity(prior_supervisor) == prior_start
            ):
                self._orphan_report = (
                    f"stale transition retained because prior supervisor PID "
                    f"{prior_supervisor} is live"
                )
                return
        candidates = (
            ("candidate", "candidate_sidecar_pid", "candidate_process_start"),
            ("obsolete", "obsolete_sidecar_pid", "obsolete_process_start"),
        )
        for label, pid_key, start_key in candidates:
            pid = transition.get(pid_key)
            if type(pid) is not int or not pid_alive(pid):
                continue
            uid = transition.get("candidate_uid")
            start = transition.get(start_key)
            socket = transition.get("leader_socket")
            role = transition.get("candidate_role")
            if (
                type(uid) is not int
                or not isinstance(start, str)
                or not start
                or socket != self.config.socket_path
                or role != "grok-agent-stdio"
                or not sidecar_process_matches(
                    pid,
                    expected_uid=uid,
                    expected_start=start,
                    leader_socket=self.config.socket_path,
                )
            ):
                self._orphan_report = (
                    f"unproven stale {label} sidecar PID {pid}; not signaled"
                )
                continue
            os.killpg(pid, signal.SIGTERM)
            deadline = time.monotonic() + 3
            while process_group_alive(pid) and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            if process_group_alive(pid):
                if pid_alive(pid) and not sidecar_process_matches(
                    pid,
                    expected_uid=uid,
                    expected_start=start,
                    leader_socket=self.config.socket_path,
                ):
                    self._orphan_report = (
                        f"stale {label} sidecar PID {pid} changed identity; not killed"
                    )
                    continue
                os.killpg(pid, signal.SIGKILL)
            self.log("stale_sidecar_reaped", role=label, pid=pid)

    async def _reap_stale_process(
        self,
        *,
        label: str,
        pid: int,
        matches: Callable[[], bool],
        process_group_owned: bool,
    ) -> None:
        """Terminate one positively identified child from a hard-killed run."""

        if not pid_alive(pid):
            return
        if not matches():
            raise RuntimeError(
                f"unproven stale {label} PID {pid}; refusing to signal or relaunch"
            )
        if process_group_owned:
            os.killpg(pid, signal.SIGTERM)
            alive: Callable[[int], bool] = process_group_alive
        else:
            os.kill(pid, signal.SIGTERM)
            alive = pid_alive
        deadline = time.monotonic() + 3
        while alive(pid) and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        if alive(pid):
            if pid_alive(pid) and not matches():
                raise RuntimeError(
                    f"stale {label} PID {pid} changed identity before SIGKILL"
                )
            if process_group_owned:
                os.killpg(pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        self.log("stale_process_reaped", role=label, pid=pid)

    @staticmethod
    def _run_socket_identity(
        state: dict[str, Any], prefix: str
    ) -> tuple[int, int] | None:
        device = state.get(f"{prefix}_device")
        inode = state.get(f"{prefix}_inode")
        if type(device) is int and type(inode) is int and device >= 0 and inode > 0:
            return device, inode
        return None

    async def recover_stale_runtime(self) -> None:
        """Recover only kernel-identified children and sockets of a dead owner."""

        state = load_json(self.config.run_path)
        if not state:
            return
        prior_supervisor = state.get("supervisor_pid")
        prior_start = state.get("supervisor_process_start")
        if type(prior_supervisor) is int and pid_alive(prior_supervisor):
            if not isinstance(prior_start, str) or (
                process_start_identity(prior_supervisor) == prior_start
            ):
                raise RuntimeError(
                    f"prior hcom-grok supervisor PID {prior_supervisor} is still live"
                )

        await self.recover_stale_sidecars()
        uid = os.getuid()
        saved_session = state.get("tui_argv_session_id", state.get("session_id"))
        if not isinstance(saved_session, str):
            saved_session = ""

        tui_pid = state.get("tui_pid")
        tui_start = state.get("tui_process_start")
        if type(tui_pid) is int and pid_alive(tui_pid):
            if not isinstance(tui_start, str) or not tui_start:
                raise RuntimeError(
                    f"stale TUI PID {tui_pid} lacks a safe process identity"
                )
            tui_group = state.get("tui_process_group")
            await self._reap_stale_process(
                label="tui",
                pid=tui_pid,
                matches=lambda: tui_process_matches(
                    tui_pid,
                    expected_uid=uid,
                    expected_start=tui_start,
                    leader_socket=self.config.socket_path,
                    session_id=saved_session,
                ),
                process_group_owned=(type(tui_group) is int and tui_group == tui_pid),
            )

        sidecar_pid = state.get("sidecar_pid")
        sidecar_start = state.get("sidecar_process_start")
        if type(sidecar_pid) is int and pid_alive(sidecar_pid):
            if not isinstance(sidecar_start, str) or not sidecar_start:
                raise RuntimeError(
                    f"stale sidecar PID {sidecar_pid} lacks a safe process identity"
                )
            await self._reap_stale_process(
                label="sidecar",
                pid=sidecar_pid,
                matches=lambda: sidecar_process_matches(
                    sidecar_pid,
                    expected_uid=uid,
                    expected_start=sidecar_start,
                    leader_socket=self.config.socket_path,
                ),
                process_group_owned=True,
            )

        leader_pid = state.get("leader_pid")
        leader_start = state.get("leader_process_start")
        if type(leader_pid) is int and pid_alive(leader_pid):
            if not isinstance(leader_start, str) or not leader_start:
                raise RuntimeError(
                    f"stale leader PID {leader_pid} lacks a safe process identity"
                )
            await self._reap_stale_process(
                label="leader",
                pid=leader_pid,
                matches=lambda: leader_process_matches(
                    leader_pid,
                    expected_uid=uid,
                    expected_start=leader_start,
                    leader_socket=self.config.socket_path,
                ),
                process_group_owned=True,
            )

        leader_identity = self._run_socket_identity(state, "leader_socket")
        if state.get("socket_path") == self.config.socket_path:
            remove_owned_socket(Path(self.config.socket_path), leader_identity)
        pager_path = self.pager_status.ingest_socket_path
        pager_identity = self._run_socket_identity(state, "pager_socket")
        if pager_path is not None and state.get("pager_socket_path") == str(pager_path):
            remove_owned_socket(pager_path, pager_identity)

        remaining: list[str] = []
        for path in (Path(self.config.socket_path), pager_path):
            if path is not None and (path.exists() or path.is_symlink()):
                remaining.append(str(path))
        if remaining:
            raise RuntimeError(
                "stale runtime socket could not be proven owned: "
                + ", ".join(remaining)
            )

    def load_session(self) -> tuple[str, bool]:
        saved = load_json(self.config.session_path)
        if self.config.session_path.exists() and saved is None:
            raise RuntimeError(
                f"Saved Grok session state is unreadable or invalid: {self.config.session_path}"
            )
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
            try:
                saved_project = (
                    Path(project).expanduser().resolve()
                    if isinstance(project, str) and project
                    else None
                )
            except (OSError, RuntimeError, ValueError):
                saved_project = None
            if saved_project != self.config.project.expanduser().resolve():
                raise RuntimeError(
                    f"Saved Grok session belongs to {project}, not {self.config.project}. "
                    "Run hcom-grok to start fresh."
                )
            if not session_directory(
                self.config.grok_home, self.config.project, session_id
            ).is_dir():
                raise RuntimeError(
                    f"Saved Grok session is no longer available: {session_id}. "
                    "Run hcom-grok to start fresh."
                )
            schema = saved.get("schema", 1)
            if type(schema) is not int or schema not in {1, SESSION_STATE_SCHEMA}:
                raise RuntimeError(f"Unsupported saved session schema: {schema!r}")
            if schema == SESSION_STATE_SCHEMA:
                launch_id = saved.get("launch_session_id")
                generation = saved.get("binding_generation")
                bound_at_ns = saved.get("bound_at_ns")
                previous = saved.get("previous_session_id")
                reason = saved.get("transition_reason")
                if not isinstance(launch_id, str) or not launch_id:
                    raise RuntimeError("Saved session has an invalid launch_session_id")
                if type(generation) is not int or generation < 0:
                    raise RuntimeError("Saved session has an invalid binding_generation")
                if type(bound_at_ns) is not int or bound_at_ns <= 0:
                    raise RuntimeError("Saved session has an invalid bound_at_ns")
                if previous is not None and not isinstance(previous, str):
                    raise RuntimeError("Saved session has an invalid previous_session_id")
                if reason is not None and not isinstance(reason, str):
                    raise RuntimeError("Saved session has an invalid transition_reason")
            launch_id = saved.get("launch_session_id", session_id)
            self.launch_session_id = (
                launch_id if isinstance(launch_id, str) and launch_id else session_id
            )
            generation = saved.get("binding_generation", 0)
            self.binding_generation = (
                generation if type(generation) is int and generation >= 0 else 0
            )
            previous = saved.get("previous_session_id")
            self.previous_session_id = previous if isinstance(previous, str) else None
            reason = saved.get("transition_reason")
            self.transition_reason = reason if isinstance(reason, str) else None
            return session_id, True
        # New conversations are created by the ACP sidecar after the leader is
        # up.  Grok's direct ``--session-id`` construction path does not arm
        # the pager status runner, while ACP creation followed by the first
        # visible ``--resume`` does.  Do not invent a UUID or persist a session
        # until the agent has returned one.
        self.launch_session_id = ""
        self.previous_session_id = None
        self.binding_generation = 0
        self.transition_reason = None
        return "", False

    def adopt_created_session(self, session_id: str) -> None:
        """Persist the ACP-created launch session before its first TUI attach."""

        try:
            canonical = str(uuid.UUID(session_id))
        except ValueError as exc:
            raise RuntimeError("ACP returned an invalid created session ID") from exc
        if canonical != session_id.lower():
            raise RuntimeError("ACP returned a non-canonical created session ID")
        self.session_id = canonical
        self.launch_session_id = canonical
        self.previous_session_id = None
        self.binding_generation = 0
        self.transition_reason = None
        atomic_json(
            self.config.session_path,
            {
                "schema": SESSION_STATE_SCHEMA,
                "session_id": canonical,
                "launch_session_id": canonical,
                "binding_generation": 0,
                "bound_at_ns": time.time_ns(),
                "previous_session_id": None,
                "transition_reason": None,
                "project": str(self.config.project),
                "created_ns": time.time_ns(),
            },
        )

    def persist_binding_session(self, target_session_id: str) -> None:
        saved = load_json(self.config.session_path) or {}
        created_ns = saved.get("created_ns")
        if type(created_ns) is not int:
            created_ns = time.time_ns()
        atomic_json(
            self.config.session_path,
            {
                "schema": SESSION_STATE_SCHEMA,
                "session_id": target_session_id,
                "launch_session_id": self.launch_session_id or self.session_id,
                "binding_generation": self.binding_generation + 1,
                "bound_at_ns": time.time_ns(),
                "previous_session_id": self.session_id,
                "transition_reason": "visible_tui_selection",
                "project": str(self.config.project),
                "created_ns": created_ns,
            },
        )

    def _tui_is_owned_child(self, pid: int) -> bool:
        return self.tui is not None and pid == self.tui.pid and not self.tui.exited()

    def observe_binding(
        self, minimum_monotonic_ns: int | None = None
    ) -> VisibleSessionObservation:
        binding = self.binding
        if binding is None or self.tui is None:
            return VisibleSessionObservation(
                "unsafe", "no active sidecar binding or owned TUI"
            )
        minimum_sample_ns = max(
            minimum_monotonic_ns or 0,
            self._pager_publication_floor_ns,
        )
        return observe_pager_session(
            setup=self.pager_status,
            grok_home=self.config.grok_home,
            project=self.config.project,
            tui_pid=self.tui.pid,
            bound_session_id=binding.session_id,
            agent_version=binding.handshake.agent_version,
            minimum_monotonic_ns=minimum_sample_ns or None,
            minimum_state_monotonic_ns=self._pager_focus_floor_ns or None,
            pid_alive=self._tui_is_owned_child,
        )

    def observe_registry_diagnostic(self) -> None:
        """Sample Grok's PID registry for diagnostics, never focus authority."""

        binding = self.binding
        if binding is None or self.tui is None:
            self.registry_session_id = None
            self.registry_observation_reason = "no active binding or TUI"
            return
        observed = observe_visible_session(
            grok_home=self.config.grok_home,
            project=self.config.project,
            tui_pid=self.tui.pid,
            bound_session_id=binding.session_id,
            agent_version=binding.handshake.agent_version,
            pid_alive=self._tui_is_owned_child,
        )
        self.registry_session_id = observed.session_id
        self.registry_observation_reason = observed.reason

    async def observe_fresh_binding(
        self, minimum_monotonic_ns: int | None
    ) -> VisibleSessionObservation:
        """Wait a bounded time for the pager's next post-gate sample."""

        deadline = time.monotonic() + DELIVERY_OBSERVATION_TIMEOUT_SECONDS
        last = self.observe_binding(minimum_monotonic_ns)
        while last.kind == "transient-missing" and time.monotonic() < deadline:
            if self.tui is None or self.tui.exited():
                return VisibleSessionObservation("unsafe", "owned TUI exited")
            await asyncio.sleep(DELIVERY_OBSERVATION_POLL_SECONDS)
            last = self.observe_binding(minimum_monotonic_ns)
        return last

    async def observe_settled_binding(
        self,
        minimum_monotonic_ns: int | None = None,
    ) -> VisibleSessionObservation:
        """Require the last state-driven focus identity to finish debouncing."""

        first = await self.observe_fresh_binding(minimum_monotonic_ns)
        if first.kind not in {"aligned", "visible-change"}:
            return first
        state_ns = first.focus_state_monotonic_ns
        if state_ns is None:
            # Synthetic observer seams used by state-machine tests do not carry
            # pager timing. Production pager observations always do.
            return first
        remaining_ns = state_ns + FOCUS_SETTLE_NS - time.monotonic_ns()
        if remaining_ns > 0:
            await asyncio.sleep(remaining_ns / 1_000_000_000)
        confirmed = self.observe_binding()
        if (
            confirmed.kind not in {"aligned", "visible-change"}
            or confirmed.session_id != first.session_id
            or confirmed.focus_state_monotonic_ns != state_ns
        ):
            return VisibleSessionObservation(
                "transient-missing",
                "pager focus changed during delivery debounce",
                session_id=confirmed.session_id,
                pid=confirmed.pid,
                cwd=confirmed.cwd,
                focus_source=confirmed.focus_source,
                status_trigger=confirmed.status_trigger,
                status_sample_monotonic_ns=(
                    confirmed.status_sample_monotonic_ns
                ),
                focus_state_monotonic_ns=(
                    confirmed.focus_state_monotonic_ns
                ),
            )
        return confirmed

    def _observation_matches_focus_floor(
        self,
        observed: VisibleSessionObservation,
    ) -> bool:
        """Reject a sample superseded by a newer authenticated state event."""

        state_ns = observed.focus_state_monotonic_ns
        if state_ns is None:
            # Test seams may provide synthetic observations without pager
            # timing. Production pager observations always carry this field.
            return True
        return state_ns == self._pager_focus_floor_ns

    def _enter_degraded(self, reason: str, *, recoverable: bool = False) -> None:
        bounded = reason[:500]
        if self.bridge_state == "DEGRADED" and self.degraded_reason == bounded:
            self._degraded_recoverable = self._degraded_recoverable and recoverable
            return
        self.bridge_state = "DEGRADED"
        self.degraded_reason = bounded
        self._degraded_recoverable = recoverable
        self.log("bridge_degraded", reason=self.degraded_reason)

    def _record_observation(self, observed: VisibleSessionObservation) -> None:
        if observed.session_id is not None:
            self.visible_session_id = observed.session_id
        if observed.focus_source is not None:
            self.focus_source = observed.focus_source
        if observed.status_trigger is not None:
            self.status_trigger = observed.status_trigger
        if observed.status_sample_monotonic_ns is not None:
            self.status_sample_monotonic_ns = observed.status_sample_monotonic_ns
        if observed.focus_state_monotonic_ns is not None:
            self.focus_state_monotonic_ns = observed.focus_state_monotonic_ns
        if observed.kind == "aligned":
            self._transient_since = None
            self._pending_observation = None
            if self.bridge_state in {"REBIND_PENDING", "BOUND", "STARTING"} or (
                self.bridge_state == "DEGRADED" and self._degraded_recoverable
            ):
                self.bridge_state = "DELIVERING" if self._delivery_active else "BOUND"
                self.degraded_reason = None
                self._degraded_recoverable = False
            return
        if observed.kind == "visible-change":
            self._transient_since = None
            self._pending_observation = observed
            if self.bridge_state in {"REBINDING", "RECOVERING_COMMITTED"}:
                return
            if self.bridge_state != "DEGRADED" or self._degraded_recoverable:
                self.bridge_state = "REBIND_PENDING"
                self.degraded_reason = observed.reason[:500]
                self._degraded_recoverable = False
            return
        if observed.kind == "transient-missing":
            now = time.monotonic()
            if self._transient_since is None:
                self._transient_since = now
            self._pending_observation = observed
            elapsed = now - self._transient_since
            if self.bridge_state in {"REBINDING", "RECOVERING_COMMITTED"}:
                return
            if elapsed >= TRANSIENT_GRACE_SECONDS:
                self._enter_degraded(observed.reason, recoverable=True)
            elif self.bridge_state != "DEGRADED":
                self.bridge_state = "REBIND_PENDING"
                self.degraded_reason = observed.reason[:500]
            return
        self._pending_observation = observed
        self._enter_degraded(observed.reason)

    async def close_binding(self, binding: SidecarBinding) -> None:
        await binding.permission.cancel_pending()
        await binding.client.close_transport()
        await binding.permission.wait_pending_cancelled()
        await binding.permission.finish_cancellation()

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
            "schema": CURSOR_STATE_SCHEMA,
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
        existing_pending = self.cursor.get("pending_reply")
        if pending_reply is None and isinstance(existing_pending, dict):
            pending_reply = existing_pending
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

    def _pager_peer_is_owned(
        self, peer: Any
    ) -> tuple[PagerPeerProof | None, str]:
        identity = unix_peer_identity(peer)
        if identity is None:
            return None, "kernel peer identity is unavailable"
        peer_pid, peer_uid = identity
        if peer_uid != os.getuid() or peer_pid <= 0:
            return None, "pager peer uid is not owned"
        tui = self.tui
        tui_start = self._tui_start_identity
        if tui is None or tui_start is None:
            return None, "owned TUI is unavailable"
        if process_start_identity(tui.pid) != tui_start:
            return None, "owned TUI identity changed"
        peer_start = process_start_identity(peer_pid)
        if peer_start is None:
            return None, "pager peer exited before inspection"
        expected_shim = self.pager_status.shim_path
        expected_socket = self.pager_status.ingest_socket_path
        if expected_shim is None or expected_socket is None:
            return None, "pager command identity is incomplete"
        try:
            executable_matches = (
                Path(live_process_executable(peer_pid)).resolve()
                == Path(live_process_executable()).resolve()
            )
        except (OSError, RuntimeError, ValueError):
            executable_matches = False
        argv = process_argv(peer_pid)
        try:
            shim_argv_matches = (
                argv is not None
                and len(argv) == 2
                and Path(argv[1]).expanduser().resolve() == expected_shim.resolve()
            )
        except (OSError, RuntimeError, ValueError):
            shim_argv_matches = False
        if not executable_matches or not shim_argv_matches:
            return None, "pager peer executable or argv is not the owned shim"
        if process_parent_pid(peer_pid) != tui.pid:
            return None, "pager peer is not a direct child of the owned TUI"
        if (
            process_start_identity(peer_pid) != peer_start
            or process_start_identity(tui.pid) != tui_start
        ):
            return None, "pager peer or TUI identity changed during inspection"
        return (
            PagerPeerProof(peer_pid, peer_start, tui.pid, tui_start),
            "owned pager peer",
        )

    def _pager_peer_proof_is_current(
        self, proof: PagerPeerProof
    ) -> tuple[bool, str]:
        """Cheap final identity fence immediately before focus publication."""

        tui = self.tui
        if (
            tui is None
            or tui.pid != proof.tui_pid
            or self._tui_start_identity != proof.tui_start_identity
        ):
            return False, "owned TUI reference changed during payload read"
        if process_start_identity(proof.pid) != proof.start_identity:
            return False, "pager peer identity changed during payload read"
        if process_start_identity(proof.tui_pid) != proof.tui_start_identity:
            return False, "owned TUI identity changed during payload read"
        return True, "owned pager peer identity remains current"

    async def _handle_pager_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            peer = writer.get_extra_info("socket")
            proof, reason = await asyncio.to_thread(
                self._pager_peer_is_owned,
                peer,
            )
            if proof is None:
                self.log("pager_status_peer_rejected", peer_pid=None, reason=reason)
                return
            try:
                raw = await asyncio.wait_for(
                    reader.read(MAX_STATUS_INPUT_BYTES + 1),
                    timeout=0.5,
                )
            except TimeoutError:
                return
            # The helper waits for this acknowledgement, keeping its kernel PID
            # alive through both identity checks and the bounded read. Serialize
            # the actual focus publication with only ACP frame admission; never
            # with the full model turn.
            async with self._focus_admission_lock:
                still_owned, current_reason = self._pager_peer_proof_is_current(
                    proof
                )
                if not still_owned:
                    self.log(
                        "pager_status_peer_rejected",
                        peer_pid=proof.pid,
                        reason=current_reason,
                    )
                    return
                assert self.tui is not None
                outcome = record_authenticated_pager_payload(
                    self.pager_status,
                    raw,
                    tui_pid=self.tui.pid,
                )
                if outcome.state_observed_monotonic_ns is not None:
                    self._pager_focus_floor_ns = max(
                        self._pager_focus_floor_ns,
                        outcome.state_observed_monotonic_ns,
                    )
                if outcome.captured_monotonic_ns is not None:
                    self._pager_publication_floor_ns = max(
                        self._pager_publication_floor_ns,
                        outcome.captured_monotonic_ns,
                    )
            if outcome.kind == "invalidated":
                self.log("pager_status_invalidated", reason=outcome.reason)
            writer.write(b"1")
            await writer.drain()
        except (ConnectionError, OSError, RuntimeError):
            return
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def start_pager_broker(self) -> None:
        if not self.pager_status.enabled:
            return
        path = self.pager_status.ingest_socket_path
        if path is None:
            raise RuntimeError("pager ingest socket path is absent")
        if path.exists() or path.is_symlink():
            kind = "socket" if filesystem_socket_identity(path) is not None else "non-socket"
            raise RuntimeError(f"pager ingest socket path already contains a {kind}: {path}")
        self._pager_server = await asyncio.start_unix_server(
            self._handle_pager_connection,
            path=str(path),
        )
        os.chmod(path, 0o600)
        self._pager_socket_identity = filesystem_socket_identity(path)
        if self._pager_socket_identity is None:
            self._pager_server.close()
            await self._pager_server.wait_closed()
            self._pager_server = None
            raise RuntimeError("pager ingest socket was not safely created")

    async def stop_pager_broker(self) -> None:
        if self._pager_server is not None:
            self._pager_server.close()
            await self._pager_server.wait_closed()
            self._pager_server = None
        path = self.pager_status.ingest_socket_path
        if path is not None:
            remove_owned_socket(path, self._pager_socket_identity)
        self._pager_socket_identity = None

    async def spawn_leader(self) -> None:
        socket_path = Path(self.config.socket_path)
        if socket_path.exists() or socket_path.is_symlink():
            kind = (
                "socket"
                if filesystem_socket_identity(socket_path) is not None
                else "non-socket"
            )
            raise RuntimeError(
                f"Grok leader socket path already contains a {kind}: {socket_path}"
            )
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
        self._leader_start_identity = process_start_identity(self.leader.pid)
        if self._leader_start_identity is None:
            raise RuntimeError("could not capture the owned leader process identity")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.leader.returncode is not None:
                raise RuntimeError(f"Grok leader exited with {self.leader.returncode}")
            identity = filesystem_socket_identity(socket_path)
            if identity is not None:
                self._leader_socket_identity = identity
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
            self._tui_argv_session_id = self.session_id
            self._tui_start_identity = process_start_identity(proc.pid)
            with contextlib.suppress(OSError):
                self._tui_process_group = os.getpgid(proc.pid)
            if self._tui_start_identity is None:
                raise RuntimeError("could not capture the owned TUI process identity")
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
        self._tui_argv_session_id = self.session_id
        self._tui_start_identity = process_start_identity(pid)
        with contextlib.suppress(OSError):
            self._tui_process_group = os.getpgid(pid)
        if self._tui_start_identity is None:
            raise RuntimeError("could not capture the owned TUI process identity")

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
        target = session_directory(
            self.config.grok_home, self.config.project, self.session_id
        )
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if target.is_dir():
                return
            if self.tui is not None and self.tui.exited():
                raise RuntimeError("Grok TUI exited before creating its session")
            await asyncio.sleep(0.1)
        raise RuntimeError(f"Grok session directory did not appear: {target}")

    async def create_sidecar(self) -> SidecarBinding:
        """Create the launch session through ACP before the first pager attaches."""

        attempt_id = uuid.uuid4().hex[:10]
        fence = NewSessionFence()
        collector: TurnCollector | None = None
        permission: PermissionBroker | None = None

        async def permission_proxy(params: JsonObject) -> JsonObject:
            if permission is None:
                # A permission request before session/new returns cannot be
                # assigned safely to an as-yet unknown conversation.
                return {"outcome": {"outcome": "cancelled"}}
            return await permission(params)

        argv = [
            self.config.grok_bin,
            "agent",
            "--leader",
            "--leader-socket",
            self.config.socket_path,
            "--debug-file",
            str(self.config.log_root / f"creator-{attempt_id}.debug.log"),
            "stdio",
        ]
        client: AsyncAcpClient | None = None
        self._binding_transition = {
            "phase": "creating_launch_session",
            "generation": 0,
            "reason": "acp_session_new",
            "started_ns": time.time_ns(),
            "authoritative": False,
            "candidate_uid": os.getuid(),
            "candidate_role": "grok-agent-stdio",
            "leader_socket": self.config.socket_path,
        }
        try:
            client = await AsyncAcpClient.spawn(
                argv,
                cwd=self.config.project,
                env=clean_child_env(self.config, self.run_token),
                log_path=self.config.log_root / f"creator-{attempt_id}.jsonl",
                reverse_handlers={"session/request_permission": permission_proxy},
                notification_sink=fence,
                start_new_session=True,
            )
            candidate_pid = client.proc.pid
            self._binding_transition.update(
                {
                    "phase": "creator_spawned",
                    "candidate_sidecar_pid": candidate_pid,
                    "candidate_process_start": process_start_identity(candidate_pid),
                }
            )
            self.write_run_state(False, starting=True, startup_phase="creator-spawned")
            handshake = await initialize_authenticated(client, self.config.project)

            def make_collector(session_id: str) -> TurnCollector:
                nonlocal collector
                collector = TurnCollector(session_id)
                return collector

            session_id, _ = await create_acp_session(
                client,
                self.config.project,
                notification_fence=fence,
                live_sink_factory=make_collector,
            )
            if collector is None:
                raise RuntimeError("ACP new-session did not establish a turn collector")
            permission = PermissionBroker(
                session_id, decision=lambda _params: "allow_once", timeout=60
            )
            self._binding_transition.update(
                {
                    "phase": "creator_ready",
                    "to_session_id": session_id,
                }
            )
            return SidecarBinding(
                session_id,
                client,
                collector,
                permission,
                handshake,
                fence,
                0,
                process_start_identity(client.proc.pid),
            )
        except BaseException:
            if client is not None:
                await client.close_transport()
            raise

    async def connect_sidecar(
        self,
        target_session_id: str | None = None,
        *,
        generation: int | None = None,
    ) -> SidecarBinding:
        target = target_session_id or self.session_id
        target_generation = (
            self.binding_generation if generation is None else generation
        )
        attempt_id = uuid.uuid4().hex[:10]
        collector = TurnCollector(target)
        permission = PermissionBroker(
            target, decision=lambda _params: "allow_once", timeout=60
        )
        replay_fence = ResumeReplayFence(target, "load", live_sink=collector)
        argv = [
            self.config.grok_bin,
            "agent",
            "--leader",
            "--leader-socket",
            self.config.socket_path,
            "--debug-file",
            str(
                self.config.log_root
                / f"sidecar-g{target_generation}-{attempt_id}.debug.log"
            ),
            "stdio",
        ]
        client: AsyncAcpClient | None = None
        try:
            client = await AsyncAcpClient.spawn(
                argv,
                cwd=self.config.project,
                env=clean_child_env(self.config, self.run_token),
                log_path=(
                    self.config.log_root
                    / f"sidecar-g{target_generation}-{attempt_id}.jsonl"
                ),
                reverse_handlers={"session/request_permission": permission},
                notification_sink=replay_fence,
                start_new_session=True,
            )
            if self._binding_transition is not None:
                candidate_pid = getattr(client.proc, "pid", None)
                self._binding_transition.update(
                    {
                        "phase": "candidate_spawned",
                        "candidate_sidecar_pid": candidate_pid,
                        "candidate_uid": os.getuid(),
                        "candidate_process_start": (
                            process_start_identity(candidate_pid)
                            if type(candidate_pid) is int
                            else None
                        ),
                        "leader_socket": self.config.socket_path,
                        "candidate_role": "grok-agent-stdio",
                    }
                )
                self.write_run_state(True)
            handshake = await initialize_authenticated(client, self.config.project)
            await load_acp_session(
                client,
                handshake,
                target,
                self.config.project,
                replay_fence=replay_fence,
            )
            recovered = await replay_fence.activate_passthrough()
            self.log(
                "sidecar_loaded",
                session_id=target,
                replay_events=len(replay_fence.replay_event_ids),
                recovered_live_events=len(recovered),
            )
            return SidecarBinding(
                target,
                client,
                collector,
                permission,
                handshake,
                replay_fence,
                target_generation,
                process_start_identity(client.proc.pid),
            )
        except BaseException:
            if client is not None:
                await client.close_transport()
            raise

    def _transition_record(
        self, from_session_id: str, to_session_id: str, generation: int
    ) -> dict[str, Any]:
        old = self.binding
        return {
            "phase": "preparing",
            "from_session_id": from_session_id,
            "to_session_id": to_session_id,
            "generation": generation,
            "reason": "visible_tui_selection",
            "tui_pid": self.tui.pid if self.tui is not None else None,
            "started_ns": time.time_ns(),
            "authoritative": False,
            "candidate_uid": os.getuid(),
            "candidate_role": "grok-agent-stdio",
            "leader_socket": self.config.socket_path,
            "obsolete_sidecar_pid": old.pid if old is not None else None,
            "obsolete_process_start": old.process_start if old is not None else None,
        }

    async def _finish_transition(
        self, *, outcome: str, error: str | None = None
    ) -> None:
        async with self._publication_lock:
            record = dict(self._binding_transition or {})
            record.update(
                {
                    "phase": "finished",
                    "outcome": outcome,
                    "finished_ns": time.time_ns(),
                }
            )
            if error is not None:
                record["error"] = error[:500]
            record.pop("candidate_process_start", None)
            self._last_transition = record
            self._binding_transition = None
            self.write_run_state(True)

    async def rebind_visible(
        self, initial: VisibleSessionObservation
    ) -> bool:
        """Replace the hidden sidecar and commit the TUI-selected session."""

        old = self.binding
        if old is None or initial.session_id is None:
            self._enter_degraded("cannot rebind without an existing binding and target")
            return False
        from_session_id = self.session_id
        generation = self.binding_generation + 1
        async with self._binding_lock:
            if self.binding is not old or old.session_id != from_session_id:
                self._enter_degraded("binding changed before rebind admission")
                return False
            self.bridge_state = "REBINDING"
            self.degraded_reason = None
            self._binding_transition = self._transition_record(
                from_session_id, initial.session_id, generation
            )
        await self.publish_runtime("listening", "rebinding")

        old_closed = False
        target = initial.session_id
        for _attempt in range(4):
            fresh = await self.observe_settled_binding()
            async with self._binding_lock:
                self._record_observation(fresh)
                if fresh.kind == "visible-change" and fresh.session_id is not None:
                    target = fresh.session_id
                elif fresh.kind == "aligned" and old_closed:
                    # The pane switched back after close-first detached the old
                    # transport. Recreate that same durable binding.
                    target = from_session_id
                elif fresh.kind == "aligned":
                    self.bridge_state = "BOUND"
                    self.degraded_reason = None
                    await self._finish_transition(outcome="superseded")
                    return True
                else:
                    reason = f"rebind pre-attach observation: {fresh.reason}"
                    self._enter_degraded(reason)
                    await self._finish_transition(outcome="failed", error=reason)
                    if old_closed:
                        self.binding = None
                    return False
                assert self._binding_transition is not None
                self._binding_transition["to_session_id"] = target
                self._binding_transition["phase"] = "attaching"

            if self.config.sidecar_handoff == "close-first" and not old_closed:
                try:
                    await self.close_binding(old)
                    old_closed = True
                except BaseException as exc:
                    reason = (
                        "failed to close old sidecar before rebind: "
                        f"{type(exc).__name__}"
                    )
                    self._enter_degraded(reason)
                    await self._finish_transition(outcome="failed", error=reason)
                    return False

            try:
                candidate_generation = (
                    generation
                    if target != from_session_id
                    else self.binding_generation
                )
                candidate = await self.connect_sidecar(
                    target,
                    generation=candidate_generation,
                )
            except BaseException as exc:
                reason = f"candidate sidecar attach failed: {type(exc).__name__}"
                self._enter_degraded(reason)
                if old_closed:
                    self.binding = None
                await self._finish_transition(outcome="failed", error=reason)
                return False

            # Require liveness after the candidate has attached. A refresh may
            # carry forward only the last state-proven identity; a new state
            # event advances the broker's focus floor and wins below.
            post_attach_gate_ns = time.monotonic_ns()
            final_observation = await self.observe_settled_binding(
                post_attach_gate_ns
            )
            async with self._binding_lock:
                stable_target = (
                    final_observation.session_id == target
                    and final_observation.kind in {"aligned", "visible-change"}
                    and self._observation_matches_focus_floor(final_observation)
                )
                if not stable_target:
                    self._record_observation(final_observation)
            if not stable_target:
                await self.close_binding(candidate)
                if final_observation.kind in {"aligned", "visible-change"} and (
                    final_observation.session_id is not None
                ):
                    target = final_observation.session_id
                    if target == from_session_id and not old_closed:
                        self.bridge_state = "BOUND"
                        self.degraded_reason = None
                        self._pending_observation = None
                        await self._finish_transition(outcome="superseded")
                        return True
                    continue
                reason = f"rebind pre-commit observation: {final_observation.reason}"
                self._enter_degraded(reason)
                if old_closed:
                    self.binding = None
                await self._finish_transition(outcome="failed", error=reason)
                return False

            mirror_error: BaseException | None = None
            publication_error: BaseException | None = None
            committed_change = target != self.session_id
            committed = False
            commit_error: BaseException | None = None
            commit_observation: VisibleSessionObservation | None = None
            commit_stable = False
            async with self._focus_admission_lock:
                async with self._binding_lock:
                    async with self._publication_lock:
                        commit_observation = self.observe_binding()
                        commit_stable = (
                            commit_observation.session_id == target
                            and commit_observation.kind
                            in {"aligned", "visible-change"}
                            and self._observation_matches_focus_floor(
                                commit_observation
                            )
                        )
                        self._record_observation(commit_observation)
                        if commit_stable:
                            try:
                                assert self._binding_transition is not None
                                if committed_change:
                                    self.persist_binding_session(target)
                                    committed = True
                                    previous = self.session_id
                                    self.session_id = target
                                    self.previous_session_id = previous
                                    self.binding_generation = generation
                                    self.transition_reason = "visible_tui_selection"
                                self.binding = candidate
                                self.visible_session_id = target
                                self.bridge_state = "RECOVERING_COMMITTED"
                                self._binding_transition.update(
                                    {
                                        "phase": "committed",
                                        "authoritative": True,
                                        "committed_ns": time.time_ns(),
                                    }
                                )
                                try:
                                    self._mirror_hcom_binding(
                                        "listening", "rebinding"
                                    )
                                except BaseException as exc:
                                    mirror_error = exc
                                    self._repair_hcom = True
                                try:
                                    self.write_run_state(True)
                                except BaseException as exc:
                                    publication_error = exc
                            except BaseException as exc:
                                commit_error = exc

            if not commit_stable:
                await self.close_binding(candidate)
                assert commit_observation is not None
                if commit_observation.kind in {"aligned", "visible-change"} and (
                    commit_observation.session_id is not None
                ):
                    target = commit_observation.session_id
                    if target == from_session_id and not old_closed:
                        self.bridge_state = "BOUND"
                        self.degraded_reason = None
                        self._pending_observation = None
                        await self._finish_transition(outcome="superseded")
                        return True
                    continue
                reason = (
                    "rebind commit-boundary observation: "
                    f"{commit_observation.reason}"
                )
                self._enter_degraded(reason)
                if old_closed:
                    self.binding = None
                await self._finish_transition(outcome="failed", error=reason)
                return False

            if commit_error is not None:
                if not committed:
                    with contextlib.suppress(BaseException):
                        await self.close_binding(candidate)
                    reason = (
                        "binding commit failed before session.json: "
                        f"{type(commit_error).__name__}"
                    )
                    self._enter_degraded(reason)
                    if old_closed:
                        self.binding = None
                    await self._finish_transition(outcome="failed", error=reason)
                    return False
                self.session_id = target
                self.previous_session_id = from_session_id
                self.binding_generation = generation
                self.transition_reason = "visible_tui_selection"
                self.binding = candidate
                self.visible_session_id = target
                self._repair_hcom = True
                if not old_closed:
                    self._obsolete_binding = old
                reason = (
                    "binding committed; recovering forward after state promotion "
                    f"failure: {type(commit_error).__name__}"
                )
                self._enter_degraded(reason)
                return False

            if not old_closed:
                try:
                    await self.close_binding(old)
                except BaseException as exc:
                    self._obsolete_binding = old
                    reason = (
                        "committed new binding but obsolete sidecar did not close: "
                        f"{type(exc).__name__}"
                    )
                    self._enter_degraded(reason)
                    if self._binding_transition is not None:
                        self._binding_transition.update(
                            {
                                "phase": "obsolete_cleanup",
                                "obsolete_sidecar_pid": old.pid,
                                "obsolete_process_start": (
                                    process_start_identity(old.pid)
                                    if old.pid is not None
                                    else None
                                ),
                            }
                        )
                    self.write_run_state(True)
                    return False

            if mirror_error is not None:
                reason = (
                    "binding committed but HCOM mirror failed: "
                    f"{type(mirror_error).__name__}"
                )
                self._enter_degraded(reason)
                with contextlib.suppress(BaseException):
                    self.write_run_state(True)
                return False

            if publication_error is not None:
                reason = (
                    "binding committed but run state publication failed: "
                    f"{type(publication_error).__name__}"
                )
                self._enter_degraded(reason)
                self._repair_hcom = True
                return False

            self.bridge_state = "BOUND"
            self.degraded_reason = None
            self._pending_observation = None
            self._transient_since = None
            self._needs_bridge_context = committed_change
            await self._finish_transition(outcome="committed")
            self.log(
                "binding_rebound",
                from_session_id=from_session_id,
                to_session_id=target,
                generation=self.binding_generation,
                reason="visible_tui_selection",
            )
            return True

        reason = "visible session changed too rapidly to establish a stable binding"
        self._enter_degraded(reason)
        if old_closed:
            self.binding = None
        await self._finish_transition(outcome="failed", error=reason)
        return False

    async def ensure_delivery_binding(self) -> SidecarBinding | None:
        binding = self.binding
        if binding is None:
            return None
        if self.bridge_state == "DEGRADED" and not self._degraded_recoverable:
            return None
        delivery_gate_ns = time.monotonic_ns()
        observed = await self.observe_settled_binding(delivery_gate_ns)
        async with self._binding_lock:
            if self.binding is not binding:
                self._enter_degraded(
                    "binding changed during delivery admission",
                    recoverable=True,
                )
                return None
            self._record_observation(observed)
            if (
                observed.kind == "aligned"
                and self.bridge_state != "DEGRADED"
                and self._observation_matches_focus_floor(observed)
            ):
                return self.binding
            if observed.kind == "transient-missing":
                self._enter_degraded(observed.reason, recoverable=True)
        if observed.kind == "visible-change" and self.bridge_state != "DEGRADED":
            if await self.rebind_visible(observed):
                rebound = self.binding
                if rebound is None:
                    return None
                confirmation = await self.observe_settled_binding(
                    time.monotonic_ns()
                )
                async with self._binding_lock:
                    if self.binding is not rebound:
                        self._enter_degraded(
                            "binding changed after rebind delivery admission",
                            recoverable=True,
                        )
                        return None
                    self._record_observation(confirmation)
                    if (
                        confirmation.kind == "aligned"
                        and confirmation.session_id == rebound.session_id
                        and self.bridge_state != "DEGRADED"
                        and self._observation_matches_focus_floor(confirmation)
                    ):
                        return rebound
        return None

    async def maintain_binding(self) -> bool:
        """Repair/rebind only while the main loop is between HCOM deliveries."""

        if self._obsolete_binding is not None:
            try:
                await self.close_binding(self._obsolete_binding)
            except BaseException:
                return False
            self._obsolete_binding = None
            self.bridge_state = "BOUND"
            self.degraded_reason = None
            await self._finish_transition(outcome="recovered")
        if self._repair_hcom:
            try:
                await self.publish_runtime("listening", "repairing binding")
            except BaseException:
                return False
            self._repair_hcom = False
            self.bridge_state = "BOUND"
            self.degraded_reason = None
            await self._finish_transition(outcome="recovered")
        if self.bridge_state == "DEGRADED" and not self._degraded_recoverable:
            return False
        observed = self.observe_binding()
        async with self._binding_lock:
            self._record_observation(observed)
        if observed.kind == "aligned":
            return self.binding is not None
        if observed.kind == "visible-change":
            return await self.rebind_visible(observed)
        return False

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
        self._hcom_registered = True
        if self.bridge_state == "DEGRADED":
            self.heartbeat("blocked", self.degraded_reason or "bridge degraded")
        elif self.bridge_state == "REBIND_PENDING":
            self.heartbeat("blocked", self.degraded_reason or "binding pending")
        else:
            self.heartbeat("listening", "ready")

    def heartbeat(self, status: str, detail: str = "") -> None:
        if self.tui is None or not self.config.hcom_db.is_file():
            return
        self._mirror_hcom_binding(status, detail)
        self._last_heartbeat = time.monotonic()
        self.harden_runtime_files()

    async def publish_runtime(
        self, status: str, detail: str = "", *, ready: bool = True, **extra: Any
    ) -> None:
        async with self._publication_lock:
            self.heartbeat(status, detail)
            self.write_run_state(ready, **extra)

    def _mirror_hcom_binding(self, status: str, detail: str = "") -> None:
        """Idempotently mirror the durable/in-memory binding in one DB transaction."""

        if self.tui is None or not self.config.hcom_db.is_file():
            return
        now = time.time()
        bound_id = self.session_id
        con = sqlite3.connect(self.config.hcom_db, timeout=5)
        try:
            con.execute("PRAGMA busy_timeout=5000")
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("BEGIN IMMEDIATE")
            con.execute("PRAGMA defer_foreign_keys=ON")
            previous_row = con.execute(
                "SELECT session_id FROM instances WHERE name=?",
                (self.config.seat,),
            ).fetchone()
            previous_id = previous_row[0] if previous_row else None
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
                    bound_id,
                    str(self.config.project),
                    self.config.seat,
                ),
            )
            if (
                isinstance(previous_id, str)
                and previous_id
                and previous_id != bound_id
            ):
                con.execute(
                    "UPDATE instances SET parent_session_id=? WHERE parent_session_id=?",
                    (bound_id, previous_id),
                )
            con.execute(
                "INSERT OR REPLACE INTO process_bindings"
                "(process_id,session_id,instance_name,updated_at) VALUES(?,?,?,?)",
                (self.run_token, bound_id, self.config.seat, now),
            )
            con.execute(
                "INSERT OR REPLACE INTO session_bindings"
                "(session_id,instance_name,created_at) VALUES(?,?,?)",
                (bound_id, self.config.seat, now),
            )
            con.execute(
                "DELETE FROM session_bindings WHERE instance_name=? AND session_id<>?",
                (self.config.seat, bound_id),
            )
            con.commit()
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

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
        active_client = self.binding.client if self.binding is not None else None
        sidecar_proc = getattr(active_client, "proc", None)
        alignment = "unknown"
        if self.visible_session_id is not None:
            alignment = (
                "aligned"
                if self.visible_session_id == self.session_id
                else "divergent"
            )
        data = {
            "schema": RUN_STATE_SCHEMA,
            "supervisor_pid": os.getpid(),
            "supervisor_process_start": process_start_identity(os.getpid()),
            "tui_pid": self.tui.pid if self.tui else None,
            "tui_process_start": self._tui_start_identity,
            "tui_process_group": self._tui_process_group,
            "leader_pid": self.leader.pid if self.leader else None,
            "leader_process_start": self._leader_start_identity,
            "sidecar_pid": getattr(sidecar_proc, "pid", None),
            "sidecar_process_start": (
                self.binding.process_start if self.binding is not None else None
            ),
            "run_token": self.run_token,
            "socket_path": self.config.socket_path,
            "session_id": self.session_id,
            "tui_argv_session_id": self._tui_argv_session_id,
            "bound_session_id": self.session_id,
            "visible_session_id": self.visible_session_id,
            "session_alignment": alignment,
            "focus_source": self.focus_source,
            "status_trigger": self.status_trigger,
            "status_sample_monotonic_ns": self.status_sample_monotonic_ns,
            "focus_state_monotonic_ns": self.focus_state_monotonic_ns,
            "pager_status_enabled": self.pager_status.enabled,
            "pager_status_reason": self.pager_status.reason,
            "pager_status_path": str(self.pager_status.status_path),
            "pager_socket_path": (
                str(self.pager_status.ingest_socket_path)
                if self.pager_status.ingest_socket_path is not None
                else None
            ),
            "pager_socket_device": (
                self._pager_socket_identity[0]
                if self._pager_socket_identity is not None
                else None
            ),
            "pager_socket_inode": (
                self._pager_socket_identity[1]
                if self._pager_socket_identity is not None
                else None
            ),
            "pager_bootstrap_retained": self.pager_bootstrap_retained,
            "registry_session_id": self.registry_session_id,
            "registry_observation_reason": self.registry_observation_reason,
            "binding_generation": self.binding_generation,
            "bridge_state": self.bridge_state,
            "degraded_reason": self.degraded_reason,
            "grok_home": str(self.config.grok_home),
            "launch_mode": "resumed" if self.config.session_mode == "resume" else "new",
            "project": str(self.config.project),
            "seat": self.config.seat,
            "background_tui": self.config.background_tui,
            "leader_socket_device": (
                self._leader_socket_identity[0]
                if self._leader_socket_identity is not None
                else None
            ),
            "leader_socket_inode": (
                self._leader_socket_identity[1]
                if self._leader_socket_identity is not None
                else None
            ),
            "supervisor_executable": live_process_executable(),
            "argv_marker": self.operator_marker,
            "release": os.environ.get("HCOM_GROK_RELEASE", "development"),
            "started_ns": self.started_ns,
            "ready": ready,
            "busy": False,
            "updated_ns": time.time_ns(),
        }
        if self._binding_transition is not None:
            data["binding_transition"] = self._binding_transition
        if self._last_transition is not None:
            data["last_binding_transition"] = self._last_transition
        if self._orphan_report is not None:
            data["orphan_report"] = self._orphan_report
        data.update(extra)
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

    async def deliver(self, envelope: Envelope, binding: SidecarBinding) -> None:
        if binding is not self.binding or binding.session_id != self.session_id:
            raise RuntimeError("delivery binding changed after admission")
        delivery_id = f"local:{envelope.event.event_id}:{envelope.event.sha256[:16]}"
        body = prompt_text(envelope, self.config.seat, delivery_id)
        include_bridge_context = self._needs_bridge_context
        if include_bridge_context:
            body = "[HCOM BRIDGE CONTEXT]\n" + rules_text(self.config) + "\n" + body
        prompt_id = self.prompt_id(envelope)
        params = {
            "sessionId": binding.session_id,
            "prompt": [{"type": "text", "text": body}],
            "_meta": {
                "promptId": prompt_id,
                "sendNow": False,
                "clientIdentifier": CLIENT_ID,
            },
        }
        await binding.collector.begin(prompt_id)
        started_ns = time.time_ns()
        await self.publish_runtime(
            "active",
            f"event:{envelope.event.event_id}",
            busy=True,
            active_event=envelope.event.event_id,
        )
        # Order focus updates and the actual ACP frame, not the whole turn. Once
        # begin_request returns, the complete prompt frame has drained to this
        # binding; pager state may then continue updating while Grok works.
        async with self._focus_admission_lock:
            admitted = self.observe_binding()
            if (
                binding is not self.binding
                or binding.session_id != self.session_id
                or admitted.kind != "aligned"
                or admitted.session_id != binding.session_id
                or not self._observation_matches_focus_floor(admitted)
            ):
                self._record_observation(admitted)
                raise RuntimeError("visible focus changed before ACP prompt admission")
            handle = await binding.client.begin_request("session/prompt", params)
        result = await binding.client.await_response(handle, timeout=900)
        if not isinstance(result, dict) or not isinstance(result.get("stopReason"), str):
            raise RuntimeError("Grok session/prompt returned an invalid completion")
        await binding.client.flush_notifications()
        assistant_text, observed_reason = await binding.collector.finish(prompt_id)
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
        if include_bridge_context:
            self._needs_bridge_context = False
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
        detail = "visible session change pending" if self._pending_observation else "ready"
        await self.publish_runtime("listening", detail, busy=False)

    async def process_mail(self) -> str:
        pending = self.cursor.get("pending_reply")
        if isinstance(pending, dict):
            try:
                await self.send_reply(pending)
            except Exception as exc:
                self._enter_degraded(
                    f"pending HCOM reply could not be sent: {type(exc).__name__}"
                )
                return "held"
        reader = HcomReader(self.config.hcom_db)
        last_id = int(self.cursor["last_event_id"])
        rows = reader.rows_after(last_id, limit=256)
        if not rows:
            return "idle"
        progressed = False
        for row in rows:
            classified = classify_event(row, self.config.seat)
            if classified.disposition in {"request", "inform"}:
                assert classified.envelope is not None
                binding = await self.ensure_delivery_binding()
                if binding is None:
                    return "held"
                self._delivery_active = True
                self._active_event_id = row.event_id
                self.bridge_state = "DELIVERING"
                try:
                    await self.deliver(classified.envelope, binding)
                    if self._pending_observation is None:
                        self.bridge_state = "BOUND"
                        self.degraded_reason = None
                except Exception as exc:
                    self._enter_degraded(
                        f"delivery failed before durable cursor completion: {type(exc).__name__}"
                    )
                    await self.publish_runtime(
                        "blocked",
                        self.degraded_reason or "delivery failed",
                        busy=False,
                    )
                    return "held"
                finally:
                    self._delivery_active = False
                    self._active_event_id = None
                # A focus transition noticed during the completed turn is a
                # batch boundary. Do not let later ack/quarantine rows move the
                # durable cursor before the next model-bound admission repairs
                # the binding.
                if (
                    self._pending_observation is not None
                    or self.bridge_state != "BOUND"
                ):
                    return "progressed"
            else:
                self.save_cursor(row)
                if classified.disposition == "quarantine":
                    self.log(
                        "message_quarantined",
                        event_id=row.event_id,
                        reason=classified.reason,
                    )
            progressed = True
        return "progressed" if progressed else "idle"

    async def runtime_monitor(self) -> None:
        """Keep visibility diagnostics and HCOM heartbeats live during long turns."""

        while not self.stop_event.is_set():
            try:
                before = (
                    self.visible_session_id,
                    self.bridge_state,
                    self.degraded_reason,
                )
                if self.binding is not None:
                    async with self._binding_lock:
                        observed = self.observe_binding()
                        self._record_observation(observed)
                    self.observe_registry_diagnostic()
                if self._delivery_active:
                    status = "active"
                    detail = f"event:{self._active_event_id}"
                elif self.bridge_state == "DEGRADED":
                    status = "blocked"
                    detail = self.degraded_reason or "bridge degraded"
                else:
                    status = "listening"
                    detail = self.bridge_state.lower()
                changed = before != (
                    self.visible_session_id,
                    self.bridge_state,
                    self.degraded_reason,
                )
                heartbeat_due = (
                    time.monotonic() - self._last_heartbeat >= HEARTBEAT_SECONDS
                )
                if changed or heartbeat_due:
                    await self.publish_runtime(
                        status,
                        detail,
                        busy=self._delivery_active,
                        active_event=self._active_event_id,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log(
                    "runtime_monitor_failed",
                    error_type=type(exc).__name__,
                    error=str(exc)[:300],
                )
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=OBSERVATION_SECONDS
                )
            except TimeoutError:
                pass

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
        if self._monitor_task is not None:
            self._monitor_task.cancel()
            await asyncio.gather(self._monitor_task, return_exceptions=True)
            self._monitor_task = None
        self.write_run_state(False, stopping=True)
        closed_clients: set[int] = set()
        for binding in (self.binding, self._obsolete_binding):
            if binding is None or id(binding.client) in closed_clients:
                continue
            closed_clients.add(id(binding.client))
            with contextlib.suppress(BaseException):
                await self.close_binding(binding)
        await self.stop_tui()
        await self.stop_pager_broker()
        await self.terminate_child(self.leader, "leader")
        remove_owned_socket(
            Path(self.config.socket_path),
            self._leader_socket_identity,
        )
        self._leader_socket_identity = None
        retained_pager_paths = cleanup_pager_status(self.pager_status)
        if retained_pager_paths:
            self.log("pager_status_cleanup_retained", paths=retained_pager_paths)
        if self._hcom_registered:
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
        started_ns = time.time_ns()
        self.started_ns = started_ns
        try:
            launch_home = self.config.launch_home or Path(
                os.environ.get("HOME", str(Path.home()))
            )
            self.pager_status = prepare_pager_status(
                state_root=self.config.state_root,
                launch_home=launch_home,
                grok_home=self.config.grok_home,
                isolated_home=self.config.isolated_home,
            )
            await self.recover_stale_runtime()
            pager_recovery_error = recover_pager_config(self.pager_status)
            if pager_recovery_error is not None:
                raise RuntimeError(
                    f"pager config recovery failed: {pager_recovery_error}"
                )
            await self.start_pager_broker()
            self.write_run_state(False, starting=True, startup_phase="pager-ready")
            self.cursor = self.load_cursor()
            self.session_id, resume = self.load_session()
            self.write_run_state(False, starting=True)
            # Both the leader and pager consume the user configuration at
            # startup. Publish the isolated seat's owned block before either
            # Grok process can cache a status-line-disabled snapshot.
            self._pager_launch_gate_ns = time.monotonic_ns()
            pager_stage_error = stage_pager_status(self.pager_status)
            await self.spawn_leader()
            self.write_run_state(False, starting=True, startup_phase="leader-ready")
            if resume:
                await self.spawn_tui(True)
            else:
                binding = await self.create_sidecar()
                self.binding = binding
                self.adopt_created_session(binding.session_id)
                self.visible_session_id = binding.session_id
                self._binding_transition = None
                self.write_run_state(
                    False, starting=True, startup_phase="creator-ready"
                )
                # The one visible pager is deliberately an attachment.  Grok
                # 1.0.13 arms its status-line runner on this path, whereas the
                # direct new-pager construction path never emits a sample.
                self._pager_launch_gate_ns = time.monotonic_ns()
                await self.spawn_tui(True)
            self.write_run_state(False, starting=True, startup_phase="tui-ready")
            await self.wait_session()
            if resume:
                binding = await self.connect_sidecar(self.session_id)
                self.binding = binding
                self.visible_session_id = self.session_id
            else:
                binding = self.binding
                if binding is None:
                    raise RuntimeError("ACP launch-session creator binding is absent")
            self.bridge_state = "BOUND"
            self.write_run_state(False, starting=True, startup_phase="sidecar-ready")
            if pager_stage_error is None:
                observed = await self.observe_fresh_binding(
                    self._pager_launch_gate_ns
                )
            else:
                observed = VisibleSessionObservation(
                    "unsafe",
                    f"pager status bootstrap unavailable: {pager_stage_error}",
                )
            self._record_observation(observed)
            if observed.kind == "transient-missing":
                self._enter_degraded(observed.reason, recoverable=True)
            self.observe_registry_diagnostic()
            if self._orphan_report is not None:
                self._enter_degraded(self._orphan_report, recoverable=True)
            await self.register_hcom()
            await self.retry_pending_reply()
            self.write_run_state(True)
            self._monitor_task = asyncio.create_task(
                self.runtime_monitor(), name="hcom-grok-runtime-monitor"
            )
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
                if not self._delivery_active:
                    await self.maintain_binding()
                outcome = await self.process_mail()
                await asyncio.sleep(0 if outcome == "progressed" else POLL_SECONDS)
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
