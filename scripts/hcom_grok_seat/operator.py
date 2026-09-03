#!/usr/bin/env python3
"""Operator CLI for the local HCOM Grok seat.

The operator owns only lifecycle controls and filesystem installation state. The
supervisor owns the Grok processes and writes the authoritative ``run.json``.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import quote

from .envelope import HcomReader
from .registry import SeatRegistry, normalize_seat


SUPERVISOR_MODULE = "scripts.hcom_grok_seat.supervisor"
PROCESS_MARKER_PREFIX = "hcom-grok:"
HCOM_NAME_MARKER = re.compile(r"(?m)^\[hcom:([A-Za-z0-9][A-Za-z0-9_.-]{0,79})\]\s*$")
PINNED_ENV = {
    "HCOM_GROK_STATE_ROOT",
    "HCOM_GROK_SEAT",
    "HCOM_GROK_CURSOR",
    "HCOM_GROK_HCOM_DB",
}
RUN_REQUIRED = {
    "supervisor_pid",
    "tui_pid",
    "run_token",
    "socket_path",
    "session_id",
    "project",
    "seat",
    "background_tui",
    "ready",
    "started_ns",
    "release",
    "supervisor_executable",
    "argv_marker",
}


def _path_env(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default))).expanduser().resolve()


@dataclass(frozen=True)
class Config:
    state_root: Path
    log_root: Path
    release_root: Path
    bin_root: Path
    hcom_db: Path
    cursor_path: Path
    project: Path
    seat: str
    grok_bin: str
    hcom_bin: str

    @property
    def current_state(self) -> Path:
        return self.state_root

    @property
    def run_path(self) -> Path:
        return self.current_state / "run.json"

    @property
    def session_path(self) -> Path:
        return self.current_state / "session.json"


def load_config(*, fresh_project: bool = False) -> Config:
    home = Path.home()
    state_root = _path_env(
        "HCOM_GROK_STATE_ROOT", home / ".local/state/hcom-grok/current"
    )
    persisted_session: dict[str, Any] = {}
    persisted_run: dict[str, Any] = {}
    for path, target in (
        (state_root / "session.json", persisted_session),
        (state_root / "run.json", persisted_run),
    ):
        try:
            value = json.loads(path.read_text())
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            target.update(value)
    if fresh_project:
        project_default = str(Path.cwd())
    else:
        project_default = persisted_session.get("project")
        if not isinstance(project_default, str) or not project_default:
            project_default = persisted_run.get("project")
        if not isinstance(project_default, str) or not project_default:
            project_default = str(Path.cwd())
    seat_default = persisted_run.get("seat")
    if not isinstance(seat_default, str) or not seat_default:
        seat_default = "gsea"
    return Config(
        state_root=state_root,
        log_root=_path_env(
            "HCOM_GROK_LOG_DIR",
            _path_env("HCOM_GROK_LOG_ROOT", home / "Library/Logs/hcom-grok"),
        ),
        release_root=_path_env("HCOM_GROK_RELEASE_ROOT", home / ".local/lib/hcom-grok"),
        bin_root=_path_env("HCOM_GROK_BIN_ROOT", home / ".local/bin"),
        hcom_db=_path_env(
            "HCOM_GROK_HCOM_DB",
            _path_env("HCOM_DIR", home / ".hcom") / "hcom.db",
        ),
        cursor_path=_path_env("HCOM_GROK_CURSOR", state_root / "cursor.json"),
        project=_path_env("HCOM_GROK_PROJECT", Path(project_default)),
        seat=os.environ.get("HCOM_GROK_SEAT", seat_default),
        grok_bin=os.environ.get("GROK_BIN", "grok"),
        hcom_bin=os.environ.get("HCOM_BIN", "hcom"),
    )


def managed_mode() -> bool:
    """Use dynamic seats unless the caller explicitly pins legacy state."""
    return not any(os.environ.get(name) for name in PINNED_ENV)


def current_hcom_db() -> Path:
    home = Path.home()
    return _path_env(
        "HCOM_GROK_HCOM_DB",
        _path_env("HCOM_DIR", home / ".hcom") / "hcom.db",
    )


def managed_config(
    registry: SeatRegistry,
    record: dict[str, Any],
    *,
    fresh_project: bool = False,
) -> Config:
    state_root = Path(str(record["state_root"])).expanduser().resolve()
    log_root = Path(str(record["log_root"])).expanduser().resolve()
    persisted_session = _read_json(state_root / "session.json") or {}
    persisted_run = _read_json(state_root / "run.json") or {}
    if fresh_project:
        project_default = str(record["project"])
    else:
        project_default = persisted_session.get("project")
        if not isinstance(project_default, str) or not project_default:
            project_default = persisted_run.get("project")
        if not isinstance(project_default, str) or not project_default:
            project_default = str(record["project"])
    home = Path.home()
    return Config(
        state_root=state_root,
        log_root=log_root,
        release_root=_path_env("HCOM_GROK_RELEASE_ROOT", home / ".local/lib/hcom-grok"),
        bin_root=_path_env("HCOM_GROK_BIN_ROOT", home / ".local/bin"),
        hcom_db=registry.hcom_db,
        cursor_path=state_root / "cursor.json",
        project=Path(project_default).expanduser().resolve(),
        seat=normalize_seat(str(record["name"])),
        grok_bin=os.environ.get("GROK_BIN", "grok"),
        hcom_bin=os.environ.get("HCOM_BIN", "hcom"),
    )


def _isolated_hcom_env(hcom_db: Path) -> dict[str, str]:
    env = os.environ.copy()
    for key in tuple(env):
        if key.startswith("HCOM_"):
            env.pop(key, None)
        if key.startswith(
            ("ANTIGRAVITY_", "CLAUDE_", "CODEX_", "CURSOR_", "GEMINI_", "KIMI_")
        ):
            env.pop(key, None)
        if key in {"CLAUDECODE", "GEMINI_CLI", "OPENCODE", "KILO"}:
            env.pop(key, None)
    env["HCOM_DIR"] = str(hcom_db.parent)
    return env


def allocate_hcom_seat(hcom_db: Path, hcom_bin: str, project: Path) -> str:
    result = subprocess.run(
        [hcom_bin, "start"],
        cwd=str(project),
        env=_isolated_hcom_env(hcom_db),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[:500]
        raise RuntimeError(f"HCOM seat allocation failed: {detail}")
    names = set(HCOM_NAME_MARKER.findall(result.stdout))
    if len(names) != 1:
        raise RuntimeError("HCOM seat allocation returned no unique [hcom:name] marker")
    return normalize_seat(names.pop())


def release_hcom_seat(hcom_db: Path, hcom_bin: str, seat: str) -> None:
    try:
        subprocess.run(
            [hcom_bin, "stop", normalize_seat(seat)],
            cwd=str(hcom_db.parent),
            env=_isolated_hcom_env(hcom_db),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError, ValueError):
        return


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def load_run_state(config: Config) -> dict[str, Any] | None:
    return _read_json(config.run_path)


def validate_run_state(state: dict[str, Any] | None) -> list[str]:
    if state is None:
        return ["run state is absent or unreadable"]
    problems = [f"missing {key}" for key in sorted(RUN_REQUIRED - set(state))]
    if "supervisor_pid" in state and type(state["supervisor_pid"]) is not int:
        problems.append("supervisor_pid is not an integer")
    if "tui_pid" in state and not (
        type(state["tui_pid"]) is int or (state.get("ready") is False and state["tui_pid"] is None)
    ):
        problems.append("tui_pid is not an integer")
    if "started_ns" in state and type(state["started_ns"]) is not int:
        problems.append("started_ns is not an integer")
    for key in (
        "socket_path",
        "session_id",
        "project",
        "seat",
        "run_token",
        "release",
        "supervisor_executable",
        "argv_marker",
    ):
        if key in state and not isinstance(state[key], str):
            problems.append(f"{key} is not a string")
    if "launch_mode" in state and state["launch_mode"] not in {"new", "resumed"}:
        problems.append("launch_mode is not new or resumed")
    for key in ("background_tui", "ready", "busy"):
        if key in state and type(state[key]) is not bool:
            problems.append(f"{key} is not a boolean")
    return problems


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def process_argv(pid: int) -> list[str] | None:
    """Return a live process argv without trusting the recorded run state."""
    proc_cmdline = Path(f"/proc/{pid}/cmdline")
    try:
        raw = proc_cmdline.read_bytes()
    except OSError:
        raw = b""
    if raw:
        return [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
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


def verify_process_owner(state: dict[str, Any]) -> tuple[bool, str]:
    """Require an executable and unique argv marker before any signal."""
    try:
        pid = int(state["supervisor_pid"])
    except (KeyError, TypeError, ValueError):
        return False, "run state has no valid supervisor PID"
    expected_executable = state.get("supervisor_executable")
    marker = state.get("argv_marker")
    if not isinstance(expected_executable, str) or not expected_executable:
        return False, "run state has no supervisor_executable ownership record"
    if not isinstance(marker, str) or not marker.startswith(PROCESS_MARKER_PREFIX):
        return False, "run state has no valid argv_marker ownership record"
    argv = process_argv(pid)
    if not argv:
        return False, "cannot read the supervisor process argv"
    try:
        actual_executable = str(Path(argv[0]).expanduser().resolve())
        recorded_executable = str(Path(expected_executable).expanduser().resolve())
    except (OSError, RuntimeError):
        return False, "cannot resolve the supervisor executable"
    if actual_executable != recorded_executable:
        return False, "live PID executable does not match run state"
    if marker not in argv:
        return False, "live PID argv marker does not match run state"
    try:
        module_index = argv.index(SUPERVISOR_MODULE)
    except ValueError:
        return False, "live PID is not the HCOM Grok supervisor module"
    if module_index < 1 or argv[module_index - 1] != "-m":
        return False, "live PID did not start the supervisor as a Python module"
    if "run" not in argv[module_index + 1 :]:
        return False, "live PID is not running the supervisor run command"
    try:
        marker_flag = argv.index("--operator-marker", module_index + 1)
    except ValueError:
        return False, "live PID has no operator marker flag"
    if marker_flag + 1 >= len(argv) or argv[marker_flag + 1] != marker:
        return False, "live PID operator marker argument does not match run state"
    return True, "ownership verified"


def _current_release(config: Config) -> str:
    current = config.release_root / "current"
    try:
        return str(current.resolve(strict=True))
    except OSError:
        module = Path(__file__).resolve()
        return str(module.parents[2]) if len(module.parents) > 2 else "development"


def supervisor_command(config: Config, marker: str, background_tui: bool) -> list[str]:
    command = [
        sys.executable,
        "-m",
        SUPERVISOR_MODULE,
        "run",
        "--operator-marker",
        marker,
    ]
    if background_tui:
        command.append("--background-tui")
    return command


def _grok_session_directory(project: Path, session_id: str) -> Path:
    encoded = quote(str(project), safe="")
    return Path.home() / ".grok" / "sessions" / encoded / session_id


def require_resumable_session(config: Config) -> dict[str, Any]:
    saved = _read_json(config.session_path)
    if saved is None:
        raise RuntimeError("No resumable Grok session. Run hcom-grok to start fresh.")
    session_id = saved.get("session_id")
    project = saved.get("project")
    if not isinstance(session_id, str) or not session_id:
        raise RuntimeError("No resumable Grok session. Run hcom-grok to start fresh.")
    if not isinstance(project, str) or not project:
        raise RuntimeError("Saved Grok session has no project. Run hcom-grok to start fresh.")
    saved_project = Path(project).expanduser().resolve()
    configured_project = config.project.expanduser().resolve()
    if saved_project != configured_project:
        raise RuntimeError(
            f"Saved Grok session belongs to {saved_project}, not {configured_project}. "
            "Run hcom-grok to start fresh."
        )
    if not saved_project.is_dir():
        raise RuntimeError(f"Saved Grok project no longer exists: {saved_project}")
    session_dir = _grok_session_directory(saved_project, session_id)
    if not session_dir.is_dir():
        raise RuntimeError(
            f"Saved Grok session is no longer available: {session_id}. "
            "Run hcom-grok to start fresh."
        )
    return saved


def start(
    config: Config,
    background: bool = False,
    *,
    session_mode: str = "resume",
) -> dict[str, Any]:
    if session_mode not in {"new", "resume"}:
        raise ValueError(f"unsupported Grok session mode: {session_mode}")
    _private_dir(config.state_root)
    _private_dir(config.log_root)
    state = load_run_state(config)
    if state:
        try:
            recorded_pid = int(state.get("supervisor_pid", 0))
        except (TypeError, ValueError):
            recorded_pid = 0
        if recorded_pid > 0 and pid_alive(recorded_pid):
            problems = validate_run_state(state)
            if problems:
                raise RuntimeError(
                    f"refusing to start over live PID {recorded_pid} with invalid run state: "
                    + "; ".join(problems)
                )
            owned, reason = verify_process_owner(state)
            if not owned:
                raise RuntimeError(f"refusing to replace live unowned PID {recorded_pid}: {reason}")
            if session_mode == "new":
                raise RuntimeError(
                    f"Grok is already running as session {state.get('session_id')}. "
                    "Run hcom-grok stop before starting fresh."
                )
            return {"ok": True, "running": True, "already_running": True, **state}

    if session_mode == "resume":
        require_resumable_session(config)

    marker = PROCESS_MARKER_PREFIX + secrets.token_hex(16)
    command = supervisor_command(config, marker, background_tui=background)
    env = os.environ.copy()
    package_root = str(Path(__file__).resolve().parents[2])
    inherited_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        package_root + os.pathsep + inherited_pythonpath
        if inherited_pythonpath
        else package_root
    )
    env.update(
        {
            "HCOM_GROK_STATE_ROOT": str(config.current_state),
            "HCOM_GROK_LOG_DIR": str(config.log_root),
            "HCOM_GROK_RELEASE_ROOT": str(config.release_root),
            "HCOM_GROK_RELEASE": _current_release(config),
            "HCOM_GROK_PROJECT": str(config.project),
            "HCOM_GROK_SESSION_MODE": session_mode,
            "HCOM_GROK_SEAT": config.seat,
            "HCOM_DIR": str(config.hcom_db.parent),
            "GROK_BIN": config.grok_bin,
            "HCOM_BIN": config.hcom_bin,
        }
    )
    if not background:
        result = subprocess.run(command, cwd=str(config.project), env=env, check=False)
        return {"ok": result.returncode == 0, "returncode": result.returncode}

    launch_log = config.log_root / "operator-launch.log"
    old_umask = os.umask(0o077)
    try:
        stream = launch_log.open("ab", buffering=0)
    finally:
        os.umask(old_umask)
    try:
        process = subprocess.Popen(
            command,
            cwd=str(config.project),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    finally:
        stream.close()
    deadline = time.monotonic() + float(os.environ.get("HCOM_GROK_START_TIMEOUT", "60"))
    while time.monotonic() < deadline:
        state = load_run_state(config)
        if (
            state
            and not validate_run_state(state)
            and int(state["supervisor_pid"]) == process.pid
            and state.get("ready") is True
        ):
            return {"ok": True, "running": True, "started": True, **state}
        returncode = process.poll()
        if returncode is not None:
            raise RuntimeError(f"supervisor exited during start with status {returncode}")
        time.sleep(0.05)
    process.terminate()
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)
    raise RuntimeError(f"supervisor did not publish {config.run_path} within startup timeout")


def status(config: Config) -> dict[str, Any]:
    state = load_run_state(config)
    problems = validate_run_state(state)
    if problems:
        return {"running": False, "state_path": str(config.run_path), "problems": problems}
    assert state is not None
    pid = int(state["supervisor_pid"])
    alive = pid_alive(pid)
    owned, ownership = verify_process_owner(state) if alive else (False, "PID is not alive")
    result: dict[str, Any] = {
        "running": alive and owned,
        "pid_alive": alive,
        "ownership": ownership,
        **state,
    }
    return result


def stop(config: Config, timeout: float = 12.0) -> dict[str, Any]:
    state = load_run_state(config)
    problems = validate_run_state(state)
    if problems:
        if isinstance(state, dict):
            try:
                recorded_pid = int(state.get("supervisor_pid", 0))
            except (TypeError, ValueError):
                recorded_pid = 0
            if recorded_pid > 0 and pid_alive(recorded_pid):
                raise RuntimeError(
                    f"refusing to signal live PID {recorded_pid} with invalid run state: "
                    + "; ".join(problems)
                )
        return {"ok": True, "already_stopped": True, "problems": problems}
    assert state is not None
    pid = int(state["supervisor_pid"])
    if not pid_alive(pid):
        return {"ok": True, "already_stopped": True, "pid": pid}
    owned, reason = verify_process_owner(state)
    if not owned:
        raise RuntimeError(f"refusing to signal PID {pid}: {reason}")

    owned, reason = verify_process_owner(state)
    if not owned:
        raise RuntimeError(f"refusing final ownership check for PID {pid}: {reason}")
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.05)
    if pid_alive(pid):
        raise RuntimeError(f"supervisor PID {pid} did not stop after SIGTERM")
    return {"ok": True, "stopped": True, "pid": pid}


def restart(config: Config, background: bool = False) -> dict[str, Any]:
    require_resumable_session(config)
    stopped = stop(config)
    started = start(config, background=background, session_mode="resume")
    return {"ok": bool(started.get("ok")), "stop": stopped, "start": started}


def _db_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    uri = f"file:{path.as_posix()}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=2)
        row = connection.execute(
            "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM events"
        ).fetchone()
    except sqlite3.Error as exc:
        return {"exists": True, "path": str(path), "error": str(exc)}
    finally:
        if connection is not None:
            connection.close()
    return {
        "exists": True,
        "path": str(path),
        "event_count": int(row[0]),
        "max_event_id": int(row[1]),
    }


def _read_cursor(path: Path) -> dict[str, Any] | None:
    return _read_json(path)


def _atomic_cursor(path: Path, value: dict[str, Any]) -> None:
    _private_dir(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    old_umask = os.umask(0o077)
    try:
        with temporary.open("w") as stream:
            stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        os.umask(old_umask)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def clear_pending(config: Config) -> dict[str, Any]:
    state = load_run_state(config)
    if state:
        try:
            recorded_pid = int(state.get("supervisor_pid", 0))
        except (TypeError, ValueError):
            recorded_pid = 0
        if recorded_pid > 0 and pid_alive(recorded_pid):
            problems = validate_run_state(state)
            if problems:
                raise RuntimeError(
                    f"refusing clear-pending with live PID {recorded_pid} and invalid run state: "
                    + "; ".join(problems)
                )
            owned, reason = verify_process_owner(state)
            if not owned:
                raise RuntimeError(f"refusing live clear-pending: {reason}")
            raise RuntimeError(
                "clear-pending requires the supervisor to be stopped so its in-memory cursor cannot overwrite the change"
            )

    before = _db_summary(config.hcom_db)
    if not before.get("exists") or "error" in before:
        raise RuntimeError(str(before.get("error") or f"HCOM DB not found: {config.hcom_db}"))
    max_event_id = int(before["max_event_id"])
    previous_state = _read_cursor(config.cursor_path)
    previous = previous_state.get("last_event_id") if previous_state else None
    if previous is not None and type(previous) is not int:
        raise RuntimeError("cursor last_event_id is not an integer")
    if isinstance(previous, int) and previous > max_event_id:
        raise RuntimeError(
            f"cursor {previous} is ahead of HCOM MAX(id) {max_event_id}; refusing to move backward"
        )
    db_stat = config.hcom_db.stat()
    digest = HcomReader(config.hcom_db).event_digest(max_event_id) if max_event_id else None
    if max_event_id and digest is None:
        raise RuntimeError(f"HCOM MAX(id) {max_event_id} disappeared during clear-pending")
    cursor_state = {
        "schema": 1,
        "db_path": str(config.hcom_db),
        "db_device": db_stat.st_dev,
        "db_inode": db_stat.st_ino,
        "last_event_id": max_event_id,
        "last_event_sha256": digest,
        "pending_reply": None,
        "updated_ns": time.time_ns(),
    }
    _atomic_cursor(config.cursor_path, cursor_state)
    after = _db_summary(config.hcom_db)
    if before.get("event_count") != after.get("event_count") or before.get("max_event_id") != after.get("max_event_id"):
        raise RuntimeError("HCOM events changed while clear-pending ran; history was not modified by this command")
    return {
        "ok": True,
        "cursor_path": str(config.cursor_path),
        "previous_cursor": previous,
        "cursor": max_event_id,
        "hcom_event_count": after["event_count"],
        "history_deleted": False,
    }


def doctor(config: Config) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail})

    add("project", config.project.is_dir(), str(config.project))
    grok = shutil.which(config.grok_bin)
    hcom = shutil.which(config.hcom_bin)
    add("grok", grok is not None, grok or f"not found: {config.grok_bin}")
    add("hcom", hcom is not None, hcom or f"not found: {config.hcom_bin}")
    db = _db_summary(config.hcom_db)
    add("hcom_db", db.get("exists") is True and "error" not in db, json.dumps(db, sort_keys=True))
    for label, path in (("state_root", config.state_root), ("log_root", config.log_root)):
        try:
            exists = path.is_dir()
            mode = path.stat().st_mode & 0o777 if exists else None
            private = exists and mode is not None and mode & 0o077 == 0
            detail = f"{path} mode={oct(mode)}" if mode is not None else f"missing: {path}"
            add(label, private, detail)
        except OSError as exc:
            add(label, False, str(exc))
    current = config.release_root / "current"
    add("installed_release", current.is_symlink() and current.exists(), str(current))
    return {"ok": all(check["ok"] for check in checks), "checks": checks, "status": status(config)}


def inspect(config: Config) -> dict[str, Any]:
    result: dict[str, Any] = {
        "config": {key: str(value) if isinstance(value, Path) else value for key, value in asdict(config).items()},
        "run_state": load_run_state(config),
        "status": status(config),
        "cursor": _read_cursor(config.cursor_path),
        "hcom_db": _db_summary(config.hcom_db),
        "current_release": str(config.release_root / "current"),
        "previous_release": str(config.release_root / "previous"),
        "log_root": str(config.log_root),
    }
    return result


def show_logs(config: Config, follow: bool = False, lines: int = 100) -> int:
    candidates = [config.log_root / "bridge.jsonl", config.log_root / "operator-launch.log"]
    path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
    if not path.exists():
        raise RuntimeError(f"no log file found under {config.log_root}")
    if follow:
        return subprocess.run(["tail", "-n", str(lines), "-F", str(path)], check=False).returncode
    content = path.read_text(errors="replace").splitlines()
    for line in content[-lines:]:
        print(line)
    return 0


def _print_result(value: dict[str, Any], json_output: bool) -> None:
    if json_output:
        print(json.dumps(value, indent=2, sort_keys=True, default=str))
        return
    if value.get("running") is True:
        lifecycle = "busy" if value.get("busy") is True else (
            "running" if value.get("ready") is True else "starting"
        )
        mode = str(value.get("launch_mode", "unknown")).upper()
        print(
            f"{lifecycle}: mode={mode} seat={value.get('seat')} "
            f"pid={value.get('supervisor_pid')} session={value.get('session_id')} "
            f"project={value.get('project')}"
        )
        return
    if value.get("running") is False:
        print("stopped")
        for problem in value.get("problems", []):
            print(f"  {problem}")
        return
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def seat_inventory(registry: SeatRegistry) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for record in registry.list():
        config = managed_config(registry, record)
        state = status(config)
        inventory.append(
            {
                "name": record["name"],
                "latest": bool(record.get("latest")),
                "legacy": bool(record.get("legacy")),
                "running": bool(state.get("running")),
                "ready": bool(state.get("ready")),
                "busy": bool(state.get("busy")),
                "launch_mode": state.get("launch_mode"),
                "session_id": state.get("session_id"),
                "project": state.get("project", record.get("project")),
                "supervisor_pid": state.get("supervisor_pid"),
                "state_root": record["state_root"],
                "log_root": record["log_root"],
            }
        )
    return inventory


def _print_seat_inventory(
    seats: list[dict[str, Any]], hcom_db: Path, json_output: bool
) -> None:
    if json_output:
        print(
            json.dumps(
                {"ok": True, "hcom_db": str(hcom_db), "seats": seats},
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        return
    if not seats:
        print("No HCOM Grok seats in this room")
        return
    for item in seats:
        marker = "*" if item["latest"] else " "
        lifecycle = "busy" if item["busy"] else "running" if item["running"] else "stopped"
        mode = str(item.get("launch_mode") or "unknown").upper()
        print(
            f"{marker} {item['name']} {lifecycle} mode={mode} "
            f"session={item.get('session_id') or '-'} project={item.get('project') or '-'}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hcom-grok",
        description="Start a fresh local HCOM Grok seat or operate its saved conversation",
        epilog="Bare hcom-grok starts fresh. Use hcom-grok -c to continue.",
    )
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    parser.add_argument(
        "-c",
        "--continue",
        dest="continue_seat",
        nargs="?",
        const="",
        metavar="SEAT",
        help="continue the latest or named Grok conversation instead of starting fresh",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="launch without attaching the visible TUI",
    )
    sub = parser.add_subparsers(dest="command")
    resume_parser = sub.add_parser("resume", help="continue the latest or named Grok conversation")
    resume_parser.add_argument("seat", nargs="?")
    resume_parser.add_argument(
        "--background",
        action="store_true",
        default=argparse.SUPPRESS,
        help="resume without attaching the visible TUI",
    )
    sub.add_parser("list", help="list adapter-managed Grok seats in this HCOM room")
    status_parser = sub.add_parser("status", help="show supervisor and seat status")
    status_parser.add_argument("seat", nargs="?")
    doctor_parser = sub.add_parser("doctor", help="check binaries, state, database, and install")
    doctor_parser.add_argument("seat", nargs="?")
    logs_parser = sub.add_parser("logs", help="show supervisor logs")
    logs_parser.add_argument("seat", nargs="?")
    logs_parser.add_argument("--follow", action="store_true")
    logs_parser.add_argument("--lines", type=int, default=100)
    stop_parser = sub.add_parser("stop", help="cleanly stop the owned supervisor")
    stop_parser.add_argument("seat", nargs="?")
    stop_parser.add_argument("--timeout", type=float, default=12.0)
    restart_parser = sub.add_parser("restart", help="stop and start the seat")
    restart_parser.add_argument("seat", nargs="?")
    restart_parser.add_argument("--background", action="store_true", default=argparse.SUPPRESS)
    inspect_parser = sub.add_parser("inspect", help="show detailed local and supervisor state")
    inspect_parser.add_argument("seat", nargs="?")
    clear_parser = sub.add_parser("clear-pending", help="advance the cursor without deleting HCOM history")
    clear_parser.add_argument("seat", nargs="?")
    sub.add_parser("rollback", help="switch to the previous installed release")
    install_parser = sub.add_parser("install", help="install a versioned local release")
    install_parser.add_argument("--version")
    return parser


def normalize_argv(argv: Sequence[str]) -> tuple[list[str], bool]:
    """Retain the old ``start`` spelling without advertising it."""
    normalized = list(argv)
    for index, token in enumerate(normalized):
        if token == "start":
            normalized[index] = "resume"
            return normalized, True
        if not token.startswith("-"):
            break
    return normalized, False


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    normalized_argv, deprecated_start = normalize_argv(raw_argv)
    parser = build_parser()
    args = parser.parse_args(normalized_argv)
    if args.continue_seat is not None and args.command is not None:
        parser.error("-c/--continue cannot be combined with a command")
    if deprecated_start:
        print(
            "hcom-grok: 'start' is deprecated and continues the saved conversation; "
            "use 'hcom-grok' for fresh or 'hcom-grok -c' to continue",
            file=sys.stderr,
        )
    launch_mode = "resume" if args.continue_seat is not None or args.command == "resume" else "new"
    selected_seat = (
        (args.continue_seat or None)
        if args.continue_seat is not None
        else getattr(args, "seat", None)
    )
    registry: SeatRegistry | None = None
    allocated_seat: str | None = None
    try:
        if args.command in {"install", "rollback"}:
            config = load_config()
        elif managed_mode():
            registry = SeatRegistry(current_hcom_db())
            if args.command == "list":
                seats = seat_inventory(registry)
                _print_seat_inventory(seats, registry.hcom_db, args.json)
                return 0
            if args.command is None and launch_mode == "new":
                project = _path_env("HCOM_GROK_PROJECT", Path.cwd())
                hcom_bin = os.environ.get("HCOM_BIN", "hcom")
                allocated_seat = allocate_hcom_seat(registry.hcom_db, hcom_bin, project)
                try:
                    record = registry.register(allocated_seat, project)
                except BaseException:
                    release_hcom_seat(registry.hcom_db, hcom_bin, allocated_seat)
                    raise
                config = managed_config(registry, record, fresh_project=True)
            else:
                if launch_mode == "resume" or args.command == "restart":
                    record = registry.get_resumable(selected_seat)
                else:
                    record = registry.get(selected_seat)
                config = managed_config(registry, record)
                if launch_mode == "resume" or args.command == "restart":
                    registry.touch(config.seat, project=config.project)
        else:
            config = load_config(fresh_project=args.command is None and launch_mode == "new")
            if selected_seat is not None and normalize_seat(selected_seat) != config.seat:
                raise RuntimeError(
                    f"fixed-seat mode owns {config.seat}, not {normalize_seat(selected_seat)}"
                )
            if args.command == "list":
                state = status(config)
                _print_seat_inventory(
                    [
                        {
                            "name": config.seat,
                            "latest": True,
                            "legacy": True,
                            "running": bool(state.get("running")),
                            "ready": bool(state.get("ready")),
                            "busy": bool(state.get("busy")),
                            "launch_mode": state.get("launch_mode"),
                            "session_id": state.get("session_id"),
                            "project": state.get("project", str(config.project)),
                            "supervisor_pid": state.get("supervisor_pid"),
                            "state_root": str(config.state_root),
                            "log_root": str(config.log_root),
                        }
                    ],
                    config.hcom_db,
                    args.json,
                )
                return 0

        if args.command is None or args.command == "resume":
            result = start(
                config,
                background=args.background,
                session_mode=launch_mode,
            )
            if allocated_seat is not None and not result.get("ok", True) and registry is not None:
                registry.remove(allocated_seat)
                release_hcom_seat(registry.hcom_db, config.hcom_bin, allocated_seat)
                allocated_seat = None
        elif args.command == "status":
            result = status(config)
            _print_result(result, args.json)
            return 0 if result.get("running") else 3
        elif args.command == "doctor":
            result = doctor(config)
            _print_result(result, args.json)
            return 0 if result["ok"] else 4
        elif args.command == "logs":
            return show_logs(config, follow=args.follow, lines=max(1, args.lines))
        elif args.command == "stop":
            result = stop(config, timeout=max(0.1, args.timeout))
        elif args.command == "restart":
            result = restart(config, background=args.background)
        elif args.command == "inspect":
            result = inspect(config)
        elif args.command == "clear-pending":
            result = clear_pending(config)
        elif args.command in {"install", "rollback"}:
            try:
                from . import install as installer
            except ImportError:
                import install as installer  # type: ignore[no-redef]
            if args.command == "install":
                result = installer.install_release(
                    package_dir=Path(__file__).resolve().parent,
                    release_root=config.release_root,
                    bin_root=config.bin_root,
                    version=args.version,
                )
            else:
                result = installer.rollback_release(config.release_root)
        else:
            raise RuntimeError(f"unsupported command: {args.command}")
    except (OSError, RuntimeError, sqlite3.Error, subprocess.SubprocessError, ValueError) as exc:
        if allocated_seat is not None and registry is not None:
            registry.remove(allocated_seat)
            release_hcom_seat(registry.hcom_db, os.environ.get("HCOM_BIN", "hcom"), allocated_seat)
        if args.json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2), file=sys.stderr)
        else:
            print(f"hcom-grok: {exc}", file=sys.stderr)
        return 2
    _print_result(result, args.json)
    return 0 if result.get("ok", True) else 2


if __name__ == "__main__":
    raise SystemExit(main())
