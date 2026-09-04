"""Read-only observation of the Grok TUI's visible conversation.

Structured metadata only: the pager's status-command record, the legacy
``active_sessions.json`` registry, and allowlisted ``summary.json`` identity
fields under a configured ``GROK_HOME``. No slash-command provenance, no
title/recap/content, no newest-directory wins.
"""

from __future__ import annotations

import errno
import json
import os
import re
import stat
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

from .acp_session import TESTED_GROK_VERSIONS
from .pager_status import PagerStatusSetup, read_pager_status

ObservationKind = Literal["aligned", "visible-change", "transient-missing", "unsafe"]

ACTIVE_SESSIONS_NAME = "active_sessions.json"
ACTIVE_SESSIONS_LOCK_NAME = "active_sessions.lock"
REQUIRED_ROW_KEYS = ("pid", "cwd", "session_id", "opened_at")
SUMMARY_IDENTITY_KEYS = ("chat_format_version", "created_at", "info", "grok_home")
PINNED_CHAT_FORMAT_VERSION = 1
MAX_METADATA_BYTES = 1_048_576
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


@dataclass(frozen=True)
class VisibleSessionObservation:
    """Immutable result of one visible-session observation."""

    kind: ObservationKind
    reason: str
    session_id: str | None = None
    pid: int | None = None
    cwd: str | None = None
    opened_at: str | None = None
    created_at: str | None = None
    grok_home: str | None = None
    git_root_dir: str | None = None
    session_directory: str | None = None
    focus_source: str | None = None
    status_trigger: str | None = None
    status_sample_monotonic_ns: int | None = None
    focus_state_monotonic_ns: int | None = None


PidAliveFn = Callable[[int], bool]
FileIdentity = tuple[int, int, int, int]


def canonical_path(value: str | Path) -> Path:
    """Normalize path equality, including trailing-separator variants."""
    return Path(value).expanduser().resolve()


def session_directory_for(grok_home: Path, project: Path, session_id: str) -> Path:
    """Return ``<GROK_HOME>/sessions/<urlencoded-project>/<session_id>``."""
    home = canonical_path(grok_home)
    project_key = quote(str(canonical_path(project)), safe="")
    return home / "sessions" / project_key / session_id


def _pid_alive(pid: int) -> bool:
    if type(pid) is not int or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _parse_rfc3339(value: object) -> str | None:
    """Require timezone-bearing RFC3339 (``Z`` or numeric offset)."""
    if not isinstance(value, str) or not value:
        return None
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return value


def _is_uuid(value: object) -> bool:
    if not isinstance(value, str) or not _UUID_RE.fullmatch(value):
        return False
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def _path_contained(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
    except ValueError:
        return False
    return True


def _lstat_or_none(path: Path) -> os.stat_result | None:
    try:
        return path.lstat()
    except OSError:
        return None


def _reject_symlink(path: Path, *, label: str) -> str | None:
    st = _lstat_or_none(path)
    if st is None:
        return None
    if stat.S_ISLNK(st.st_mode):
        return f"{label} must not be a symlink"
    return None


def _read_regular_file(path: Path, *, label: str) -> tuple[bytes, FileIdentity] | str:
    """Bounded O_NOFOLLOW regular-file read. Never follows or creates paths."""
    link_error = _reject_symlink(path, label=label)
    if link_error is not None:
        return link_error
    flags = os.O_RDONLY
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        flags |= nofollow
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return "missing"
    except OSError as exc:
        if exc.errno in {errno.ELOOP, getattr(errno, "EMLINK", -1)}:
            return f"{label} must not be a symlink"
        return f"{label} could not be opened"
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            return f"{label} must be a regular file"
        if st.st_size > MAX_METADATA_BYTES:
            return f"{label} exceeds size bound"
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_METADATA_BYTES:
                return f"{label} exceeds size bound"
            chunks.append(chunk)
        data = b"".join(chunks)
        if len(data) != st.st_size:
            return f"{label} size changed during read"
        identity: FileIdentity = (st.st_dev, st.st_ino, st.st_mtime_ns, st.st_size)
        return data, identity
    finally:
        os.close(fd)


def _parse_json_bytes(raw: bytes, *, label: str) -> Any | str:
    if not raw.strip():
        return f"{label} is empty"
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return f"{label} is not valid JSON"


def _try_shared_lock(lock_path: Path) -> Any | None:
    """Open an existing lock file read-only; never create it."""
    try:
        import fcntl
    except ImportError:
        return None
    if not lock_path.exists():
        return None
    link_error = _reject_symlink(lock_path, label="active_sessions.lock")
    if link_error is not None:
        return "symlink"
    flags = os.O_RDONLY
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        flags |= nofollow
    try:
        fd = os.open(lock_path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        if exc.errno in {errno.ELOOP, getattr(errno, "EMLINK", -1)}:
            return "symlink"
        return None
    handle = os.fdopen(fd, "rb")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return "contended"
    except OSError:
        handle.close()
        return None
    return handle


def _release_lock(handle: Any) -> None:
    if handle is None or handle in {"contended", "symlink"}:
        return
    try:
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        handle.close()
    except OSError:
        pass


def _unsafe(reason: str, **fields: Any) -> VisibleSessionObservation:
    return VisibleSessionObservation(kind="unsafe", reason=reason, **fields)


def _transient(reason: str, **fields: Any) -> VisibleSessionObservation:
    return VisibleSessionObservation(kind="transient-missing", reason=reason, **fields)


def _validate_row(row: object) -> dict[str, Any] | str:
    if not isinstance(row, dict):
        return "active_sessions row is not an object"
    missing = [key for key in REQUIRED_ROW_KEYS if key not in row]
    if missing:
        return f"active_sessions row missing keys: {missing}"
    pid = row.get("pid")
    if type(pid) is not int or isinstance(pid, bool) or pid <= 0:
        return "active_sessions.pid must be a positive int"
    cwd = row.get("cwd")
    if not isinstance(cwd, str) or not cwd:
        return "active_sessions.cwd must be a non-empty string"
    session_id = row.get("session_id")
    if not _is_uuid(session_id):
        return "active_sessions.session_id must be a UUID string"
    opened_at = _parse_rfc3339(row.get("opened_at"))
    if opened_at is None:
        return "active_sessions.opened_at must be timezone-bearing RFC3339"
    return {
        "pid": pid,
        "cwd": cwd,
        "session_id": str(session_id),
        "opened_at": opened_at,
    }


def _rows_equal(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return (
        left["pid"] == right["pid"]
        and left["session_id"] == right["session_id"]
        and left["opened_at"] == right["opened_at"]
        and left["cwd"] == right["cwd"]
    )


def _validate_summary(
    summary: object,
    *,
    session_id: str,
    project: Path,
    grok_home: Path,
) -> dict[str, Any] | str:
    if not isinstance(summary, dict):
        return "summary.json is not an object"
    for key in SUMMARY_IDENTITY_KEYS:
        if key not in summary:
            return f"summary.json missing identity field: {key}"
    version = summary.get("chat_format_version")
    if version != PINNED_CHAT_FORMAT_VERSION:
        return f"summary.chat_format_version must be {PINNED_CHAT_FORMAT_VERSION}"
    created_at = _parse_rfc3339(summary.get("created_at"))
    if created_at is None:
        return "summary.created_at must be timezone-bearing RFC3339"
    info = summary.get("info")
    if not isinstance(info, dict):
        return "summary.info must be an object"
    info_id = info.get("id")
    info_cwd = info.get("cwd")
    if not _is_uuid(info_id):
        return "summary.info.id must be a UUID string"
    if info_id != session_id:
        return "summary.info.id does not match session_id"
    if not isinstance(info_cwd, str) or not info_cwd:
        return "summary.info.cwd must be a non-empty string"
    summary_home = summary.get("grok_home")
    if not isinstance(summary_home, str) or not summary_home:
        return "summary.grok_home must be a non-empty string"
    try:
        canonical_info_cwd = canonical_path(info_cwd)
        canonical_summary_home = canonical_path(summary_home)
        canonical_project = canonical_path(project)
        canonical_home = canonical_path(grok_home)
    except (OSError, RuntimeError, ValueError):
        return "summary path fields could not be resolved"
    if canonical_info_cwd != canonical_project:
        return "summary.info.cwd does not match configured project"
    if canonical_summary_home != canonical_home:
        return "summary.grok_home does not match configured GROK_HOME"
    git_root = summary.get("git_root_dir")
    git_root_out = git_root if isinstance(git_root, str) else None
    return {
        "created_at": created_at,
        "cwd": str(canonical_info_cwd),
        "grok_home": str(canonical_summary_home),
        "git_root_dir": git_root_out,
        "chat_format_version": version,
    }


def _load_active_payload(
    active_path: Path,
) -> tuple[list[Any], FileIdentity] | VisibleSessionObservation:
    read = _read_regular_file(active_path, label="active_sessions.json")
    if isinstance(read, str):
        if read == "missing":
            return _transient("active_sessions.json is missing")
        if "symlink" in read:
            return _unsafe(read)
        if "size" in read or "regular" in read:
            return _unsafe(read)
        return _transient(read)
    raw, identity = read
    confirm = _read_regular_file(active_path, label="active_sessions.json")
    if isinstance(confirm, str) or confirm[1] != identity:
        return _transient("active_sessions.json changed during observation")
    parsed = _parse_json_bytes(raw, label="active_sessions.json")
    if isinstance(parsed, str):
        if "empty" in parsed or "valid JSON" in parsed:
            return _transient(parsed)
        return _unsafe(parsed)
    if not isinstance(parsed, list):
        return _unsafe("active_sessions.json top level must be a JSON array")
    return parsed, identity


def _pager_session_fields(
    *,
    grok_home: Path,
    project: Path,
    session_id: str,
    tui_pid: int,
    sample_cwd: str,
) -> dict[str, Any] | VisibleSessionObservation:
    """Validate one pager-selected ID without consulting the stale PID map."""

    try:
        home = canonical_path(grok_home)
        project_path = canonical_path(project)
        sample_path = canonical_path(sample_cwd)
    except (OSError, RuntimeError, ValueError):
        return _unsafe("pager status path fields could not be resolved", pid=tui_pid)
    if sample_path != project_path:
        return _unsafe(
            "pager status cwd does not match configured project",
            pid=tui_pid,
            session_id=session_id,
            cwd=str(sample_path),
        )
    sessions_root = home / "sessions"
    sessions_link = _reject_symlink(sessions_root, label="sessions root")
    if sessions_link is not None:
        return _unsafe(sessions_link, pid=tui_pid, session_id=session_id)
    session_dir = session_directory_for(home, project_path, session_id)
    try:
        resolved_session_dir = canonical_path(session_dir)
        resolved_sessions_root = canonical_path(sessions_root)
    except (OSError, RuntimeError, ValueError):
        return _unsafe(
            "session directory path could not be resolved",
            pid=tui_pid,
            session_id=session_id,
        )
    if not _path_contained(resolved_session_dir, resolved_sessions_root):
        return _unsafe(
            "session directory escapes GROK_HOME/sessions",
            pid=tui_pid,
            session_id=session_id,
            session_directory=str(resolved_session_dir),
        )
    if not resolved_session_dir.is_dir():
        return _unsafe(
            "session directory is absent",
            pid=tui_pid,
            session_id=session_id,
            session_directory=str(resolved_session_dir),
        )
    summary_path = session_dir / "summary.json"
    summary_read = _read_regular_file(summary_path, label="summary.json")
    if isinstance(summary_read, str):
        fields = {
            "pid": tui_pid,
            "session_id": session_id,
            "session_directory": str(resolved_session_dir),
        }
        if summary_read == "missing":
            return _transient("summary.json is missing", **fields)
        if "could not be opened" in summary_read:
            return _transient(summary_read, **fields)
        if "symlink" in summary_read or "size" in summary_read or "regular" in summary_read:
            return _unsafe(summary_read, **fields)
        return _transient(summary_read, **fields)
    summary_raw, summary_identity = summary_read
    summary_confirm = _read_regular_file(summary_path, label="summary.json")
    if isinstance(summary_confirm, str) or summary_confirm[1] != summary_identity:
        return _transient(
            "summary.json changed during observation",
            pid=tui_pid,
            session_id=session_id,
        )
    summary_payload = _parse_json_bytes(summary_raw, label="summary.json")
    if isinstance(summary_payload, str):
        return _transient(
            summary_payload,
            pid=tui_pid,
            session_id=session_id,
        )
    summary = _validate_summary(
        summary_payload,
        session_id=session_id,
        project=project_path,
        grok_home=home,
    )
    if isinstance(summary, str):
        return _unsafe(
            summary,
            pid=tui_pid,
            session_id=session_id,
            session_directory=str(resolved_session_dir),
        )
    return {
        "session_id": session_id,
        "pid": tui_pid,
        "cwd": str(sample_path),
        "created_at": summary["created_at"],
        "grok_home": summary["grok_home"],
        "git_root_dir": summary["git_root_dir"],
        "session_directory": str(resolved_session_dir),
    }


def observe_pager_session(
    *,
    setup: PagerStatusSetup,
    grok_home: Path,
    project: Path,
    tui_pid: int,
    bound_session_id: str,
    agent_version: str,
    minimum_monotonic_ns: int | None = None,
    minimum_state_monotonic_ns: int | None = None,
    tested_versions: set[str] | None = None,
    pid_alive: PidAliveFn | None = None,
) -> VisibleSessionObservation:
    """Observe the exact conversation selected by the owned Grok pager.

    Unlike ``active_sessions.json``, this pager-scoped source follows a
    same-process ``/resume``. A second read after summary validation prevents
    a rapid S2→S3 switch from committing S2.
    """

    allowed = tested_versions if tested_versions is not None else TESTED_GROK_VERSIONS
    if not isinstance(agent_version, str) or agent_version not in allowed:
        return _unsafe(
            f"ACP agentVersion {agent_version!r} is not in the tested allowlist"
        )
    if type(tui_pid) is not int or isinstance(tui_pid, bool) or tui_pid <= 0:
        return _unsafe("tui_pid must be a positive int")
    if not _is_uuid(bound_session_id):
        return _unsafe("bound_session_id must be a UUID string")
    alive = pid_alive or _pid_alive
    if not alive(tui_pid):
        return _unsafe("owned TUI pid is not alive", pid=tui_pid)

    read = read_pager_status(
        setup,
        tui_pid=tui_pid,
        agent_version=agent_version,
        minimum_monotonic_ns=minimum_monotonic_ns,
        minimum_state_monotonic_ns=minimum_state_monotonic_ns,
    )
    if read.kind == "transient":
        return _transient(read.reason, pid=tui_pid)
    if read.kind == "unsafe" or read.sample is None:
        return _unsafe(read.reason, pid=tui_pid)
    sample = read.sample
    fields = _pager_session_fields(
        grok_home=grok_home,
        project=project,
        session_id=sample.session_id,
        tui_pid=tui_pid,
        sample_cwd=sample.cwd,
    )
    if isinstance(fields, VisibleSessionObservation):
        return fields

    confirmed = read_pager_status(
        setup,
        tui_pid=tui_pid,
        agent_version=agent_version,
        minimum_monotonic_ns=minimum_monotonic_ns,
        minimum_state_monotonic_ns=minimum_state_monotonic_ns,
    )
    if confirmed.kind == "transient":
        return _transient(confirmed.reason, pid=tui_pid, session_id=sample.session_id)
    if confirmed.kind == "unsafe" or confirmed.sample is None:
        return _unsafe(confirmed.reason, pid=tui_pid, session_id=sample.session_id)
    if (
        confirmed.sample.session_id != sample.session_id
        or canonical_path(confirmed.sample.cwd) != canonical_path(sample.cwd)
        or confirmed.sample.state_session_id != sample.state_session_id
        or confirmed.sample.state_observed_monotonic_ns
        != sample.state_observed_monotonic_ns
    ):
        return _transient(
            "pager visible session changed during confirmation",
            pid=tui_pid,
            session_id=sample.session_id,
        )
    if not alive(tui_pid):
        return _unsafe("owned TUI pid is not alive", pid=tui_pid)
    fields.update(
        {
            "focus_source": "pager-status",
            "status_trigger": confirmed.sample.trigger,
            "status_sample_monotonic_ns": confirmed.sample.captured_monotonic_ns,
            "focus_state_monotonic_ns": (
                confirmed.sample.state_observed_monotonic_ns
            ),
        }
    )
    if sample.session_id == bound_session_id:
        return VisibleSessionObservation(
            kind="aligned",
            reason="pager status matches bound session",
            **fields,
        )
    return VisibleSessionObservation(
        kind="visible-change",
        reason="owned pager selected a different validated session",
        **fields,
    )


def observe_visible_session(
    *,
    grok_home: Path,
    project: Path,
    tui_pid: int,
    bound_session_id: str,
    agent_version: str,
    tested_versions: set[str] | None = None,
    pid_alive: PidAliveFn | None = None,
) -> VisibleSessionObservation:
    """Observe which conversation the owned TUI PID currently shows.

    Returns ``aligned`` when the visible session equals ``bound_session_id``,
    ``visible-change`` when a different uniquely validated session is selected
    by that PID, ``transient-missing`` for brief lock/replacement races, and
    ``unsafe`` for ambiguous or foreign mappings.
    """
    allowed = tested_versions if tested_versions is not None else TESTED_GROK_VERSIONS
    if not isinstance(agent_version, str) or agent_version not in allowed:
        return _unsafe(
            f"ACP agentVersion {agent_version!r} is not in the tested allowlist"
        )
    if type(tui_pid) is not int or isinstance(tui_pid, bool) or tui_pid <= 0:
        return _unsafe("tui_pid must be a positive int")
    if not _is_uuid(bound_session_id):
        return _unsafe("bound_session_id must be a UUID string")

    alive = pid_alive or _pid_alive
    if not alive(tui_pid):
        return _unsafe("owned TUI pid is not alive", pid=tui_pid)

    try:
        home = canonical_path(grok_home)
        project_path = canonical_path(project)
    except (OSError, RuntimeError, ValueError):
        return _unsafe("grok_home or project could not be resolved")

    active_path = home / ACTIVE_SESSIONS_NAME
    lock_path = home / ACTIVE_SESSIONS_LOCK_NAME
    sessions_root = home / "sessions"

    sessions_link = _reject_symlink(sessions_root, label="sessions root")
    if sessions_link is not None:
        return _unsafe(sessions_link)

    lock_handle = _try_shared_lock(lock_path)
    if lock_handle == "contended":
        return _transient("active_sessions.lock is contended")
    if lock_handle == "symlink":
        return _unsafe("active_sessions.lock must not be a symlink")

    try:
        loaded = _load_active_payload(active_path)
        if isinstance(loaded, VisibleSessionObservation):
            return loaded
        payload, _identity = loaded

        validated_rows: list[dict[str, Any]] = []
        for row in payload:
            validated = _validate_row(row)
            if isinstance(validated, str):
                return _unsafe(validated, pid=tui_pid)
            validated_rows.append(validated)

        matches = [row for row in validated_rows if row["pid"] == tui_pid]
        if not matches:
            return _transient("no active_sessions row for owned TUI pid", pid=tui_pid)
        if len(matches) > 1:
            return _unsafe(
                "multiple active_sessions rows for owned TUI pid",
                pid=tui_pid,
            )

        row = matches[0]
        try:
            row_cwd = canonical_path(row["cwd"])
        except (OSError, RuntimeError, ValueError):
            return _unsafe("active_sessions.cwd could not be resolved", pid=tui_pid)
        if row_cwd != project_path:
            return _unsafe(
                "visible session cwd does not match configured project",
                pid=tui_pid,
                session_id=row["session_id"],
                cwd=str(row_cwd),
            )

        session_id = row["session_id"]
        session_dir = session_directory_for(home, project_path, session_id)
        try:
            resolved_session_dir = canonical_path(session_dir)
            resolved_sessions_root = canonical_path(sessions_root)
        except (OSError, RuntimeError, ValueError):
            return _unsafe(
                "session directory path could not be resolved",
                pid=tui_pid,
                session_id=session_id,
            )
        if not _path_contained(resolved_session_dir, resolved_sessions_root):
            return _unsafe(
                "session directory escapes GROK_HOME/sessions",
                pid=tui_pid,
                session_id=session_id,
                session_directory=str(resolved_session_dir),
            )
        if not resolved_session_dir.is_dir():
            return _unsafe(
                "session directory is absent",
                pid=tui_pid,
                session_id=session_id,
                session_directory=str(resolved_session_dir),
            )

        summary_path = session_dir / "summary.json"
        summary_read = _read_regular_file(summary_path, label="summary.json")
        if isinstance(summary_read, str):
            if summary_read == "missing":
                return _transient(
                    "summary.json is missing",
                    pid=tui_pid,
                    session_id=session_id,
                    session_directory=str(resolved_session_dir),
                )
            if "symlink" in summary_read:
                return _unsafe(
                    summary_read,
                    pid=tui_pid,
                    session_id=session_id,
                    session_directory=str(resolved_session_dir),
                )
            if "size" in summary_read or "regular" in summary_read:
                return _unsafe(
                    summary_read,
                    pid=tui_pid,
                    session_id=session_id,
                    session_directory=str(resolved_session_dir),
                )
            return _transient(
                summary_read,
                pid=tui_pid,
                session_id=session_id,
                session_directory=str(resolved_session_dir),
            )
        summary_raw, summary_identity = summary_read
        summary_confirm = _read_regular_file(summary_path, label="summary.json")
        if isinstance(summary_confirm, str) or summary_confirm[1] != summary_identity:
            return _transient(
                "summary.json changed during observation",
                pid=tui_pid,
                session_id=session_id,
            )
        summary_payload = _parse_json_bytes(summary_raw, label="summary.json")
        if isinstance(summary_payload, str):
            return _transient(
                summary_payload,
                pid=tui_pid,
                session_id=session_id,
            )
        summary = _validate_summary(
            summary_payload,
            session_id=session_id,
            project=project_path,
            grok_home=home,
        )
        if isinstance(summary, str):
            return _unsafe(
                summary,
                pid=tui_pid,
                session_id=session_id,
                session_directory=str(resolved_session_dir),
            )

        # Full-row stable confirmation: re-read and require the same unique match.
        confirmed = _load_active_payload(active_path)
        if isinstance(confirmed, VisibleSessionObservation):
            if confirmed.kind == "transient-missing":
                return _transient(
                    "active_sessions.json changed during confirmation",
                    pid=tui_pid,
                    session_id=session_id,
                )
            return confirmed
        confirm_payload, _ = confirmed
        confirm_rows: list[dict[str, Any]] = []
        for item in confirm_payload:
            validated = _validate_row(item)
            if isinstance(validated, str):
                return _unsafe(validated, pid=tui_pid)
            confirm_rows.append(validated)
        confirm_matches = [item for item in confirm_rows if item["pid"] == tui_pid]
        if len(confirm_matches) != 1 or not _rows_equal(confirm_matches[0], row):
            return _transient(
                "owned TUI visible session changed during confirmation",
                pid=tui_pid,
                session_id=session_id,
            )

        if not alive(tui_pid):
            return _unsafe("owned TUI pid is not alive", pid=tui_pid)

        fields = {
            "session_id": session_id,
            "pid": tui_pid,
            "cwd": str(row_cwd),
            "opened_at": row["opened_at"],
            "created_at": summary["created_at"],
            "grok_home": summary["grok_home"],
            "git_root_dir": summary["git_root_dir"],
            "session_directory": str(resolved_session_dir),
        }
        if session_id == bound_session_id:
            return VisibleSessionObservation(
                kind="aligned", reason="visible session matches bound session", **fields
            )
        return VisibleSessionObservation(
            kind="visible-change",
            reason="owned TUI selected a different validated session",
            **fields,
        )
    finally:
        _release_lock(lock_handle)
