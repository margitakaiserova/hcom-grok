"""Private Grok pager-status recorder and seat configuration guard.

Grok 1.0.13's pager status-line command is the only supported structured
surface observed to follow an in-process ``/resume`` selection.  This module
keeps that command inert outside the exact adapter-owned TUI and refuses to
install it in a shared or policy-managed Grok home.
"""

from __future__ import annotations

import errno
import hashlib
import hmac
import json
import os
import pwd
import shlex
import socket
import stat
import sys
import time
import tomllib
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

PAGER_RECORD_SCHEMA = 1
STATUS_PAYLOAD_SCHEMA = 1
MAX_STATUS_INPUT_BYTES = 65_536
MAX_CONFIG_BYTES = 1_048_576
MAX_STATUS_AGE_NS = 4_000_000_000
CONFIG_NAME = "config.toml"
CONFIG_TRANSACTION_NAME = "pager-config-transaction.json"
OWNER_CLAIM_NAME = ".hcom-grok-pager-owner.json"
STATUS_RECORD_NAME = "pager-status.json"
STATUS_SHIM_NAME = "pager-status-command.py"
PAGER_SOCKET_ROOT_PREFIX = "hcom-grok-pager"
MARKER_PREFIX = "# managed-by: hcom-grok pager-status-v1 owner="

ReadKind = Literal["valid", "transient", "unsafe"]
AdmissionReadKind = Literal["valid", "missing", "transient", "unsafe"]


@dataclass(frozen=True)
class PagerStatusSetup:
    """Result of the private-home configuration admission check."""

    enabled: bool
    reason: str
    status_path: Path
    token: str
    config_path: Path
    config_transaction_path: Path
    owner_claim_path: Path
    owner_id: str
    expected_block: bytes = b""
    expected_claim: bytes = b""
    shim_path: Path | None = None
    expected_shim: bytes = b""
    ingest_socket_path: Path | None = None
    python_executable: Path | None = None

    @property
    def expected_config_sha256(self) -> str | None:
        if not self.expected_block:
            return None
        return hashlib.sha256(self.expected_block).hexdigest()

@dataclass(frozen=True)
class PagerStatusSample:
    session_id: str
    cwd: str
    status_schema_version: int
    grok_version: str
    trigger: str
    tui_pid: int
    captured_monotonic_ns: int
    captured_wall_ns: int
    state_session_id: str
    state_cwd: str
    state_observed_monotonic_ns: int


@dataclass(frozen=True)
class PagerStatusRead:
    kind: ReadKind
    reason: str
    sample: PagerStatusSample | None = None


@dataclass(frozen=True)
class _AdmissionFileRead:
    """A file read that preserves retryable I/O versus proven drift."""

    kind: AdmissionReadKind
    reason: str
    raw: bytes | None = None


@dataclass(frozen=True)
class PagerIngestResult:
    """Outcome after the supervisor authenticates one recorder connection."""

    kind: Literal["recorded", "invalidated"]
    reason: str
    state_observed_monotonic_ns: int | None = None
    captured_monotonic_ns: int | None = None


def _canonical(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _is_real_owned_directory(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISDIR(info.st_mode)
        and not stat.S_ISLNK(info.st_mode)
        and info.st_uid == os.getuid()
        and info.st_mode & 0o077 == 0
    )


def _read_regular(
    path: Path,
    *,
    limit: int,
    require_private: bool = False,
    require_owned: bool = False,
    require_single_link: bool = False,
    required_mode: int | None = None,
) -> bytes | None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        info = os.fstat(fd)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_size > limit
            or require_owned
            and info.st_uid != os.getuid()
            or require_single_link
            and info.st_nlink != 1
            or required_mode is not None
            and stat.S_IMODE(info.st_mode) != required_mode
            or require_private
            and (info.st_uid != os.getuid() or info.st_mode & 0o077 != 0)
        ):
            return None
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                return None
            chunks.append(chunk)
        if total != info.st_size:
            return None
        return b"".join(chunks)
    finally:
        os.close(fd)


def _admission_metadata_problem(
    info: os.stat_result,
    *,
    limit: int,
    require_private: bool,
    require_owned: bool,
    require_single_link: bool,
    required_mode: int | None,
) -> str | None:
    if not stat.S_ISREG(info.st_mode):
        return "must be a regular file"
    if info.st_size > limit:
        return "exceeds the size bound"
    if (require_owned or require_private) and info.st_uid != os.getuid():
        return "must be owned by the current user"
    if require_single_link and info.st_nlink != 1:
        return "must have exactly one link"
    if required_mode is not None and stat.S_IMODE(info.st_mode) != required_mode:
        return f"must have mode {required_mode:04o}"
    if require_private and info.st_mode & 0o077:
        return "must not be group- or world-accessible"
    return None


def _read_admission_file(
    path: Path,
    *,
    limit: int,
    require_private: bool = False,
    require_owned: bool = False,
    require_single_link: bool = False,
    required_mode: int | None = None,
) -> _AdmissionFileRead:
    """Read one stable file while retaining the cause of read failure."""

    try:
        path_before = path.lstat()
    except FileNotFoundError:
        return _AdmissionFileRead("missing", "is missing")
    except OSError:
        return _AdmissionFileRead("transient", "could not be inspected")
    if stat.S_ISLNK(path_before.st_mode):
        return _AdmissionFileRead("unsafe", "must not be a symlink")
    problem = _admission_metadata_problem(
        path_before,
        limit=limit,
        require_private=require_private,
        require_owned=require_owned,
        require_single_link=require_single_link,
        required_mode=required_mode,
    )
    if problem is not None:
        return _AdmissionFileRead("unsafe", problem)

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return _AdmissionFileRead("transient", "changed before it could be opened")
    except OSError as exc:
        if exc.errno in {errno.ELOOP, getattr(errno, "EMLINK", -1)}:
            return _AdmissionFileRead("unsafe", "must not be a symlink")
        return _AdmissionFileRead("transient", "could not be opened")
    try:
        try:
            before = os.fstat(fd)
        except OSError:
            return _AdmissionFileRead("transient", "could not be inspected after open")
        problem = _admission_metadata_problem(
            before,
            limit=limit,
            require_private=require_private,
            require_owned=require_owned,
            require_single_link=require_single_link,
            required_mode=required_mode,
        )
        if problem is not None:
            return _AdmissionFileRead("unsafe", problem)
        if (before.st_dev, before.st_ino) != (path_before.st_dev, path_before.st_ino):
            return _AdmissionFileRead("transient", "changed while being opened")

        chunks: list[bytes] = []
        total = 0
        try:
            while True:
                chunk = os.read(fd, min(65_536, limit + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    return _AdmissionFileRead("unsafe", "exceeds the size bound")
                chunks.append(chunk)
            after = os.fstat(fd)
        except OSError:
            return _AdmissionFileRead("transient", "could not be read")
        problem = _admission_metadata_problem(
            after,
            limit=limit,
            require_private=require_private,
            require_owned=require_owned,
            require_single_link=require_single_link,
            required_mode=required_mode,
        )
        if problem is not None:
            return _AdmissionFileRead("unsafe", problem)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_nlink",
            "st_mode",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if total != before.st_size or any(
            getattr(before, field) != getattr(after, field) for field in stable_fields
        ):
            return _AdmissionFileRead("transient", "changed during read")
        try:
            path_after = path.lstat()
        except OSError:
            return _AdmissionFileRead("transient", "changed after read")
        problem = _admission_metadata_problem(
            path_after,
            limit=limit,
            require_private=require_private,
            require_owned=require_owned,
            require_single_link=require_single_link,
            required_mode=required_mode,
        )
        if problem is not None:
            return _AdmissionFileRead("unsafe", problem)
        if (path_after.st_dev, path_after.st_ino) != (after.st_dev, after.st_ino):
            return _AdmissionFileRead("transient", "changed after read")
        return _AdmissionFileRead("valid", "is valid", b"".join(chunks))
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def _restore_detached_path(detached: Path, destination: Path) -> bool:
    """Best-effort no-clobber restore for a file detached during cleanup."""

    try:
        details = detached.lstat()
        if stat.S_ISREG(details.st_mode):
            os.link(detached, destination, follow_symlinks=False)
        elif stat.S_ISLNK(details.st_mode):
            os.symlink(os.readlink(detached), destination)
        else:
            return False
        # The replacement name must be durable before the only other name is
        # removed.  A crash between these fsyncs intentionally leaves both
        # names for transaction recovery to recognize as the same inode.
        _fsync_directory(destination.parent)
        detached.unlink()
        _fsync_directory(detached.parent)
        return True
    except OSError:
        return False


def _detach_validated_file(
    path: Path,
    *,
    limit: int,
    validator: Callable[[bytes], bool],
    require_private: bool = True,
    require_owned: bool = False,
    require_single_link: bool = False,
    required_mode: int | None = None,
) -> tuple[bool, str | None]:
    """Detach first, then remove only validated bytes at a random pathname.

    A racing replacement is restored with an exclusive hard-link/symlink
    create when possible. If its original name is already occupied, both
    objects are retained and the quarantine path is reported to the caller.
    """

    quarantine = path.with_name(f".{path.name}.retained-{uuid.uuid4().hex}")
    try:
        os.rename(path, quarantine)
    except FileNotFoundError:
        if path.exists() or path.is_symlink():
            return False, str(path)
        return True, None
    except OSError:
        return False, str(path)

    try:
        before = quarantine.lstat()
    except OSError:
        return False, str(quarantine)
    raw = _read_regular(
        quarantine,
        limit=limit,
        require_private=require_private,
        require_owned=require_owned,
        require_single_link=require_single_link,
        required_mode=required_mode,
    )
    try:
        after = quarantine.lstat()
    except OSError:
        return False, str(quarantine)
    identity_stable = (
        before.st_dev,
        before.st_ino,
        before.st_mtime_ns,
        before.st_size,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_mtime_ns,
        after.st_size,
    )
    if raw is not None and identity_stable and validator(raw):
        try:
            quarantine.unlink()
        except OSError:
            return False, str(quarantine)
        if path.exists() or path.is_symlink():
            return False, str(path)
        return True, None

    if _restore_detached_path(quarantine, path):
        return False, str(path)
    return False, str(quarantine)


def _atomic_replace(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fchmod(fd, mode)
        os.fsync(fd)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(fd)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _atomic_create(path: Path, payload: bytes, *, mode: int = 0o600) -> bool:
    """Publish a new regular file without replacing a racing writer."""

    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fchmod(fd, mode)
        os.fsync(fd)
        os.close(fd)
        fd = -1
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            return False
        directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            temporary.unlink()
        except OSError:
            pass


def _fsync_directory(path: Path) -> None:
    directory_fd = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _snapshot_fd(fd: int, *, limit: int) -> tuple[bytes, os.stat_result] | None:
    """Read one stable, current-user-owned regular-file generation."""

    try:
        os.lseek(fd, 0, os.SEEK_SET)
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size > limit
        ):
            return None
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                return None
            chunks.append(chunk)
        after = os.fstat(fd)
        stable_fields = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_nlink",
            "st_mode",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
            return None
        return b"".join(chunks), after
    except OSError:
        return None


def _regular_snapshot(
    path: Path,
    *,
    limit: int,
) -> tuple[int, bytes, os.stat_result] | None:
    """Open and read one stable, owned regular-file generation."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    snapshot = _snapshot_fd(fd, limit=limit)
    if snapshot is None:
        os.close(fd)
        return None
    raw, info = snapshot
    return fd, raw, info


def _copy_fd_metadata(source_fd: int, destination_fd: int) -> None:
    """Preserve mode plus ACL/xattrs when the host exposes an fd API."""

    source = os.fstat(source_fd)
    if sys.platform == "darwin":
        # Python builds bundled by some clients omit os.*xattr. macOS's
        # fcopyfile is fd-scoped, so it also avoids following a swapped path.
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        fcopyfile = libc.fcopyfile
        fcopyfile.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_uint32,
        ]
        fcopyfile.restype = ctypes.c_int
        copyfile_acl_and_xattrs = (1 << 0) | (1 << 2)
        if fcopyfile(
            source_fd,
            destination_fd,
            None,
            copyfile_acl_and_xattrs,
        ) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
    elif all(
        hasattr(os, name)
        for name in ("listxattr", "getxattr", "setxattr")
    ):
        for name in os.listxattr(source_fd):  # type: ignore[attr-defined]
            value = os.getxattr(source_fd, name)  # type: ignore[attr-defined]
            os.setxattr(destination_fd, name, value)  # type: ignore[attr-defined]
    os.fchmod(destination_fd, stat.S_IMODE(source.st_mode))


def _write_transform_temporary(
    temporary: Path,
    payload: bytes,
) -> None:
    fd = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(fd)
    except BaseException:
        try:
            temporary.unlink()
        except OSError:
            pass
        raise
    finally:
        os.close(fd)


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _claim_payload(owner_id: str, state_root: Path, grok_home: Path) -> bytes:
    value = {
        "schema": 1,
        "owner_id": owner_id,
        "state_root": str(state_root),
        "grok_home": str(grok_home),
        "uid": os.getuid(),
    }
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _claim_matches(raw: bytes, expected: bytes) -> bool:
    return hmac.compare_digest(raw, expected)


def _record_authentication_input(record: Mapping[str, Any]) -> bytes:
    return json.dumps(
        dict(record),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _encode_authenticated_record(
    record: Mapping[str, Any], token: str
) -> bytes:
    body = dict(record)
    body.pop("auth_tag", None)
    tag = hmac.new(
        token.encode("utf-8"),
        _record_authentication_input(body),
        hashlib.sha256,
    ).hexdigest()
    return (
        json.dumps({**body, "auth_tag": tag}, ensure_ascii=False, sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _decode_authenticated_record(
    raw: bytes, token: str
) -> dict[str, Any] | None:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    tag = value.pop("auth_tag", None)
    if not isinstance(tag, str):
        return None
    expected = hmac.new(
        token.encode("utf-8"),
        _record_authentication_input(value),
        hashlib.sha256,
    ).hexdigest()
    return value if hmac.compare_digest(tag, expected) else None


def _pager_shim(
    owner_id: str,
    python_executable: Path,
    package_root: Path,
    ingest_socket_path: Path,
) -> bytes:
    text = (
        f"#!{python_executable}\n"
        f"{MARKER_PREFIX}{owner_id}\n"
        "import sys\n"
        f"sys.path.insert(0, {str(package_root)!r})\n"
        "from scripts.hcom_grok_seat.pager_status import main\n"
        "raise SystemExit(main([\"--submit-pager-status\", "
        f"\"--socket-path\", {str(ingest_socket_path)!r}]))\n"
    )
    return text.encode("utf-8")


def _owned_shim(raw: bytes, owner_id: str) -> bool:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return False
    lines = text.splitlines()
    return (
        len(lines) == 6
        and lines[0].startswith("#!")
        and lines[1] == f"{MARKER_PREFIX}{owner_id}"
        and lines[2] == "import sys"
        and lines[3].startswith("sys.path.insert(0, ")
        and lines[4] == "from scripts.hcom_grok_seat.pager_status import main"
        and lines[5].startswith("raise SystemExit(main(")
    )


def _pager_config_block(
    owner_id: str,
    python_executable: Path,
    shim_path: Path,
    ingest_socket_path: Path,
) -> bytes:
    del python_executable, ingest_socket_path
    # Grok executes a command containing only one executable path directly;
    # any multi-word line goes through ``sh -c`` and loses direct-TUI ancestry.
    command = shlex.join([str(shim_path)])
    command_literal = json.dumps(command, ensure_ascii=False)
    text = (
        f"\n{MARKER_PREFIX}{owner_id}\n"
        "[ui.status_line]\n"
        'type = "command"\n'
        f"command = {command_literal}\n"
        "refresh_interval = 1\n"
    )
    return text.encode("utf-8")


def _semantic_status_config(
    raw: bytes,
    python_executable: Path,
    shim_path: Path,
    ingest_socket_path: Path,
) -> bool:
    try:
        parsed = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        return False
    ui = parsed.get("ui")
    if not isinstance(ui, dict):
        return False
    status_line = ui.get("status_line")
    if not (
        isinstance(status_line, dict)
        and status_line.get("type") == "command"
        and isinstance(status_line.get("command"), str)
        and status_line.get("refresh_interval") == 1
        and set(status_line) == {"type", "command", "refresh_interval"}
    ):
        return False
    try:
        command = shlex.split(status_line["command"])
    except ValueError:
        return False
    if len(command) != 1:
        return False
    try:
        command_shim_path = _canonical(command[0])
    except (OSError, RuntimeError, ValueError):
        return False
    del python_executable, ingest_socket_path
    return command_shim_path == _canonical(shim_path)


def _owned_user_config(
    raw: bytes,
    expected_block: bytes,
    owner_id: str,
    python_executable: Path,
    shim_path: Path,
    ingest_socket_path: Path,
) -> bool:
    marker = f"{MARKER_PREFIX}{owner_id}\n".encode()
    return (
        raw.count(expected_block) == 1
        and raw.count(marker) == 1
        and _semantic_status_config(
            raw,
            python_executable,
            shim_path,
            ingest_socket_path,
        )
    )


def _without_owned_block(raw: bytes, expected_block: bytes) -> bytes | None:
    if raw.count(expected_block) != 1:
        return None
    return raw.replace(expected_block, b"", 1)


def _remove_exact_file(
    path: Path,
    expected: bytes,
    *,
    require_private: bool,
) -> bool:
    if not path.exists() and not path.is_symlink():
        return True
    removed, _retained = _detach_validated_file(
        path,
        limit=max(MAX_CONFIG_BYTES, len(expected)),
        validator=lambda raw: hmac.compare_digest(raw, expected),
        require_private=require_private,
        require_owned=True,
    )
    return removed


def _transaction_record(
    setup: PagerStatusSetup,
    *,
    operation: Literal["inject", "clean"],
    quarantine: Path,
    temporary: Path,
    original: bytes,
    intended: bytes,
) -> bytes:
    value = {
        "schema": 1,
        "owner_id": setup.owner_id,
        "operation": operation,
        "target": str(setup.config_path),
        "quarantine": str(quarantine),
        "temporary": str(temporary),
        "original_sha256": _sha256(original),
        "intended_sha256": _sha256(intended),
    }
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def _transaction_artifact(
    value: object,
    *,
    parent: Path,
    owner_id: str,
    suffix: str,
) -> Path | None:
    if not isinstance(value, str):
        return None
    path = Path(value)
    prefix = f".{CONFIG_NAME}.hcom-{owner_id}-"
    if (
        path.parent != parent
        or not path.name.startswith(prefix)
        or not path.name.endswith(suffix)
    ):
        return None
    return path


def _recover_config_transaction(setup: PagerStatusSetup) -> str | None:
    """Finish or roll back only an exact adapter-authored config transaction."""

    journal_path = setup.config_transaction_path
    if not journal_path.exists() and not journal_path.is_symlink():
        return None
    journal_raw = _read_regular(journal_path, limit=16_384, require_private=True)
    if journal_raw is None:
        return "pager config transaction journal is unsafe"
    try:
        journal = json.loads(journal_raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "pager config transaction journal is invalid"
    if not isinstance(journal, dict):
        return "pager config transaction journal is invalid"
    operation = journal.get("operation")
    original_hash = journal.get("original_sha256")
    intended_hash = journal.get("intended_sha256")
    quarantine = _transaction_artifact(
        journal.get("quarantine"),
        parent=setup.config_path.parent,
        owner_id=setup.owner_id,
        suffix=".bak",
    )
    temporary = _transaction_artifact(
        journal.get("temporary"),
        parent=setup.config_path.parent,
        owner_id=setup.owner_id,
        suffix=".tmp",
    )
    if not (
        journal.get("schema") == 1
        and journal.get("owner_id") == setup.owner_id
        and operation in {"inject", "clean"}
        and journal.get("target") == str(setup.config_path)
        and isinstance(original_hash, str)
        and len(original_hash) == 64
        and isinstance(intended_hash, str)
        and len(intended_hash) == 64
        and quarantine is not None
        and temporary is not None
    ):
        return "pager config transaction journal does not match this seat"

    target_present = setup.config_path.exists() or setup.config_path.is_symlink()
    target_raw = (
        _read_regular(
            setup.config_path,
            limit=MAX_CONFIG_BYTES,
            require_owned=True,
        )
        if target_present
        else None
    )
    quarantine_present = quarantine.exists() or quarantine.is_symlink()
    quarantine_was_present = quarantine_present
    quarantine_raw = (
        _read_regular(quarantine, limit=MAX_CONFIG_BYTES, require_owned=True)
        if quarantine_present
        else None
    )
    if target_present and target_raw is None:
        return "pager config transaction target is unsafe"
    if quarantine_present and quarantine_raw is None:
        return "pager config transaction quarantine drifted"

    if target_raw is not None and quarantine_raw is not None:
        try:
            target_info = setup.config_path.lstat()
            quarantine_info = quarantine.lstat()
        except OSError:
            return "pager config transaction paths changed during recovery"
        if (
            stat.S_ISREG(target_info.st_mode)
            and stat.S_ISREG(quarantine_info.st_mode)
            and target_info.st_dev == quarantine_info.st_dev
            and target_info.st_ino == quarantine_info.st_ino
        ):
            temporary_raw = None
            if temporary.exists() or temporary.is_symlink():
                temporary_raw = _read_regular(
                    temporary,
                    limit=MAX_CONFIG_BYTES,
                    require_owned=True,
                )
                if temporary_raw is None or _sha256(temporary_raw) != intended_hash:
                    return "pager config transaction temporary file drifted"
            if not _remove_exact_file(
                quarantine,
                quarantine_raw,
                require_private=False,
            ):
                return "pager config rollback duplicate could not be removed"
            if temporary_raw is not None and not _remove_exact_file(
                temporary,
                temporary_raw,
                require_private=False,
            ):
                return "pager config transaction temporary file could not be removed"
            if not _remove_exact_file(
                journal_path,
                journal_raw,
                require_private=True,
            ):
                return "pager config transaction journal could not be removed"
            _fsync_directory(setup.config_path.parent)
            _fsync_directory(journal_path.parent)
            return None

    if quarantine_present and _sha256(quarantine_raw or b"") != original_hash:
        return "pager config transaction quarantine drifted"

    target_hash = _sha256(target_raw) if target_raw is not None else None
    if quarantine_present and target_raw is None:
        if not _restore_detached_path(quarantine, setup.config_path):
            return "pager config transaction rollback could not restore config.toml"
        target_hash = original_hash
        quarantine_present = False
    elif quarantine_present and target_hash == intended_hash:
        if not _remove_exact_file(
            quarantine,
            quarantine_raw or b"",
            require_private=False,
        ):
            return "pager config transaction quarantine could not be removed"
        quarantine_present = False
    elif quarantine_present:
        return "pager config transaction conflicts with a newer config.toml"
    else:
        # With no quarantined generation left there is nothing safe to restore.
        # Preserve the current owned regular target, abandon the journal-named
        # temporary, and let normal admission decide what may follow. If even
        # the canonical name is absent, retain all evidence instead of deleting
        # the only remaining bytes.
        del target_hash
        if target_raw is None:
            return "pager config transaction has no canonical generation"

    if temporary.exists() or temporary.is_symlink():
        temporary_raw = _read_regular(
            temporary,
            limit=MAX_CONFIG_BYTES,
            require_owned=True,
        )
        if temporary_raw is None:
            return "pager config transaction temporary file drifted"
        if quarantine_was_present and _sha256(temporary_raw) != intended_hash:
            return "pager config transaction temporary file drifted"
        if not quarantine_was_present:
            try:
                target_info = setup.config_path.lstat()
                temporary_info = temporary.lstat()
            except OSError:
                return "pager config transaction paths changed during recovery"
            intentional_pair = (
                stat.S_ISREG(target_info.st_mode)
                and stat.S_ISREG(temporary_info.st_mode)
                and target_info.st_dev == temporary_info.st_dev
                and target_info.st_ino == temporary_info.st_ino
                and target_info.st_nlink == 2
                and temporary_info.st_nlink == 2
            )
            if temporary_info.st_nlink != 1 and not intentional_pair:
                return "pager config transaction temporary file is hard-linked"
        if not _remove_exact_file(
            temporary,
            temporary_raw,
            require_private=False,
        ):
            return "pager config transaction temporary file could not be removed"
    if not _remove_exact_file(
        journal_path,
        journal_raw,
        require_private=True,
    ):
        return "pager config transaction journal could not be removed"
    _fsync_directory(setup.config_path.parent)
    _fsync_directory(journal_path.parent)
    return None


def _abort_config_transform(
    setup: PagerStatusSetup,
    *,
    quarantine: Path,
    temporary: Path,
    intended: bytes,
    journal_raw: bytes,
    published: bool,
) -> bool:
    """Restore the detached generation without replacing a racing pathname."""

    if published:
        removed, _retained = _detach_validated_file(
            setup.config_path,
            limit=MAX_CONFIG_BYTES,
            validator=lambda raw: hmac.compare_digest(raw, intended),
            require_private=False,
            require_owned=True,
        )
        if not removed:
            return False
    if not _restore_detached_path(quarantine, setup.config_path):
        return False
    temporary_removed = _remove_exact_file(
        temporary,
        intended,
        require_private=False,
    )
    journal_removed = _remove_exact_file(
        setup.config_transaction_path,
        journal_raw,
        require_private=True,
    )
    if temporary_removed and journal_removed:
        _fsync_directory(setup.config_path.parent)
        _fsync_directory(setup.config_transaction_path.parent)
    return temporary_removed and journal_removed


def _transform_config(
    setup: PagerStatusSetup,
    *,
    original: bytes,
    intended: bytes,
    operation: Literal["inject", "clean"],
) -> str | None:
    """Compare, detach, and no-clobber publish one config transformation."""

    snapshot = _regular_snapshot(setup.config_path, limit=MAX_CONFIG_BYTES)
    if snapshot is None:
        return "config.toml changed or became unsafe before pager update"
    source_fd, observed, source_info = snapshot
    temporary = setup.config_path.with_name(
        f".{CONFIG_NAME}.hcom-{setup.owner_id}-{uuid.uuid4().hex}.tmp"
    )
    quarantine = setup.config_path.with_name(
        f".{CONFIG_NAME}.hcom-{setup.owner_id}-{uuid.uuid4().hex}.bak"
    )
    journal_raw = _transaction_record(
        setup,
        operation=operation,
        quarantine=quarantine,
        temporary=temporary,
        original=original,
        intended=intended,
    )
    try:
        if not hmac.compare_digest(observed, original):
            return "config.toml changed before pager update"
        if len(intended) > MAX_CONFIG_BYTES:
            return "config.toml would exceed the pager safety limit"
        try:
            journal_created = _atomic_create(
                setup.config_transaction_path,
                journal_raw,
            )
        except OSError as exc:
            return f"pager config transaction journal could not be created: {exc}"
        if not journal_created:
            return "pager config transaction journal appeared concurrently"
        try:
            _write_transform_temporary(temporary, intended)
        except OSError as exc:
            _remove_exact_file(
                setup.config_transaction_path,
                journal_raw,
                require_private=True,
            )
            return f"pager config temporary file could not be created: {exc}"
        try:
            os.rename(setup.config_path, quarantine)
        except OSError as exc:
            _remove_exact_file(temporary, intended, require_private=False)
            _remove_exact_file(
                setup.config_transaction_path,
                journal_raw,
                require_private=True,
            )
            return f"config.toml could not be detached safely: {exc}"

        detached = _regular_snapshot(quarantine, limit=MAX_CONFIG_BYTES)
        detached_matches = False
        detached_fd = -1
        if detached is not None:
            detached_fd, detached_raw, detached_info = detached
            detached_matches = (
                detached_info.st_dev == source_info.st_dev
                and detached_info.st_ino == source_info.st_ino
                and hmac.compare_digest(detached_raw, original)
            )
        if not detached_matches:
            if detached_fd >= 0:
                os.close(detached_fd)
            if not _restore_detached_path(quarantine, setup.config_path):
                return "config.toml raced with pager update; quarantine retained"
            _remove_exact_file(temporary, intended, require_private=False)
            _remove_exact_file(
                setup.config_transaction_path,
                journal_raw,
                require_private=True,
            )
            return "config.toml changed while pager update was admitted"

        metadata_fields = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_nlink",
            "st_mode",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        metadata_error: str | None = None
        temporary_fd = -1
        try:
            before_metadata = os.fstat(detached_fd)
            flags = os.O_WRONLY | getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            temporary_fd = os.open(temporary, flags)
            _copy_fd_metadata(detached_fd, temporary_fd)
            os.fsync(temporary_fd)
            after_metadata = os.fstat(detached_fd)
            if any(
                getattr(before_metadata, field) != getattr(after_metadata, field)
                for field in metadata_fields
            ):
                metadata_error = "config.toml metadata changed during pager update"
        except OSError as exc:
            metadata_error = f"config.toml metadata could not be preserved: {exc}"
        finally:
            if temporary_fd >= 0:
                os.close(temporary_fd)
        if metadata_error is not None:
            os.close(detached_fd)
            if not _abort_config_transform(
                setup,
                quarantine=quarantine,
                temporary=temporary,
                intended=intended,
                journal_raw=journal_raw,
                published=False,
            ):
                return f"{metadata_error}; quarantine retained"
            return metadata_error

        # A process that opened the old generation before detachment can still
        # write through that fd. Re-read the detached inode immediately before
        # and after publication; detected bytes or metadata are rolled back to
        # that newer detached generation rather than silently discarded.
        before_publish = _snapshot_fd(detached_fd, limit=MAX_CONFIG_BYTES)
        detached_stable = (
            before_publish is not None
            and hmac.compare_digest(before_publish[0], original)
            and all(
                getattr(before_publish[1], field) == getattr(after_metadata, field)
                for field in metadata_fields
            )
        )
        if not detached_stable:
            os.close(detached_fd)
            restored = _abort_config_transform(
                setup,
                quarantine=quarantine,
                temporary=temporary,
                intended=intended,
                journal_raw=journal_raw,
                published=False,
            )
            suffix = "" if restored else "; quarantine retained"
            return f"config.toml changed through an open file descriptor{suffix}"

        try:
            os.link(temporary, setup.config_path, follow_symlinks=False)
            _fsync_directory(setup.config_path.parent)
        except FileExistsError:
            os.close(detached_fd)
            return "config.toml appeared while pager update was publishing"
        except OSError as exc:
            os.close(detached_fd)
            if not setup.config_path.exists() and not setup.config_path.is_symlink():
                _abort_config_transform(
                    setup,
                    quarantine=quarantine,
                    temporary=temporary,
                    intended=intended,
                    journal_raw=journal_raw,
                    published=False,
                )
            return f"pager config update could not be published: {exc}"

        after_publish = _snapshot_fd(detached_fd, limit=MAX_CONFIG_BYTES)
        detached_stable = (
            after_publish is not None
            and hmac.compare_digest(after_publish[0], original)
            and all(
                getattr(after_publish[1], field) == getattr(before_publish[1], field)
                for field in metadata_fields
            )
        )
        if not detached_stable:
            os.close(detached_fd)
            restored = _abort_config_transform(
                setup,
                quarantine=quarantine,
                temporary=temporary,
                intended=intended,
                journal_raw=journal_raw,
                published=True,
            )
            suffix = "" if restored else "; transaction retained"
            return f"config.toml changed during pager publication{suffix}"

        removed, retained = _detach_validated_file(
            quarantine,
            limit=MAX_CONFIG_BYTES,
            validator=lambda raw: hmac.compare_digest(raw, original),
            require_private=False,
            require_owned=True,
        )
        if not removed:
            # Only roll back from the journal-named inode. If a racer occupied
            # that name, preserve every generation and leave recovery evidence.
            try:
                named_info = quarantine.lstat()
                detached_info = os.fstat(detached_fd)
                same_detached_inode = (
                    stat.S_ISREG(named_info.st_mode)
                    and named_info.st_uid == os.getuid()
                    and named_info.st_dev == detached_info.st_dev
                    and named_info.st_ino == detached_info.st_ino
                )
            except OSError:
                same_detached_inode = False
            os.close(detached_fd)
            if same_detached_inode:
                restored = _abort_config_transform(
                    setup,
                    quarantine=quarantine,
                    temporary=temporary,
                    intended=intended,
                    journal_raw=journal_raw,
                    published=True,
                )
                if restored:
                    return "previous config.toml generation changed; update rolled back"
            detail = retained or str(quarantine)
            return f"previous config.toml generation was retained at {detail}"
        os.close(detached_fd)
        if not _remove_exact_file(temporary, intended, require_private=False):
            return "pager config temporary file was retained"
        if not _remove_exact_file(
            setup.config_transaction_path,
            journal_raw,
            require_private=True,
        ):
            return "pager config transaction journal was retained"
        _fsync_directory(setup.config_path.parent)
        _fsync_directory(setup.config_transaction_path.parent)
        return None
    finally:
        os.close(source_fd)


def disabled_setup(
    reason: str, *, state_root: Path, grok_home: Path, owner_id: str = ""
) -> PagerStatusSetup:
    return PagerStatusSetup(
        enabled=False,
        reason=reason,
        status_path=state_root / STATUS_RECORD_NAME,
        token="",
        config_path=grok_home / CONFIG_NAME,
        config_transaction_path=state_root / CONFIG_TRANSACTION_NAME,
        owner_claim_path=grok_home / OWNER_CLAIM_NAME,
        owner_id=owner_id,
        shim_path=state_root / STATUS_SHIM_NAME,
    )


def prepare_pager_status(
    *,
    state_root: Path,
    launch_home: Path,
    grok_home: Path,
    isolated_home: bool,
    environment: Mapping[str, str] | None = None,
    host_home: Path | None = None,
    system_grok_root: Path = Path("/etc/grok"),
    python_executable: Path | None = None,
    package_root: Path | None = None,
    socket_root: Path | None = None,
) -> PagerStatusSetup:
    """Admit recorder injection only in a proven exclusive seat home."""

    try:
        state = _canonical(state_root)
        home = _canonical(launch_home)
        grok = _canonical(grok_home)
        host = _canonical(
            host_home
            if host_home is not None
            else Path(pwd.getpwuid(os.getuid()).pw_dir)
        )
    except (KeyError, OSError, RuntimeError, ValueError):
        return disabled_setup(
            "pager status paths could not be resolved",
            state_root=state_root,
            grok_home=grok_home,
        )
    owner_id = hashlib.sha256(f"{os.getuid()}\0{state}".encode()).hexdigest()[:24]
    disabled = lambda reason: disabled_setup(  # noqa: E731
        reason, state_root=state, grok_home=grok, owner_id=owner_id
    )
    if not isolated_home:
        return disabled("pager status requires an explicitly isolated Grok home")
    if home == host or grok == host / ".grok":
        return disabled("pager status refuses the shared host Grok home")
    if grok != home / ".grok":
        return disabled("isolated GROK_HOME must be HOME/.grok")
    try:
        state.relative_to(home)
    except ValueError:
        return disabled("pager status state root must be inside the isolated HOME")
    if not all(_is_real_owned_directory(path) for path in (home, grok, state)):
        return disabled("pager status requires real user-owned private directories")

    env = os.environ if environment is None else environment
    if env.get("GROK_CONFIG") or env.get("GROK_CONFIG_PATH"):
        return disabled("pager status refuses an external Grok config overlay")

    # Never occupy a signed seat replica: that is deployment policy rather
    # than an adapter-owned slot. System policy is read-only and untouched.
    del system_grok_root
    signature_path = grok / "managed_config.sig.json"
    if signature_path.exists() or signature_path.is_symlink():
        return disabled("pager status refuses a signed seat managed configuration")

    interpreter = _canonical(python_executable or Path(sys.executable))
    package = _canonical(
        package_root if package_root is not None else Path(__file__).resolve().parents[2]
    )
    socket_parent = _canonical(
        socket_root
        if socket_root is not None
        else Path("/tmp") / f"{PAGER_SOCKET_ROOT_PREFIX}-{os.getuid()}"
    )
    if not socket_parent.exists():
        try:
            socket_parent.mkdir(mode=0o700)
        except OSError as exc:
            return disabled(f"pager socket directory could not be created: {exc}")
    if not _is_real_owned_directory(socket_parent):
        return disabled("pager socket directory must be real, user-owned, and private")
    socket_seed = hashlib.sha256(f"{os.getuid()}\0{state}".encode()).hexdigest()[:20]
    ingest_socket_path = socket_parent / f"p-{socket_seed}.sock"
    if len(os.fsencode(ingest_socket_path)) >= 100:
        return disabled("pager socket path is too long")

    run_id = uuid.uuid4().hex
    status_path = state / f"pager-status-{run_id}.json"
    shim_path = state / STATUS_SHIM_NAME
    token = uuid.uuid4().hex + uuid.uuid4().hex
    interpreter_text = str(interpreter)
    if any(character.isspace() for character in interpreter_text):
        return disabled("pager status Python path cannot be used as a script shebang")
    expected_shim = _pager_shim(
        owner_id,
        interpreter,
        package,
        ingest_socket_path,
    )
    expected_block = _pager_config_block(
        owner_id,
        interpreter,
        shim_path,
        ingest_socket_path,
    )

    claim_path = grok / OWNER_CLAIM_NAME
    expected_claim = _claim_payload(owner_id, state, grok)
    prior_claim = None
    if claim_path.exists() or claim_path.is_symlink():
        prior_claim = _read_regular(
            claim_path, limit=16_384, require_private=True
        )
        if prior_claim is None or not _claim_matches(prior_claim, expected_claim):
            return disabled("isolated Grok home is claimed by another state root")

    config_path = grok / CONFIG_NAME
    transaction_path = state / CONFIG_TRANSACTION_NAME
    transaction_pending = transaction_path.exists() or transaction_path.is_symlink()
    if transaction_pending and prior_claim is None:
        return disabled("pager config transaction has no matching ownership claim")
    if config_path.exists() or config_path.is_symlink():
        config_raw = _read_regular(
            config_path,
            limit=MAX_CONFIG_BYTES,
            require_owned=True,
            # A crash can temporarily leave the intended target hard-linked
            # to its journalled temporary. Recovery validates that exact case
            # after stale Grok processes have been reaped.
            require_single_link=not transaction_pending,
        )
        if config_raw is None:
            return disabled("pager status cannot safely read config.toml")
        if not config_raw.strip():
            return disabled("pager status refuses an empty existing config.toml")
        try:
            parsed = tomllib.loads(config_raw.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            return disabled("pager status cannot validate config.toml")
        features = parsed.get("features")
        if isinstance(features, dict) and features.get("managed_config") is True:
            return disabled("user config enables managed-config synchronization")
        ui = parsed.get("ui")
        has_status_line = isinstance(ui, dict) and "status_line" in ui
        semantic_status_line = _semantic_status_config(
            config_raw,
            interpreter,
            shim_path,
            ingest_socket_path,
        )
        if has_status_line and not semantic_status_line:
            return disabled("pager status refuses an existing foreign status line")
        if semantic_status_line and prior_claim is None:
            return disabled("stale pager status line has no matching ownership claim")
        if not semantic_status_line and MARKER_PREFIX.encode() in config_raw:
            return disabled("pager status refuses an unowned config marker")
        if not has_status_line:
            try:
                tomllib.loads((config_raw + expected_block).decode("utf-8"))
            except (UnicodeDecodeError, tomllib.TOMLDecodeError):
                return disabled("pager status block conflicts with config.toml")

    prior_shim = None
    if shim_path.exists() or shim_path.is_symlink():
        prior_shim = _read_regular(
            shim_path,
            limit=MAX_CONFIG_BYTES,
            require_private=True,
            required_mode=0o700,
        )
        if prior_shim is None or not hmac.compare_digest(prior_shim, expected_shim):
            return disabled("pager status refuses an existing unowned command shim")
        if prior_claim is None:
            return disabled("stale pager command shim has no matching ownership claim")

    if prior_claim is None and not _atomic_create(claim_path, expected_claim):
        return disabled("isolated Grok home ownership changed during admission")

    return PagerStatusSetup(
        enabled=True,
        reason="pager status recorder configured",
        status_path=status_path,
        token=token,
        config_path=config_path,
        config_transaction_path=transaction_path,
        owner_claim_path=claim_path,
        owner_id=owner_id,
        expected_block=expected_block,
        expected_claim=expected_claim,
        shim_path=shim_path,
        expected_shim=expected_shim,
        ingest_socket_path=ingest_socket_path,
        python_executable=interpreter,
    )


def recover_pager_config(setup: PagerStatusSetup) -> str | None:
    """Recover an interrupted config update before any Grok process starts."""

    if not setup.enabled:
        return None
    return _recover_config_transaction(setup)


def stage_pager_status(setup: PagerStatusSetup) -> str | None:
    """Publish one marker-owned status-line block in the isolated seat config."""

    if not setup.enabled:
        return setup.reason
    recovery_error = _recover_config_transaction(setup)
    if recovery_error is not None:
        return recovery_error
    if setup.shim_path is None:
        return "pager status command shim path is absent"
    shim = setup.shim_path
    if shim.exists() or shim.is_symlink():
        current_shim = _read_regular(
            shim,
            limit=MAX_CONFIG_BYTES,
            require_private=True,
            required_mode=0o700,
        )
        if current_shim is None or not hmac.compare_digest(
            current_shim, setup.expected_shim
        ):
            return "pager status command shim changed before staging"
    else:
        try:
            if not _atomic_create(shim, setup.expected_shim, mode=0o700):
                return "pager status command shim appeared during staging"
        except OSError as exc:
            return f"pager status command shim could not be staged: {exc}"
    path = setup.config_path
    if path.exists() or path.is_symlink():
        current = _read_regular(
            path,
            limit=MAX_CONFIG_BYTES,
            require_owned=True,
            require_single_link=True,
        )
        if current is None:
            return "config.toml became unsafe before pager staging"
        if not current.strip():
            return "pager status refuses an empty existing config.toml"
        if setup.python_executable is None or setup.ingest_socket_path is None:
            return "pager status command identity is incomplete"
        if _semantic_status_config(
            current,
            setup.python_executable,
            setup.shim_path,
            setup.ingest_socket_path,
        ):
            return None
        try:
            parsed = tomllib.loads(current.decode("utf-8"))
            proposed = current + setup.expected_block
            tomllib.loads(proposed.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            return "pager status block conflicts with config.toml"
        ui = parsed.get("ui")
        if isinstance(ui, dict) and "status_line" in ui:
            return "config.toml acquired a foreign status line before pager staging"
        features = parsed.get("features")
        if isinstance(features, dict) and features.get("managed_config") is True:
            return "config.toml enabled managed synchronization before pager staging"
        if MARKER_PREFIX.encode() in current:
            return "config.toml acquired an unowned pager marker before staging"
        return _transform_config(
            setup,
            original=current,
            intended=proposed,
            operation="inject",
        )
    try:
        if not _atomic_create(path, setup.expected_block):
            return "config.toml appeared during pager staging"
    except OSError as exc:
        return f"pager status configuration could not be staged: {exc}"
    return None


def _ownership_read_failure(
    label: str, read: _AdmissionFileRead
) -> PagerStatusRead:
    kind: ReadKind = "transient" if read.kind == "transient" else "unsafe"
    return PagerStatusRead(kind, f"pager status ownership {label} {read.reason}")


def _pager_ownership_read(setup: PagerStatusSetup) -> PagerStatusRead | None:
    if not setup.enabled:
        return PagerStatusRead("unsafe", setup.reason)
    claim_read = _read_admission_file(
        setup.owner_claim_path, limit=16_384, require_private=True
    )
    if claim_read.kind != "valid" or claim_read.raw is None:
        return _ownership_read_failure("claim", claim_read)
    if not hmac.compare_digest(claim_read.raw, setup.expected_claim):
        return PagerStatusRead("unsafe", "pager status ownership claim bytes drifted")
    if setup.shim_path is None:
        return PagerStatusRead("unsafe", "pager status ownership shim path is absent")
    shim_read = _read_admission_file(
        setup.shim_path,
        limit=MAX_CONFIG_BYTES,
        require_private=True,
        required_mode=0o700,
    )
    if shim_read.kind != "valid" or shim_read.raw is None:
        return _ownership_read_failure("shim", shim_read)
    if not hmac.compare_digest(shim_read.raw, setup.expected_shim):
        return PagerStatusRead("unsafe", "pager status ownership shim bytes drifted")
    config_read = _read_admission_file(
        setup.config_path,
        limit=MAX_CONFIG_BYTES,
        require_owned=True,
        require_single_link=True,
    )
    if (
        setup.python_executable is None
        or setup.ingest_socket_path is None
    ):
        return PagerStatusRead(
            "unsafe", "pager status ownership command identity is incomplete"
        )
    if config_read.kind != "valid" or config_read.raw is None:
        return _ownership_read_failure("config", config_read)
    if not _semantic_status_config(
        config_read.raw,
        setup.python_executable,
        setup.shim_path,
        setup.ingest_socket_path,
    ):
        return PagerStatusRead("unsafe", "pager status ownership config semantics drifted")
    return None


def pager_ownership_intact(setup: PagerStatusSetup) -> bool:
    return _pager_ownership_read(setup) is None


def _remove_owned_config_block(
    setup: PagerStatusSetup,
) -> tuple[bool, str | None]:
    recovery_error = _recover_config_transaction(setup)
    if recovery_error is not None:
        return False, str(setup.config_transaction_path)
    path = setup.config_path
    if not path.exists() and not path.is_symlink():
        return True, None
    current = _read_regular(
        path,
        limit=MAX_CONFIG_BYTES,
        require_owned=True,
        require_single_link=True,
    )
    if current is None:
        return False, str(path)
    cleaned = _without_owned_block(current, setup.expected_block)
    if cleaned is None:
        try:
            parsed = tomllib.loads(current.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            return False, str(path)
        ui = parsed.get("ui")
        if MARKER_PREFIX.encode() in current or (
            isinstance(ui, dict) and "status_line" in ui
        ):
            return False, str(path)
        return True, None
    if (
        setup.python_executable is None
        or setup.shim_path is None
        or setup.ingest_socket_path is None
        or not _owned_user_config(
            current,
            setup.expected_block,
            setup.owner_id,
            setup.python_executable,
            setup.shim_path,
            setup.ingest_socket_path,
        )
    ):
        return False, str(path)
    if not cleaned.strip():
        removed, retained = _detach_validated_file(
            path,
            limit=MAX_CONFIG_BYTES,
            validator=lambda raw: hmac.compare_digest(raw, current),
            require_private=False,
            require_owned=True,
            require_single_link=True,
        )
        return removed, retained or (None if removed else str(path))
    try:
        tomllib.loads(cleaned.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        return False, str(path)
    error = _transform_config(
        setup,
        original=current,
        intended=cleaned,
        operation="clean",
    )
    return error is None, None if error is None else str(path)


def cleanup_pager_status(setup: PagerStatusSetup) -> list[str]:
    """Remove only exact adapter-owned artifacts; retain any drifted file."""

    retained: list[str] = []
    if not setup.enabled:
        return retained
    config_removed, config_retained = _remove_owned_config_block(setup)
    if not config_removed:
        retained.append(config_retained or str(setup.config_path))

    status_removed, status_retained = _detach_validated_file(
        setup.status_path,
        limit=MAX_STATUS_INPUT_BYTES,
        validator=lambda raw: _decode_authenticated_record(raw, setup.token)
        is not None,
    )
    if not status_removed:
        retained.append(status_retained or str(setup.status_path))

    shim_removed = True
    if setup.shim_path is not None:
        shim_removed, shim_retained = _detach_validated_file(
            setup.shim_path,
            limit=MAX_CONFIG_BYTES,
            validator=lambda raw: hmac.compare_digest(raw, setup.expected_shim),
            required_mode=0o700,
        )
        if not shim_removed:
            retained.append(shim_retained or str(setup.shim_path))

    # Keep the ownership claim whenever another adapter artifact drifted. That
    # prevents a later run from treating the still-present artifact as an
    # unclaimed slot and makes the retained state diagnosable/retryable.
    if config_removed and shim_removed:
        claim_removed, claim_retained = _detach_validated_file(
            setup.owner_claim_path,
            limit=16_384,
            validator=lambda raw: hmac.compare_digest(raw, setup.expected_claim),
        )
        if not claim_removed:
            retained.append(claim_retained or str(setup.owner_claim_path))
    else:
        retained.append(str(setup.owner_claim_path))
    return retained


def _publish_invalidation(
    setup: PagerStatusSetup,
    *,
    tui_pid: int,
    reason: str,
    captured_monotonic_ns: int,
    retained_state: Mapping[str, Any] | None = None,
) -> PagerIngestResult:
    record = {
        "schema": PAGER_RECORD_SCHEMA,
        "invalidated": True,
        "reason": reason[:300],
        "tui_pid": tui_pid,
        "captured_monotonic_ns": captured_monotonic_ns,
        "captured_wall_ns": time.time_ns(),
    }
    if retained_state is not None:
        record.update(retained_state)
    try:
        _atomic_replace(
            setup.status_path,
            _encode_authenticated_record(record, setup.token),
        )
    except OSError:
        pass
    retained_ns = (
        retained_state.get("state_observed_monotonic_ns")
        if retained_state is not None
        else None
    )
    return PagerIngestResult(
        "invalidated",
        reason[:300],
        retained_ns if type(retained_ns) is int else None,
        captured_monotonic_ns,
    )


def _retained_state_proof(record: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Extract only a complete authenticated state proof from a prior record."""

    if record is None:
        return None
    session_id = record.get("state_session_id")
    cwd = record.get("state_cwd")
    observed_ns = record.get("state_observed_monotonic_ns")
    schema_version = record.get("status_schema_version")
    version = record.get("grok_version")
    tui_pid = record.get("tui_pid")
    if not (
        isinstance(session_id, str)
        and isinstance(cwd, str)
        and cwd
        and type(observed_ns) is int
        and observed_ns > 0
        and type(schema_version) is int
        and isinstance(version, str)
        and version
        and type(tui_pid) is int
        and tui_pid > 0
    ):
        return None
    return {
        "state_session_id": session_id,
        "state_cwd": cwd,
        "state_observed_monotonic_ns": observed_ns,
        "status_schema_version": schema_version,
        "grok_version": version,
        "tui_pid": tui_pid,
    }


def record_authenticated_pager_payload(
    setup: PagerStatusSetup,
    raw: bytes,
    *,
    tui_pid: int,
) -> PagerIngestResult:
    """Record bytes received from a kernel-authenticated recorder process."""

    captured_monotonic_ns = time.monotonic_ns()
    previous_raw = _read_regular(
        setup.status_path,
        limit=MAX_STATUS_INPUT_BYTES,
        require_private=True,
    )
    previous = (
        _decode_authenticated_record(previous_raw, setup.token)
        if previous_raw is not None
        else None
    )
    retained_state = _retained_state_proof(previous)

    def invalidate(reason: str) -> PagerIngestResult:
        return _publish_invalidation(
            setup,
            tui_pid=tui_pid,
            reason=reason,
            captured_monotonic_ns=captured_monotonic_ns,
            retained_state=retained_state,
        )

    if not setup.enabled or len(raw) > MAX_STATUS_INPUT_BYTES:
        return invalidate("trusted pager payload is unavailable or oversized")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return invalidate("trusted pager payload is not valid JSON")
    if not isinstance(payload, dict):
        return invalidate("trusted pager payload is not an object")
    schema_version = payload.get("schema_version")
    session_text = payload.get("session_id")
    cwd = payload.get("cwd")
    version = payload.get("version")
    trigger = payload.get("trigger")
    if type(schema_version) is not int or schema_version != STATUS_PAYLOAD_SCHEMA:
        return invalidate("trusted pager payload schema is unsupported")
    try:
        valid_session = (
            isinstance(session_text, str)
            and str(uuid.UUID(session_text)) == session_text.lower()
        )
    except ValueError:
        valid_session = False
    if not valid_session:
        return invalidate("trusted pager payload session_id is invalid")
    assert isinstance(session_text, str)
    session_id = session_text.lower()
    if not isinstance(cwd, str) or not cwd:
        return invalidate("trusted pager payload cwd is invalid")
    if not isinstance(version, str) or not version:
        return invalidate("trusted pager payload version is invalid")
    if trigger not in {"state", "refresh_interval"}:
        return invalidate("trusted pager payload trigger is invalid")

    if trigger == "state":
        state_session_id = session_id
        state_cwd = cwd
        state_observed_monotonic_ns = captured_monotonic_ns
    else:
        if retained_state is None:
            return invalidate("refresh has no prior state-proven focus")
        state_session_id = retained_state["state_session_id"]
        state_cwd = retained_state["state_cwd"]
        state_observed_monotonic_ns = retained_state[
            "state_observed_monotonic_ns"
        ]
        if (
            state_session_id != session_id
            or state_cwd != cwd
            or retained_state["tui_pid"] != tui_pid
            or retained_state["grok_version"] != version
            or retained_state["status_schema_version"] != schema_version
            or type(state_observed_monotonic_ns) is not int
            or state_observed_monotonic_ns <= 0
        ):
            return invalidate("refresh disagrees with the last state-proven focus")

    record = {
        "schema": PAGER_RECORD_SCHEMA,
        "session_id": session_id,
        "cwd": cwd,
        "status_schema_version": schema_version,
        "grok_version": version,
        "trigger": trigger,
        "tui_pid": tui_pid,
        "captured_monotonic_ns": captured_monotonic_ns,
        "captured_wall_ns": time.time_ns(),
        "state_session_id": state_session_id,
        "state_cwd": state_cwd,
        "state_observed_monotonic_ns": state_observed_monotonic_ns,
    }
    try:
        _atomic_replace(
            setup.status_path,
            _encode_authenticated_record(record, setup.token),
        )
    except OSError:
        return invalidate("trusted pager focus could not be recorded")
    return PagerIngestResult(
        "recorded",
        "trusted pager focus recorded",
        state_observed_monotonic_ns,
        captured_monotonic_ns,
    )


def _submit_pager_status(socket_path_text: str) -> int:
    """Silent status client: no secret and no writable target are supplied."""

    try:
        raw = sys.stdin.buffer.read(MAX_STATUS_INPUT_BYTES + 1)
        if len(raw) > MAX_STATUS_INPUT_BYTES:
            return 0
        target = Path(socket_path_text).expanduser()
        if (
            not target.is_absolute()
            or len(os.fsencode(target)) >= 100
            or not _is_real_owned_directory(target.parent)
        ):
            return 0
        kind = socket.SOCK_STREAM | getattr(socket, "SOCK_CLOEXEC", 0)
        client = socket.socket(socket.AF_UNIX, kind)
        try:
            client.settimeout(0.5)
            client.connect(str(target))
            client.sendall(raw)
            client.shutdown(socket.SHUT_WR)
            client.recv(1)
        finally:
            client.close()
    except (OSError, RuntimeError, ValueError):
        return 0
    return 0


def read_pager_status(
    setup: PagerStatusSetup,
    *,
    tui_pid: int,
    agent_version: str,
    minimum_monotonic_ns: int | None = None,
    minimum_state_monotonic_ns: int | None = None,
    now_monotonic_ns: int | None = None,
) -> PagerStatusRead:
    ownership_problem = _pager_ownership_read(setup)
    if ownership_problem is not None:
        return ownership_problem
    first_read = _read_admission_file(
        setup.status_path,
        limit=MAX_STATUS_INPUT_BYTES,
        require_private=True,
    )
    if first_read.kind != "valid" or first_read.raw is None:
        kind: ReadKind = "unsafe" if first_read.kind == "unsafe" else "transient"
        return PagerStatusRead(kind, f"pager status sample {first_read.reason}")
    second_read = _read_admission_file(
        setup.status_path,
        limit=MAX_STATUS_INPUT_BYTES,
        require_private=True,
    )
    if second_read.kind == "unsafe":
        return PagerStatusRead("unsafe", f"pager status sample {second_read.reason}")
    if (
        second_read.kind != "valid"
        or second_read.raw is None
        or not hmac.compare_digest(first_read.raw, second_read.raw)
    ):
        return PagerStatusRead("transient", "pager status sample changed during read")
    value = _decode_authenticated_record(first_read.raw, setup.token)
    if value is None:
        return PagerStatusRead(
            "unsafe", "pager status sample authentication failed"
        )
    if value.get("schema") != PAGER_RECORD_SCHEMA:
        return PagerStatusRead("unsafe", "pager status record schema is unsupported")
    if value.get("invalidated") is True:
        reason = value.get("reason")
        detail = reason if isinstance(reason, str) else "trusted pager focus was invalidated"
        return PagerStatusRead("transient", detail[:300])
    session_id = value.get("session_id")
    cwd = value.get("cwd")
    schema_version = value.get("status_schema_version")
    version = value.get("grok_version")
    trigger = value.get("trigger")
    parent = value.get("tui_pid")
    monotonic_ns = value.get("captured_monotonic_ns")
    wall_ns = value.get("captured_wall_ns")
    state_session_id = value.get("state_session_id")
    state_cwd = value.get("state_cwd")
    state_monotonic_ns = value.get("state_observed_monotonic_ns")
    try:
        valid_uuid = (
            isinstance(session_id, str)
            and str(uuid.UUID(session_id)) == session_id.lower()
        )
    except ValueError:
        valid_uuid = False
    if not valid_uuid:
        return PagerStatusRead("unsafe", "pager status session_id is invalid")
    if not isinstance(cwd, str) or not cwd:
        return PagerStatusRead("unsafe", "pager status cwd is invalid")
    if type(schema_version) is not int or schema_version != STATUS_PAYLOAD_SCHEMA:
        return PagerStatusRead("unsafe", "pager status payload schema is unsupported")
    if version != agent_version:
        return PagerStatusRead("unsafe", "pager status Grok version disagrees with ACP")
    if trigger not in {"state", "refresh_interval"}:
        return PagerStatusRead("unsafe", "pager status trigger is invalid")
    if state_session_id != session_id or state_cwd != cwd:
        return PagerStatusRead(
            "unsafe", "pager status identity lacks a matching state-event proof"
        )
    if type(parent) is not int or parent != tui_pid:
        return PagerStatusRead("unsafe", "pager status writer is not the owned TUI child")
    if type(monotonic_ns) is not int or monotonic_ns <= 0:
        return PagerStatusRead("unsafe", "pager status monotonic timestamp is invalid")
    if type(wall_ns) is not int or wall_ns <= 0:
        return PagerStatusRead("unsafe", "pager status wall timestamp is invalid")
    if (
        type(state_monotonic_ns) is not int
        or state_monotonic_ns <= 0
        or state_monotonic_ns > monotonic_ns
        or trigger == "state"
        and state_monotonic_ns != monotonic_ns
    ):
        return PagerStatusRead(
            "unsafe", "pager status state-event timestamp is invalid"
        )
    now = time.monotonic_ns() if now_monotonic_ns is None else now_monotonic_ns
    if monotonic_ns > now + 1_000_000_000:
        return PagerStatusRead("unsafe", "pager status timestamp is in the future")
    if now - monotonic_ns > MAX_STATUS_AGE_NS:
        return PagerStatusRead("transient", "pager status sample is stale")
    if minimum_monotonic_ns is not None and monotonic_ns < minimum_monotonic_ns:
        return PagerStatusRead("transient", "pager status sample predates delivery gate")
    if (
        minimum_state_monotonic_ns is not None
        and state_monotonic_ns < minimum_state_monotonic_ns
    ):
        return PagerStatusRead("unsafe", "pager status state proof was replayed")
    sample = PagerStatusSample(
        session_id=str(session_id),
        cwd=cwd,
        status_schema_version=schema_version,
        grok_version=str(version),
        trigger=str(trigger),
        tui_pid=parent,
        captured_monotonic_ns=monotonic_ns,
        captured_wall_ns=wall_ns,
        state_session_id=str(state_session_id),
        state_cwd=str(state_cwd),
        state_observed_monotonic_ns=state_monotonic_ns,
    )
    return PagerStatusRead("valid", "pager status sample is valid", sample)


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if (
        len(args) == 3
        and args[0] == "--submit-pager-status"
        and args[1] == "--socket-path"
    ):
        return _submit_pager_status(args[2])
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
