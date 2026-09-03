"""Private, room-scoped registry for adapter-managed Grok seats."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Iterator


REGISTRY_SCHEMA = 1
SEAT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


def normalize_seat(value: str) -> str:
    seat = value.strip().removeprefix("@")
    if not SEAT_RE.fullmatch(seat):
        raise ValueError(f"invalid Grok seat name: {value!r}")
    return seat


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    _private_dir(path.parent)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    old_umask = os.umask(0o077)
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        with contextlib.suppress(OSError):
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        os.umask(old_umask)
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


class SeatRegistry:
    """Map one HCOM database to independently owned Grok seat state."""

    def __init__(self, hcom_db: Path) -> None:
        self.hcom_db = hcom_db.expanduser().resolve()
        room_id = hashlib.sha256(str(self.hcom_db).encode("utf-8")).hexdigest()[:16]
        home = Path.home()
        state_base = Path(
            os.environ.get(
                "HCOM_GROK_MANAGED_STATE_ROOT",
                str(home / ".local/state/hcom-grok/rooms"),
            )
        ).expanduser().resolve()
        log_base = Path(
            os.environ.get(
                "HCOM_GROK_LOG_ROOT",
                str(home / "Library/Logs/hcom-grok"),
            )
        ).expanduser().resolve()
        self.root = state_base / room_id
        self.seats_root = self.root / "seats"
        self.logs_root = log_base / "rooms" / room_id
        self.index_path = self.root / "registry.json"
        self.lock_path = self.root / "registry.lock"

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        _private_dir(self.root)
        old_umask = os.umask(0o077)
        try:
            handle = self.lock_path.open("a+", encoding="utf-8")
        finally:
            os.umask(old_umask)
        os.chmod(self.lock_path, 0o600)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def _empty(self) -> dict[str, Any]:
        return {
            "schema": REGISTRY_SCHEMA,
            "hcom_db": str(self.hcom_db),
            "latest_seat": None,
            "seats": {},
            "updated_ns": time.time_ns(),
        }

    def _load_unlocked(self) -> dict[str, Any]:
        value = _read_json(self.index_path)
        if value is None:
            return self._empty()
        if value.get("schema") != REGISTRY_SCHEMA:
            raise RuntimeError(f"unsupported HCOM Grok registry schema: {self.index_path}")
        if value.get("hcom_db") != str(self.hcom_db):
            raise RuntimeError(f"Grok registry belongs to another HCOM database: {self.index_path}")
        if not isinstance(value.get("seats"), dict):
            raise RuntimeError(f"Grok registry has invalid seats map: {self.index_path}")
        return value

    def _write_unlocked(self, value: dict[str, Any]) -> None:
        value["updated_ns"] = time.time_ns()
        _atomic_json(self.index_path, value)

    def _legacy_record(self) -> tuple[str, dict[str, Any]] | None:
        legacy = Path.home() / ".local/state/hcom-grok/current"
        run = _read_json(legacy / "run.json") or {}
        session = _read_json(legacy / "session.json") or {}
        cursor = _read_json(legacy / "cursor.json") or {}
        if not run and not session and not cursor:
            return None
        cursor_db = cursor.get("db_path")
        default_db = (Path.home() / ".hcom/hcom.db").resolve()
        if isinstance(cursor_db, str) and cursor_db:
            if Path(cursor_db).expanduser().resolve() != self.hcom_db:
                return None
        elif self.hcom_db != default_db:
            return None
        raw_seat = run.get("seat", "gsea")
        if not isinstance(raw_seat, str):
            raw_seat = "gsea"
        seat = normalize_seat(raw_seat)
        raw_project = session.get("project") or run.get("project") or str(Path.cwd())
        project = str(Path(str(raw_project)).expanduser().resolve())
        now = time.time_ns()
        return seat, {
            "state_root": str(legacy.resolve()),
            "log_root": str(
                Path(
                    os.environ.get(
                        "HCOM_GROK_LOG_DIR",
                        str(Path.home() / "Library/Logs/hcom-grok"),
                    )
                ).expanduser().resolve()
            ),
            "project": project,
            "legacy": True,
            "created_ns": int(run.get("started_ns", now))
            if type(run.get("started_ns")) is int
            else now,
            "updated_ns": int(run.get("updated_ns", now))
            if type(run.get("updated_ns")) is int
            else now,
        }

    def ensure_legacy(self) -> None:
        candidate = self._legacy_record()
        if candidate is None:
            return
        seat, record = candidate
        with self._locked():
            value = self._load_unlocked()
            seats = value["seats"]
            if seat in seats:
                return
            seats[seat] = record
            if not value.get("latest_seat"):
                value["latest_seat"] = seat
            self._write_unlocked(value)

    def register(self, seat: str, project: Path) -> dict[str, Any]:
        seat = normalize_seat(seat)
        now = time.time_ns()
        record = {
            "state_root": str((self.seats_root / seat).resolve()),
            "log_root": str((self.logs_root / seat).resolve()),
            "project": str(project.expanduser().resolve()),
            "legacy": False,
            "created_ns": now,
            "updated_ns": now,
        }
        with self._locked():
            value = self._load_unlocked()
            seats = value["seats"]
            existing = seats.get(seat)
            if existing is not None and existing != record:
                raise RuntimeError(f"Grok seat already exists in this room: {seat}")
            seats[seat] = record
            value["latest_seat"] = seat
            self._write_unlocked(value)
        return {"name": seat, **record}

    def remove(self, seat: str) -> None:
        seat = normalize_seat(seat)
        with self._locked():
            value = self._load_unlocked()
            record = value["seats"].get(seat)
            if isinstance(record, dict) and record.get("legacy") is True:
                return
            value["seats"].pop(seat, None)
            if value.get("latest_seat") == seat:
                remaining = value["seats"]
                value["latest_seat"] = max(
                    remaining,
                    key=lambda name: int(remaining[name].get("updated_ns", 0)),
                    default=None,
                )
            self._write_unlocked(value)

    def touch(self, seat: str, *, project: Path | None = None) -> None:
        seat = normalize_seat(seat)
        with self._locked():
            value = self._load_unlocked()
            record = value["seats"].get(seat)
            if not isinstance(record, dict):
                raise RuntimeError(f"Unknown Grok seat in this room: {seat}")
            record["updated_ns"] = time.time_ns()
            if project is not None:
                record["project"] = str(project.expanduser().resolve())
            value["latest_seat"] = seat
            self._write_unlocked(value)

    def get(self, seat: str | None = None) -> dict[str, Any]:
        self.ensure_legacy()
        with self._locked():
            value = self._load_unlocked()
            seats = value["seats"]
            selected = normalize_seat(seat) if seat else value.get("latest_seat")
            if not isinstance(selected, str) or selected not in seats:
                raise RuntimeError(
                    "No resumable Grok seat in this HCOM room. Run hcom-grok to start fresh."
                )
            record = seats[selected]
            if not isinstance(record, dict):
                raise RuntimeError(f"Grok registry entry is invalid: {selected}")
            return {"name": selected, **record}

    def get_resumable(self, seat: str | None = None) -> dict[str, Any]:
        """Select an exact seat or the newest seat with a saved Grok session."""
        if seat is not None:
            return self.get(seat)
        self.ensure_legacy()
        with self._locked():
            value = self._load_unlocked()
            candidates = sorted(
                (
                    (name, record)
                    for name, record in value["seats"].items()
                    if isinstance(record, dict)
                ),
                key=lambda item: int(item[1].get("updated_ns", 0)),
                reverse=True,
            )
            for name, record in candidates:
                state_root = Path(str(record.get("state_root", ""))).expanduser()
                session = _read_json(state_root / "session.json") or {}
                if isinstance(session.get("session_id"), str) and session.get("session_id"):
                    return {"name": name, **record}
        raise RuntimeError(
            "No resumable Grok seat in this HCOM room. Run hcom-grok to start fresh."
        )

    def list(self) -> list[dict[str, Any]]:
        self.ensure_legacy()
        with self._locked():
            value = self._load_unlocked()
            latest = value.get("latest_seat")
            records = [
                {"name": name, "latest": name == latest, **record}
                for name, record in value["seats"].items()
                if isinstance(record, dict)
            ]
        records.sort(key=lambda item: int(item.get("updated_ns", 0)), reverse=True)
        return records
