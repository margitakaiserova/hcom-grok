"""Read and classify immutable HCOM event envelopes.

This module never mutates the HCOM database.  The bridge's only authority here
is to inspect ordered events and preserve their exact routing metadata.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote


SUPPORTED_HCOM_SCHEMA = 18
DATABASE_UUID_KEY = "hcom:database_uuid"


class HcomReadError(RuntimeError):
    """Raised when HCOM state cannot be read without guessing."""


class HcomSchemaError(HcomReadError):
    """Raised for an unsupported or incomplete HCOM schema."""


@dataclass(frozen=True)
class HcomIdentity:
    db_uuid: str
    canonical_path: str
    schema_version: int
    device: int
    inode: int
    anchor_event_id: int
    anchor_sha256: str


@dataclass(frozen=True)
class EventRow:
    event_id: int
    timestamp: str
    event_type: str
    instance: str
    data: str

    @property
    def sha256(self) -> str:
        digest = hashlib.sha256()
        for value in (
            str(self.event_id),
            self.timestamp,
            self.event_type,
            self.instance,
            self.data,
        ):
            raw = value.encode("utf-8", "surrogatepass")
            digest.update(len(raw).to_bytes(8, "big"))
            digest.update(raw)
        return digest.hexdigest()


Disposition = Literal["irrelevant", "own", "ack", "request", "inform", "quarantine"]


@dataclass(frozen=True)
class Envelope:
    event: EventRow
    sender: str
    intent: str
    text: str
    reply_ref: str
    original_reply_to: str | None
    thread: str | None
    bundle_id: str | None
    scope: str | None
    delivered_to: tuple[str, ...]
    mentions: tuple[str, ...]
    sender_kind: str | None
    origin: str | None
    relay_event_id: str | None
    relay_device: str | None
    relay_short: str | None
    relay_reset_generation: str | None
    raw_object: dict[str, Any]


@dataclass(frozen=True)
class ClassifiedEvent:
    disposition: Disposition
    envelope: Envelope | None
    reason: str = ""


def base_name(value: str) -> str:
    return value.split(":", 1)[0]


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _relay_reference(
    obj: dict[str, Any], local_event_id: int
) -> tuple[str, str | None, str | None, str | None, str | None]:
    if "_relay" not in obj:
        return str(local_event_id), None, None, None, None
    relay = obj["_relay"]
    if not isinstance(relay, dict):
        raise ValueError("relay metadata is not an object")
    remote_id = relay.get("id")
    short = relay.get("short")
    device = relay.get("device") or relay.get("device_id")
    reset = relay.get("reset") or relay.get("db_uuid")
    if not isinstance(remote_id, int) or isinstance(remote_id, bool) or remote_id <= 0:
        raise ValueError("relay metadata lacks a positive remote event ID")
    if not isinstance(short, str) or not short:
        raise ValueError("relay metadata lacks a device short ID")
    if not isinstance(device, str) or not device:
        raise ValueError("relay metadata lacks a stable device ID")
    if not isinstance(reset, (str, int)) or isinstance(reset, bool) or not str(reset):
        raise ValueError("relay metadata lacks a reset-generation identity")
    remote_text = str(remote_id)
    return f"{remote_text}:{short}", remote_text, device, short, str(reset)


def classify_event(row: EventRow, seat: str) -> ClassifiedEvent:
    if row.event_type != "message":
        return ClassifiedEvent("irrelevant", None, "not a message")
    try:
        obj = json.loads(row.data)
    except json.JSONDecodeError as exc:
        return ClassifiedEvent("quarantine", None, f"invalid message JSON: {exc}")
    if not isinstance(obj, dict):
        return ClassifiedEvent("quarantine", None, "message data is not an object")

    sender = obj.get("from")
    if not isinstance(sender, str) or not sender:
        return ClassifiedEvent("quarantine", None, "missing sender")
    if sender == seat:
        return ClassifiedEvent("own", None, "outbound from this seat")

    mentions = _string_tuple(obj.get("mentions"))
    delivered_to = _string_tuple(obj.get("delivered_to"))
    scope = _optional_string(obj.get("scope"))
    addressed = scope == "broadcast" or any(
        base_name(item) == base_name(seat) for item in (*mentions, *delivered_to)
    )
    if not addressed:
        return ClassifiedEvent("irrelevant", None, "not delivered to this seat")

    intent_value = obj.get("intent", "inform")
    if not isinstance(intent_value, str) or intent_value not in {"request", "inform", "ack"}:
        return ClassifiedEvent("quarantine", None, "unsupported intent")
    text = obj.get("text")
    if not isinstance(text, str) or not text.strip():
        return ClassifiedEvent("quarantine", None, "missing message body")

    remote_shaped = (
        ":" in sender
        or ":" in row.instance
        or _optional_string(obj.get("origin") or obj.get("origin_device_id")) is not None
    )
    if remote_shaped and "_relay" not in obj:
        return ClassifiedEvent(
            "quarantine", None, "remote-shaped message lacks relay metadata"
        )

    try:
        reply_ref, relay_event_id, relay_device, relay_short, relay_reset = _relay_reference(
            obj, row.event_id
        )
    except ValueError as exc:
        return ClassifiedEvent("quarantine", None, str(exc))
    envelope = Envelope(
        event=row,
        sender=sender,
        intent=intent_value,
        text=text,
        reply_ref=reply_ref,
        original_reply_to=_optional_string(obj.get("reply_to")),
        thread=_optional_string(obj.get("thread")),
        bundle_id=_optional_string(obj.get("bundle_id") or obj.get("bundle")),
        scope=scope,
        delivered_to=delivered_to,
        mentions=mentions,
        sender_kind=_optional_string(obj.get("sender_kind")),
        origin=_optional_string(obj.get("origin") or obj.get("origin_device_id")),
        relay_event_id=relay_event_id,
        relay_device=relay_device,
        relay_short=relay_short,
        relay_reset_generation=relay_reset,
        raw_object=obj,
    )
    return ClassifiedEvent(intent_value, envelope)


def prompt_text(envelope: Envelope, seat: str, delivery_id: str) -> str:
    metadata = {
        "delivery_id": delivery_id,
        "from": envelope.sender,
        "to": seat,
        "event_id": envelope.event.event_id,
        "intent": envelope.intent,
        "reply_ref": envelope.reply_ref,
        "original_reply_to": envelope.original_reply_to,
        "thread": envelope.thread,
        "bundle_id": envelope.bundle_id,
        "scope": envelope.scope,
        "origin": envelope.origin,
    }
    if envelope.intent == "request":
        contract = (
            "Complete the entire request and answer normally in your final response. "
            "The bridge returns that response to the sender as one HCOM inform, so do not send "
            "a duplicate reply through hcom. Keep the completion summary concise unless the sender "
            "requests detail. If the request explicitly asks you to initiate a separate HCOM message, "
            f"you may run hcom send --name {seat} for that separate message using the supplied routing "
            "details; it does not replace your normal final response."
        )
    else:
        contract = (
            "Treat this as information. Do not acknowledge it automatically. "
            f"Reply only when useful by running hcom send --name {seat} with the "
            "sender, thread, and reply reference shown above."
        )
    return (
        "[HCOM DELIVERY]\n"
        + json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        + "\n\n"
        + contract
        + "\n\nMESSAGE BODY\n"
        + envelope.text
    )


class HcomReader:
    """Schema-guarded, read-only access to an HCOM event database."""

    REQUIRED_EVENT_COLUMNS = {"id", "timestamp", "type", "instance", "data"}

    def __init__(self, db_path: Path, expected_schema: int = SUPPORTED_HCOM_SCHEMA) -> None:
        self.db_path = db_path.expanduser().resolve()
        self.expected_schema = expected_schema

    def _connect(self) -> sqlite3.Connection:
        if not self.db_path.is_file():
            raise HcomReadError(f"HCOM database does not exist: {self.db_path}")
        uri = "file:" + quote(str(self.db_path), safe="/") + "?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True, timeout=5)
        except sqlite3.Error as exc:
            raise HcomReadError(f"cannot open HCOM database: {exc}") from exc
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=5000")
        return con

    def _validate(self, con: sqlite3.Connection) -> None:
        schema = int(con.execute("PRAGMA user_version").fetchone()[0])
        if schema != self.expected_schema:
            raise HcomSchemaError(
                f"unsupported HCOM schema {schema}; expected {self.expected_schema}"
            )
        event_columns = {
            str(row[1]) for row in con.execute("PRAGMA table_info(events)").fetchall()
        }
        missing = self.REQUIRED_EVENT_COLUMNS - event_columns
        if missing:
            raise HcomSchemaError(f"HCOM events table missing columns: {sorted(missing)}")
        tables = {
            str(row[0])
            for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if not {"instances", "kv"}.issubset(tables):
            raise HcomSchemaError("HCOM instances or kv table is missing")

    def identity(self) -> HcomIdentity:
        stat = os.stat(self.db_path, follow_symlinks=False)
        with closing(self._connect()) as con:
            self._validate(con)
            row = con.execute("SELECT value FROM kv WHERE key=?", (DATABASE_UUID_KEY,)).fetchone()
            if row is None or not isinstance(row[0], str) or not row[0]:
                raise HcomReadError(
                    f"HCOM database UUID key {DATABASE_UUID_KEY!r} is not initialized"
                )
            anchor_row = con.execute(
                "SELECT id, timestamp, type, instance, data FROM events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            schema = int(con.execute("PRAGMA user_version").fetchone()[0])
        if anchor_row is None:
            anchor_id = 0
            anchor_sha = hashlib.sha256(b"empty-hcom-events").hexdigest()
        else:
            event = EventRow(
                int(anchor_row["id"]),
                str(anchor_row["timestamp"]),
                str(anchor_row["type"]),
                str(anchor_row["instance"]),
                str(anchor_row["data"]),
            )
            anchor_id = event.event_id
            anchor_sha = event.sha256
        return HcomIdentity(
            db_uuid=str(row[0]),
            canonical_path=str(self.db_path),
            schema_version=schema,
            device=int(stat.st_dev),
            inode=int(stat.st_ino),
            anchor_event_id=anchor_id,
            anchor_sha256=anchor_sha,
        )

    def max_event_id(self) -> int:
        with closing(self._connect()) as con:
            self._validate(con)
            row = con.execute("SELECT COALESCE(MAX(id), 0) FROM events").fetchone()
            return int(row[0] or 0)

    def event_sequence(self) -> int:
        """Return the AUTOINCREMENT high-water mark, including deleted rows."""
        with closing(self._connect()) as con:
            self._validate(con)
            row = con.execute(
                "SELECT seq FROM sqlite_sequence WHERE name='events'"
            ).fetchone()
            return int(row[0] or 0) if row is not None else 0

    def validate_committed_cursor(
        self, event_id: int, expected_sha256: str | None
    ) -> tuple[bool, str]:
        sequence = self.event_sequence()
        if sequence < event_id:
            raise HcomReadError(
                f"HCOM event sequence {sequence} is below committed cursor {event_id}"
            )
        if event_id == 0:
            return True, "empty cursor"
        actual = self.event_digest(event_id)
        if actual is None:
            return False, "committed event was deleted but AUTOINCREMENT remains monotonic"
        if expected_sha256 is None or actual != expected_sha256:
            raise HcomReadError("committed HCOM event digest does not match durable state")
        return True, "committed event digest matches"

    def event_digest(self, event_id: int) -> str | None:
        with closing(self._connect()) as con:
            self._validate(con)
            row = con.execute(
                "SELECT id, timestamp, type, instance, data FROM events WHERE id=?",
                (int(event_id),),
            ).fetchone()
        if row is None:
            return None
        return EventRow(
            int(row["id"]),
            str(row["timestamp"]),
            str(row["type"]),
            str(row["instance"]),
            str(row["data"]),
        ).sha256

    def rows_after(self, event_id: int, limit: int = 256) -> list[EventRow]:
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        with closing(self._connect()) as con:
            self._validate(con)
            rows = con.execute(
                "SELECT id, timestamp, type, instance, data FROM events "
                "WHERE id>? ORDER BY id LIMIT ?",
                (int(event_id), int(limit)),
            ).fetchall()
        return [
            EventRow(
                int(row["id"]),
                str(row["timestamp"]),
                str(row["type"]),
                str(row["instance"]),
                str(row["data"]),
            )
            for row in rows
        ]
