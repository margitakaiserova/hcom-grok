"""Durable bridge journal with compare-and-swap recovery transitions."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .envelope import Envelope, HcomIdentity


JOURNAL_SCHEMA = 3
APPLICATION_ID = 0x48474231
PROMPT_NAMESPACE = uuid.UUID("5e82f103-214f-58bc-b5eb-37e0705df32d")
EPOCH_MODES = {"join-live", "drain-backlog", "adopt-new-db"}

DELIVERY_TRANSITIONS: dict[str, set[str]] = {
    "prepared": {"submitting", "blocked", "abandoned"},
    "reconciled_not_seen": {"submitting", "blocked", "abandoned"},
    "submitting": {"blocked", "abandoned"},
    "persisted": {"running", "completed", "blocked", "abandoned"},
    "running": {"completed", "blocked", "abandoned"},
    "completed": {"blocked", "abandoned"},
    "blocked": {"abandoned"},
    "finalized": set(),
    "abandoned": set(),
}

OUTBOX_TRANSITIONS: dict[str, set[str]] = {
    "prepared": {"sending"},
    "sending": {"sent", "ambiguous"},
    "ambiguous": {"reconciled_absent", "sent"},
    "reconciled_absent": {"sending"},
    "sent": set(),
}


class JournalError(RuntimeError):
    """Base class for failures that must stop ingress."""


class JournalIdentityError(JournalError):
    """Durable state does not match its source or immutable envelope."""


class JournalStateError(JournalError):
    """A state transition or cursor update was not legal."""


@dataclass(frozen=True)
class Epoch:
    epoch_id: int
    seat: str
    mode: str
    initial_cursor: int


@dataclass(frozen=True)
class Delivery:
    delivery_id: str
    source_key: str
    event_id: int
    state: str
    prompt_id: str
    rendered_prompt: str
    prompt_sha256: str
    grok_session_id: str
    intent: str
    sender: str
    reply_ref: str
    thread: str | None


@dataclass(frozen=True)
class Outbound:
    outbound_id: str
    delivery_id: str
    phase: str
    recipient: str
    intent: str
    state: str
    body: str
    idempotency_key: str
    envelope_sha256: str
    source_db_uuid: str
    hcom_event_id: int | None


def _now_ns() -> int:
    return time.time_ns()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def envelope_sha256(envelope: Envelope) -> str:
    return hashlib.sha256(
        _canonical_json(envelope.raw_object).encode("utf-8", "surrogatepass")
    ).hexdigest()


def source_identity(identity: HcomIdentity, envelope: Envelope) -> str:
    if envelope.relay_event_id is not None:
        if not envelope.relay_device or not envelope.relay_reset_generation:
            raise JournalIdentityError(
                "relayed delivery lacks device or reset-generation identity"
            )
        components = _canonical_json(
            {
                "device": envelope.relay_device,
                "event_id": envelope.relay_event_id,
                "reset_generation": envelope.relay_reset_generation,
            }
        ).encode("utf-8", "surrogatepass")
        return "relay:" + hashlib.sha256(components).hexdigest()
    return f"local:{identity.db_uuid}:{envelope.event.event_id}"


def delivery_identity(identity: HcomIdentity, seat: str, envelope: Envelope) -> str:
    key = source_identity(identity, envelope)
    raw = _canonical_json({"seat": seat, "source_key": key}).encode("utf-8")
    return "hgb1:" + hashlib.sha256(raw).hexdigest()


def stable_prompt_id(delivery_id: str) -> str:
    return str(uuid.uuid5(PROMPT_NAMESPACE, delivery_id))


class Journal:
    _REQUIRED_TABLES = {
        "meta",
        "epochs",
        "cursor",
        "deliveries",
        "prompt_attempts",
        "outbox",
        "outbox_attempts",
        "transitions",
        "processes",
    }
    _REQUIRED_INDEXES = {
        "one_active_epoch_per_seat",
        "one_unsettled_delivery",
        "one_outbox_receipt_per_hcom_event",
    }

    def __init__(self, path: Path) -> None:
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        previous_umask = os.umask(0o077)
        try:
            self.con = sqlite3.connect(str(self.path), timeout=5, isolation_level=None)
        except sqlite3.Error as exc:
            raise JournalError(f"cannot open bridge journal: {exc}") from exc
        finally:
            os.umask(previous_umask)
        try:
            self.con.row_factory = sqlite3.Row
            self.con.execute("PRAGMA busy_timeout=5000")
            self.con.execute("PRAGMA foreign_keys=ON")
            self._validate_file()
            existing = self._preflight_existing()
            os.chmod(self.path, 0o600)
            self.con.execute("PRAGMA journal_mode=WAL")
            self.con.execute("PRAGMA synchronous=FULL")
            self.con.execute("PRAGMA fullfsync=ON")
            if not existing:
                self._initialize_new()
            self._validate_schema()
            self._validate_file()
            self._harden_files()
        except BaseException:
            self.con.close()
            raise

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "Journal":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.con.execute("BEGIN IMMEDIATE")
        try:
            yield self.con
        except BaseException:
            if self.con.in_transaction:
                with suppress(sqlite3.Error):
                    self.con.execute("ROLLBACK")
            raise
        else:
            try:
                self.con.execute("COMMIT")
            except BaseException:
                if self.con.in_transaction:
                    with suppress(sqlite3.Error):
                        self.con.execute("ROLLBACK")
                raise

    def _validate_file(self) -> None:
        quick = self.con.execute("PRAGMA quick_check").fetchone()
        if quick is None or quick[0] != "ok":
            raise JournalError(f"journal quick_check failed: {quick}")
        foreign = self.con.execute("PRAGMA foreign_key_check").fetchall()
        if foreign:
            raise JournalError(f"journal foreign_key_check failed: {foreign[:5]}")

    def _schema_fingerprint(self) -> str:
        rows = self.con.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' "
            "ORDER BY type,name"
        ).fetchall()
        objects = [
            {
                "type": str(row[0]),
                "name": str(row[1]),
                "table": str(row[2]),
                "sql": " ".join(str(row[3] or "").split()),
            }
            for row in rows
        ]
        return hashlib.sha256(_canonical_json(objects).encode("utf-8")).hexdigest()

    def _preflight_existing(self) -> bool:
        """Reject foreign or provisional files before any persistent PRAGMA."""
        application_id = int(self.con.execute("PRAGMA application_id").fetchone()[0])
        user_version = int(self.con.execute("PRAGMA user_version").fetchone()[0])
        tables = {
            str(row[0])
            for row in self.con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
        if not tables:
            if application_id != 0 or user_version != 0:
                raise JournalIdentityError("empty journal has unexpected SQLite identity")
            return False
        if "meta" not in tables:
            raise JournalIdentityError("existing SQLite file is not an HCOM Grok journal")
        try:
            existing_schema = self.con.execute(
                "SELECT value FROM meta WHERE key='journal_schema'"
            ).fetchone()
        except sqlite3.Error as exc:
            raise JournalIdentityError("existing journal metadata is unreadable") from exc
        if existing_schema is None:
            raise JournalIdentityError("existing journal has no schema identity")
        try:
            schema = int(existing_schema[0])
        except (TypeError, ValueError) as exc:
            raise JournalIdentityError("existing journal schema identity is invalid") from exc
        if schema != JOURNAL_SCHEMA:
            raise JournalError(
                f"journal schema {schema} is provisional and cannot be opened "
                f"as schema {JOURNAL_SCHEMA}; archive it before creating production state"
            )
        if application_id != APPLICATION_ID:
            raise JournalIdentityError(
                f"unexpected SQLite application_id {application_id:#x}"
            )
        if user_version != JOURNAL_SCHEMA:
            raise JournalIdentityError(
                f"unexpected SQLite user_version {user_version}; expected {JOURNAL_SCHEMA}"
            )
        missing = self._REQUIRED_TABLES - tables
        if missing:
            raise JournalIdentityError(f"journal is missing tables: {sorted(missing)}")
        indexes = {
            str(row[0])
            for row in self.con.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
            ).fetchall()
        }
        missing_indexes = self._REQUIRED_INDEXES - indexes
        if missing_indexes:
            raise JournalIdentityError(
                f"journal is missing indexes: {sorted(missing_indexes)}"
            )
        fingerprint = self.con.execute(
            "SELECT value FROM meta WHERE key='schema_fingerprint'"
        ).fetchone()
        if (
            fingerprint is None
            or not _is_sha256(fingerprint[0])
            or str(fingerprint[0]) != self._schema_fingerprint()
        ):
            raise JournalIdentityError("journal schema fingerprint does not match")
        return True

    def _initialize_new(self) -> None:
        schema = f"""
            BEGIN IMMEDIATE;
            PRAGMA application_id={APPLICATION_ID};
            PRAGMA user_version={JOURNAL_SCHEMA};
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS epochs (
                epoch_id INTEGER PRIMARY KEY AUTOINCREMENT,
                db_uuid TEXT NOT NULL,
                canonical_hcom_db TEXT NOT NULL,
                hcom_schema INTEGER NOT NULL CHECK(hcom_schema>=0),
                db_device INTEGER NOT NULL,
                db_inode INTEGER NOT NULL,
                anchor_event_id INTEGER NOT NULL CHECK(anchor_event_id>=0),
                anchor_sha256 TEXT NOT NULL,
                seat TEXT NOT NULL,
                mode TEXT NOT NULL CHECK(mode IN ('join-live','drain-backlog','adopt-new-db')),
                initial_cursor INTEGER NOT NULL CHECK(initial_cursor>=0),
                created_ns INTEGER NOT NULL,
                active INTEGER NOT NULL CHECK(active IN (0,1))
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS one_active_epoch_per_seat
                ON epochs(seat) WHERE active=1;
            CREATE TABLE IF NOT EXISTS cursor (
                epoch_id INTEGER PRIMARY KEY REFERENCES epochs(epoch_id) ON DELETE CASCADE,
                committed_event_id INTEGER NOT NULL CHECK(committed_event_id>=0),
                committed_event_sha256 TEXT,
                hcom_synced_event_id INTEGER NOT NULL CHECK(hcom_synced_event_id>=-1),
                updated_ns INTEGER NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS deliveries (
                delivery_id TEXT PRIMARY KEY,
                seat TEXT NOT NULL,
                source_key TEXT NOT NULL,
                epoch_id INTEGER NOT NULL REFERENCES epochs(epoch_id),
                local_event_id INTEGER NOT NULL CHECK(local_event_id>=0),
                cursor_before_event_id INTEGER NOT NULL CHECK(cursor_before_event_id>=0),
                raw_event_sha256 TEXT NOT NULL,
                envelope_sha256 TEXT NOT NULL,
                raw_event_json TEXT NOT NULL,
                envelope_json TEXT NOT NULL CHECK(json_valid(envelope_json)),
                sender TEXT NOT NULL,
                intent TEXT NOT NULL CHECK(intent IN ('request','inform')),
                reply_ref TEXT NOT NULL,
                original_reply_to TEXT,
                thread TEXT,
                bundle_id TEXT,
                relay_event_id TEXT,
                relay_device TEXT,
                relay_short TEXT,
                relay_reset_generation TEXT,
                prompt_id TEXT NOT NULL UNIQUE,
                rendered_prompt TEXT NOT NULL,
                prompt_sha256 TEXT NOT NULL,
                formatter_version INTEGER NOT NULL CHECK(formatter_version>=1),
                grok_session_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN (
                    'prepared','reconciled_not_seen','submitting','persisted','running','completed',
                    'blocked','finalized','abandoned'
                )),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
                prepared_ns INTEGER NOT NULL,
                persisted_ns INTEGER,
                completed_ns INTEGER,
                finalized_ns INTEGER,
                completion_json TEXT CHECK(completion_json IS NULL OR json_valid(completion_json)),
                completion_sha256 TEXT,
                assistant_text TEXT,
                stop_reason TEXT,
                last_error TEXT,
                UNIQUE(epoch_id, local_event_id),
                UNIQUE(seat, source_key)
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS one_unsettled_delivery
                ON deliveries((1)) WHERE state NOT IN ('finalized','abandoned');
            CREATE TABLE IF NOT EXISTS prompt_attempts (
                attempt_id INTEGER PRIMARY KEY,
                delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
                attempt_no INTEGER NOT NULL CHECK(attempt_no>=1),
                grok_session_id TEXT NOT NULL,
                rpc_id TEXT,
                started_ns INTEGER NOT NULL,
                admission_state TEXT NOT NULL,
                result_json TEXT CHECK(result_json IS NULL OR json_valid(result_json)),
                ended_ns INTEGER,
                UNIQUE(delivery_id, attempt_no)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS outbox (
                outbound_id TEXT PRIMARY KEY,
                delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
                epoch_id INTEGER NOT NULL REFERENCES epochs(epoch_id),
                source_db_uuid TEXT NOT NULL,
                phase TEXT NOT NULL CHECK(phase IN ('ack','final','inform_response')),
                recipient TEXT NOT NULL,
                intent TEXT NOT NULL CHECK(intent IN ('ack','inform')),
                reply_ref TEXT NOT NULL,
                thread TEXT,
                exact_body TEXT NOT NULL,
                body_sha256 TEXT NOT NULL,
                envelope_sha256 TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN (
                    'prepared','sending','ambiguous','reconciled_absent','sent'
                )),
                hcom_event_id INTEGER CHECK(hcom_event_id IS NULL OR hcom_event_id>0),
                hcom_event_sha256 TEXT,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
                prepared_ns INTEGER NOT NULL,
                sent_ns INTEGER,
                last_error TEXT,
                UNIQUE(delivery_id, phase)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS outbox_attempts (
                attempt_id INTEGER PRIMARY KEY,
                outbound_id TEXT NOT NULL REFERENCES outbox(outbound_id),
                attempt_no INTEGER NOT NULL CHECK(attempt_no>=1),
                started_ns INTEGER NOT NULL,
                result_state TEXT NOT NULL,
                result_json TEXT CHECK(result_json IS NULL OR json_valid(result_json)),
                ended_ns INTEGER,
                UNIQUE(outbound_id, attempt_no)
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS one_outbox_receipt_per_hcom_event
                ON outbox(source_db_uuid, hcom_event_id)
                WHERE hcom_event_id IS NOT NULL;
            CREATE TABLE IF NOT EXISTS transitions (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_kind TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                from_state TEXT,
                to_state TEXT NOT NULL,
                timestamp_ns INTEGER NOT NULL,
                detail_json TEXT NOT NULL CHECK(json_valid(detail_json))
            ) STRICT;
            CREATE TABLE IF NOT EXISTS processes (
                run_token TEXT NOT NULL,
                role TEXT NOT NULL,
                uid INTEGER NOT NULL,
                pid INTEGER NOT NULL CHECK(pid>0),
                birth_identity TEXT NOT NULL,
                executable TEXT NOT NULL,
                argv_sha256 TEXT NOT NULL,
                socket_path TEXT,
                session_id TEXT,
                recorded_ns INTEGER NOT NULL,
                PRIMARY KEY(run_token, role)
            ) STRICT;
            INSERT INTO meta(key,value) VALUES('journal_schema','{JOURNAL_SCHEMA}');
            """
        try:
            self.con.executescript(schema)
            self.con.execute(
                "INSERT INTO meta(key,value) VALUES('schema_fingerprint',?)",
                (self._schema_fingerprint(),),
            )
            self.con.execute("COMMIT")
        except BaseException:
            if self.con.in_transaction:
                with suppress(sqlite3.Error):
                    self.con.execute("ROLLBACK")
            raise

    def _validate_schema(self) -> None:
        application_id = int(self.con.execute("PRAGMA application_id").fetchone()[0])
        user_version = int(self.con.execute("PRAGMA user_version").fetchone()[0])
        if application_id != APPLICATION_ID or user_version != JOURNAL_SCHEMA:
            raise JournalIdentityError("journal SQLite identity changed during initialization")
        row = self.con.execute("SELECT value FROM meta WHERE key='journal_schema'").fetchone()
        if row is None or int(row[0]) != JOURNAL_SCHEMA:
            raise JournalError(
                f"unsupported journal schema {None if row is None else row[0]}; "
                f"expected {JOURNAL_SCHEMA}"
            )
        fingerprint = self.con.execute(
            "SELECT value FROM meta WHERE key='schema_fingerprint'"
        ).fetchone()
        if fingerprint is None or str(fingerprint[0]) != self._schema_fingerprint():
            raise JournalIdentityError("journal schema fingerprint changed")

    def _harden_files(self) -> None:
        for candidate in (self.path, Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm")):
            if candidate.exists():
                os.chmod(candidate, 0o600)

    def create_epoch(
        self,
        identity: HcomIdentity,
        seat: str,
        mode: str,
        initial_cursor: int,
        initial_cursor_sha256: str | None = None,
    ) -> Epoch:
        if mode not in EPOCH_MODES:
            raise ValueError(f"invalid new epoch mode: {mode}")
        if type(initial_cursor) is not int or initial_cursor < 0:
            raise ValueError("initial cursor cannot be negative")
        if (
            initial_cursor_sha256 is None
            and initial_cursor > 0
            and initial_cursor == identity.anchor_event_id
        ):
            initial_cursor_sha256 = identity.anchor_sha256
        if initial_cursor > 0 and not _is_sha256(initial_cursor_sha256):
            raise JournalIdentityError("a nonzero initial cursor requires its event digest")
        if initial_cursor == 0 and initial_cursor_sha256 is not None:
            raise JournalIdentityError("event zero must not have an event digest")
        with self.transaction() as con:
            existing = con.execute(
                "SELECT epoch_id FROM epochs WHERE seat=? AND active=1", (seat,)
            ).fetchone()
            if existing is not None:
                raise JournalStateError(f"seat {seat} already has an active epoch")
            now = _now_ns()
            inserted = con.execute(
                "INSERT INTO epochs(db_uuid,canonical_hcom_db,hcom_schema,db_device,db_inode,"
                "anchor_event_id,anchor_sha256,seat,mode,initial_cursor,created_ns,active) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,1)",
                (
                    identity.db_uuid,
                    identity.canonical_path,
                    identity.schema_version,
                    identity.device,
                    identity.inode,
                    identity.anchor_event_id,
                    identity.anchor_sha256,
                    seat,
                    mode,
                    initial_cursor,
                    now,
                ),
            )
            epoch_id = int(inserted.lastrowid)
            con.execute(
                "INSERT INTO cursor(epoch_id,committed_event_id,committed_event_sha256,"
                "hcom_synced_event_id,updated_ns) VALUES(?,?,?,?,?)",
                (epoch_id, initial_cursor, initial_cursor_sha256, -1, now),
            )
        return Epoch(epoch_id, seat, mode, initial_cursor)

    def resume_epoch(
        self,
        identity: HcomIdentity,
        seat: str,
        *,
        event_sequence: int,
        committed_event_digest: str | None,
    ) -> Epoch:
        row = self.con.execute(
            "SELECT * FROM epochs WHERE seat=? AND active=1", (seat,)
        ).fetchone()
        if row is None:
            raise JournalIdentityError(f"no active journal epoch for {seat}")
        expected = {
            "db_uuid": identity.db_uuid,
            "canonical_hcom_db": identity.canonical_path,
            "hcom_schema": identity.schema_version,
        }
        mismatch = [key for key, value in expected.items() if row[key] != value]
        if mismatch:
            raise JournalIdentityError("HCOM identity mismatch: " + ", ".join(mismatch))
        cursor = self.con.execute(
            "SELECT committed_event_id,committed_event_sha256 FROM cursor WHERE epoch_id=?",
            (int(row["epoch_id"]),),
        ).fetchone()
        if cursor is None:
            raise JournalStateError("active epoch has no cursor")
        committed = int(cursor["committed_event_id"])
        expected_digest = cursor["committed_event_sha256"]
        if event_sequence < committed:
            raise JournalIdentityError(
                f"HCOM event sequence {event_sequence} is below committed cursor {committed}"
            )
        if committed > 0:
            if not _is_sha256(expected_digest) or not _is_sha256(committed_event_digest):
                raise JournalIdentityError("committed HCOM event digest is missing or invalid")
            if expected_digest != committed_event_digest:
                raise JournalIdentityError("committed HCOM event digest changed")
        elif expected_digest is not None or committed_event_digest is not None:
            raise JournalIdentityError("event zero must not have an event digest")
        return Epoch(int(row["epoch_id"]), seat, "resume", int(row["initial_cursor"]))

    def epoch_anchor(self, epoch_id: int) -> tuple[int, str]:
        row = self.con.execute(
            "SELECT anchor_event_id,anchor_sha256 FROM epochs WHERE epoch_id=?", (epoch_id,)
        ).fetchone()
        if row is None:
            raise JournalStateError(f"unknown epoch {epoch_id}")
        return int(row[0]), str(row[1])

    def cursor(self, epoch_id: int) -> tuple[int, int]:
        row = self.con.execute(
            "SELECT committed_event_id,hcom_synced_event_id FROM cursor WHERE epoch_id=?",
            (epoch_id,),
        ).fetchone()
        if row is None:
            raise JournalStateError(f"missing cursor for epoch {epoch_id}")
        return int(row[0]), int(row[1])

    def commit_scanned_event(
        self,
        epoch_id: int,
        expected_cursor: int,
        event_id: int,
        event_sha256_value: str | None,
    ) -> None:
        if type(epoch_id) is not int or type(expected_cursor) is not int:
            raise JournalIdentityError("cursor identifiers must be exact integers")
        if type(event_id) is not int or event_id < 0:
            raise JournalIdentityError("event ID must be a nonnegative exact integer")
        if event_id > 0 and not _is_sha256(event_sha256_value):
            raise JournalIdentityError("a positive event ID requires a SHA-256 digest")
        if event_id == 0 and event_sha256_value is not None:
            raise JournalIdentityError("event zero must not have an event digest")
        with self.transaction() as con:
            row = con.execute(
                "SELECT committed_event_id,committed_event_sha256 FROM cursor WHERE epoch_id=?",
                (epoch_id,),
            ).fetchone()
            if row is None:
                raise JournalStateError(f"missing cursor for epoch {epoch_id}")
            current, current_sha = int(row[0]), row[1]
            if current != expected_cursor:
                raise JournalStateError(
                    f"cursor compare-and-swap failed; expected={expected_cursor} actual={current}"
                )
            if event_id == current:
                if current_sha != event_sha256_value:
                    raise JournalIdentityError("equal cursor event has a different digest")
                return
            if event_id < current:
                raise JournalStateError(f"cursor regression {current} to {event_id}")
            unsettled = con.execute(
                "SELECT MIN(local_event_id) FROM deliveries "
                "WHERE state NOT IN ('finalized','abandoned')"
            ).fetchone()[0]
            if unsettled is not None and event_id >= int(unsettled):
                raise JournalStateError(
                    f"cursor cannot advance past unsettled delivery event {int(unsettled)}"
                )
            updated = con.execute(
                "UPDATE cursor SET committed_event_id=?,committed_event_sha256=?,updated_ns=? "
                "WHERE epoch_id=? AND committed_event_id=?",
                (event_id, event_sha256_value, _now_ns(), epoch_id, expected_cursor),
            )
            if updated.rowcount != 1:
                raise JournalStateError("cursor compare-and-swap update failed")

    def mark_hcom_synced(
        self, epoch_id: int, expected_synced: int, event_id: int
    ) -> None:
        if any(type(value) is not int for value in (epoch_id, expected_synced, event_id)):
            raise JournalIdentityError("HCOM cursor values must be exact integers")
        with self.transaction() as con:
            row = con.execute(
                "SELECT committed_event_id,hcom_synced_event_id FROM cursor WHERE epoch_id=?",
                (epoch_id,),
            ).fetchone()
            if row is None:
                raise JournalStateError(f"missing cursor for epoch {epoch_id}")
            committed, synced = int(row[0]), int(row[1])
            if synced != expected_synced:
                raise JournalStateError(
                    f"HCOM sync compare-and-swap failed; expected={expected_synced} actual={synced}"
                )
            if event_id < synced or event_id > committed:
                raise JournalStateError(
                    f"invalid HCOM cursor sync {event_id}; synced={synced} committed={committed}"
                )
            updated = con.execute(
                "UPDATE cursor SET hcom_synced_event_id=?,updated_ns=? "
                "WHERE epoch_id=? AND hcom_synced_event_id=?",
                (event_id, _now_ns(), epoch_id, expected_synced),
            )
            if updated.rowcount != 1:
                raise JournalStateError("HCOM sync compare-and-swap update failed")

    def prepare_delivery(
        self,
        epoch: Epoch,
        identity: HcomIdentity,
        envelope: Envelope,
        rendered_prompt: str,
        grok_session_id: str,
        formatter_version: int = 1,
    ) -> Delivery:
        source_key = source_identity(identity, envelope)
        delivery_id = delivery_identity(identity, epoch.seat, envelope)
        prompt_id = stable_prompt_id(delivery_id)
        envelope_json = _canonical_json(envelope.raw_object)
        envelope_digest = envelope_sha256(envelope)
        prompt_digest = hashlib.sha256(
            rendered_prompt.encode("utf-8", "surrogatepass")
        ).hexdigest()
        with self.transaction() as con:
            epoch_row = con.execute(
                "SELECT db_uuid,canonical_hcom_db,hcom_schema,seat,active FROM epochs "
                "WHERE epoch_id=?",
                (epoch.epoch_id,),
            ).fetchone()
            if epoch_row is None or int(epoch_row["active"]) != 1:
                raise JournalIdentityError("delivery epoch is missing or inactive")
            expected_epoch = {
                "db_uuid": identity.db_uuid,
                "canonical_hcom_db": identity.canonical_path,
                "hcom_schema": identity.schema_version,
                "seat": epoch.seat,
            }
            if any(epoch_row[key] != value for key, value in expected_epoch.items()):
                raise JournalIdentityError("delivery identity does not match its epoch")
            existing = con.execute(
                "SELECT * FROM deliveries WHERE seat=? AND source_key=?",
                (epoch.seat, source_key),
            ).fetchone()
            if existing is not None:
                if existing["envelope_sha256"] != envelope_digest:
                    raise JournalIdentityError("source identity has a different envelope")
                return self._delivery_from_row(existing)
            cursor_row = con.execute(
                "SELECT committed_event_id FROM cursor WHERE epoch_id=?", (epoch.epoch_id,)
            ).fetchone()
            if cursor_row is None:
                raise JournalStateError("delivery epoch has no cursor")
            current_cursor = int(cursor_row[0])
            if envelope.event.event_id <= current_cursor:
                raise JournalStateError(
                    f"delivery event {envelope.event.event_id} is not newer than cursor "
                    f"{current_cursor}"
                )
            try:
                con.execute(
                    "INSERT INTO deliveries(delivery_id,seat,source_key,epoch_id,local_event_id,"
                    "cursor_before_event_id,raw_event_sha256,envelope_sha256,raw_event_json,"
                    "envelope_json,sender,intent,reply_ref,original_reply_to,thread,bundle_id,"
                    "relay_event_id,relay_device,relay_short,relay_reset_generation,prompt_id,"
                    "rendered_prompt,prompt_sha256,formatter_version,grok_session_id,state,prepared_ns) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        delivery_id,
                        epoch.seat,
                        source_key,
                        epoch.epoch_id,
                        envelope.event.event_id,
                        current_cursor,
                        envelope.event.sha256,
                        envelope_digest,
                        envelope.event.data,
                        envelope_json,
                        envelope.sender,
                        envelope.intent,
                        envelope.reply_ref,
                        envelope.original_reply_to,
                        envelope.thread,
                        envelope.bundle_id,
                        envelope.relay_event_id,
                        envelope.relay_device,
                        envelope.relay_short,
                        envelope.relay_reset_generation,
                        prompt_id,
                        rendered_prompt,
                        prompt_digest,
                        formatter_version,
                        grok_session_id,
                        "prepared",
                        _now_ns(),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise JournalStateError(
                    "another unsettled delivery already owns the Grok session"
                ) from exc
            self._record_transition(con, "delivery", delivery_id, None, "prepared", {})
        return self.get_delivery(delivery_id)

    def _delivery_from_row(self, row: sqlite3.Row) -> Delivery:
        return Delivery(
            delivery_id=str(row["delivery_id"]),
            source_key=str(row["source_key"]),
            event_id=int(row["local_event_id"]),
            state=str(row["state"]),
            prompt_id=str(row["prompt_id"]),
            rendered_prompt=str(row["rendered_prompt"]),
            prompt_sha256=str(row["prompt_sha256"]),
            grok_session_id=str(row["grok_session_id"]),
            intent=str(row["intent"]),
            sender=str(row["sender"]),
            reply_ref=str(row["reply_ref"]),
            thread=row["thread"],
        )

    def get_delivery(self, delivery_id: str) -> Delivery:
        row = self.con.execute(
            "SELECT * FROM deliveries WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise JournalStateError(f"unknown delivery {delivery_id}")
        return self._delivery_from_row(row)

    def begin_submission(self, delivery_id: str, rpc_id: str | None = None) -> Delivery:
        with self.transaction() as con:
            row = con.execute(
                "SELECT state,attempts,grok_session_id FROM deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                raise JournalStateError(f"unknown delivery {delivery_id}")
            if row["state"] not in {"prepared", "reconciled_not_seen"}:
                raise JournalStateError(f"cannot submit delivery in state {row['state']}")
            attempt = int(row["attempts"]) + 1
            now = _now_ns()
            con.execute(
                "UPDATE deliveries SET state='submitting',attempts=?,last_error=NULL "
                "WHERE delivery_id=?",
                (attempt, delivery_id),
            )
            con.execute(
                "INSERT INTO prompt_attempts(delivery_id,attempt_no,grok_session_id,rpc_id,"
                "started_ns,admission_state) VALUES(?,?,?,?,?,'submitting')",
                (delivery_id, attempt, str(row["grok_session_id"]), rpc_id, now),
            )
            self._record_transition(
                con, "delivery", delivery_id, str(row["state"]), "submitting", {"attempt": attempt}
            )
        return self.get_delivery(delivery_id)

    def mark_prompt_reconciled_not_seen(
        self, delivery_id: str, authoritative_evidence: dict[str, Any]
    ) -> Delivery:
        with self.transaction() as con:
            row = con.execute(
                "SELECT state,prompt_id,prompt_sha256,grok_session_id,attempts "
                "FROM deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None or row["state"] != "blocked":
                raise JournalStateError("only a blocked delivery can be reconciled not seen")
            self._validate_prompt_evidence(
                authoritative_evidence,
                delivery_id,
                str(row["prompt_id"]),
                str(row["grok_session_id"]),
                str(row["prompt_sha256"]),
                "NOT_FOUND",
            )
            attempt = con.execute(
                "SELECT admission_state FROM prompt_attempts "
                "WHERE delivery_id=? AND attempt_no=?",
                (delivery_id, int(row["attempts"])),
            ).fetchone()
            if attempt is None or attempt["admission_state"] not in {"submitting", "wire_sent"}:
                raise JournalStateError(
                    "only an admission-uncertain prompt attempt can reconcile as not seen"
                )
            now = _now_ns()
            con.execute(
                "UPDATE deliveries SET state='reconciled_not_seen',last_error=NULL "
                "WHERE delivery_id=?",
                (delivery_id,),
            )
            attempt_update = con.execute(
                "UPDATE prompt_attempts SET admission_state='not_seen',result_json=?,ended_ns=? "
                "WHERE delivery_id=? AND attempt_no=? "
                "AND admission_state IN ('submitting','wire_sent')",
                (
                    _canonical_json(authoritative_evidence),
                    now,
                    delivery_id,
                    int(row["attempts"]),
                ),
            )
            if attempt_update.rowcount != 1:
                raise JournalStateError("prompt attempt reconciliation was not atomic")
            self._record_transition(
                con,
                "delivery",
                delivery_id,
                "blocked",
                "reconciled_not_seen",
                authoritative_evidence,
            )
        return self.get_delivery(delivery_id)

    def mark_wire_sent(self, delivery_id: str, rpc_id: str) -> None:
        with self.transaction() as con:
            row = con.execute(
                "SELECT attempts,state FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if row is None or row["state"] != "submitting":
                raise JournalStateError("wire-sent marker requires submitting delivery")
            updated = con.execute(
                "UPDATE prompt_attempts SET admission_state='wire_sent',rpc_id=? "
                "WHERE delivery_id=? AND attempt_no=? AND admission_state='submitting'",
                (rpc_id, delivery_id, int(row["attempts"])),
            )
            if updated.rowcount != 1:
                raise JournalStateError("missing current prompt attempt")

    def mark_persisted_and_advance(
        self,
        delivery_id: str,
        expected_cursor: int,
        authoritative_evidence: dict[str, Any],
    ) -> Delivery:
        with self.transaction() as con:
            row = con.execute(
                "SELECT epoch_id,local_event_id,cursor_before_event_id,raw_event_sha256,state,"
                "attempts,prompt_id,prompt_sha256,grok_session_id FROM deliveries "
                "WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None or row["state"] != "submitting":
                raise JournalStateError("persistence requires a submitting delivery")
            if int(row["cursor_before_event_id"]) != expected_cursor:
                raise JournalStateError("delivery was prepared against a different cursor")
            if int(row["local_event_id"]) <= expected_cursor:
                raise JournalStateError("delivery event must be newer than its prior cursor")
            attempt = con.execute(
                "SELECT admission_state FROM prompt_attempts "
                "WHERE delivery_id=? AND attempt_no=?",
                (delivery_id, int(row["attempts"])),
            ).fetchone()
            if attempt is None or attempt["admission_state"] != "wire_sent":
                raise JournalStateError(
                    "persistence requires the current attempt to cross the wire boundary"
                )
            self._validate_prompt_evidence(
                authoritative_evidence,
                delivery_id,
                str(row["prompt_id"]),
                str(row["grok_session_id"]),
                str(row["prompt_sha256"]),
                "FOUND",
            )
            cursor = con.execute(
                "SELECT committed_event_id FROM cursor WHERE epoch_id=?", (int(row["epoch_id"]),)
            ).fetchone()
            if cursor is None or int(cursor[0]) != expected_cursor:
                actual = None if cursor is None else int(cursor[0])
                raise JournalStateError(
                    f"cursor compare-and-swap failed; expected={expected_cursor} actual={actual}"
                )
            now = _now_ns()
            delivery_update = con.execute(
                "UPDATE deliveries SET state='persisted',persisted_ns=?,last_error=NULL "
                "WHERE delivery_id=? AND state='submitting'",
                (now, delivery_id),
            )
            cursor_update = con.execute(
                "UPDATE cursor SET committed_event_id=?,committed_event_sha256=?,updated_ns=? "
                "WHERE epoch_id=? AND committed_event_id=?",
                (
                    int(row["local_event_id"]),
                    str(row["raw_event_sha256"]),
                    now,
                    int(row["epoch_id"]),
                    expected_cursor,
                ),
            )
            attempt_update = con.execute(
                "UPDATE prompt_attempts SET admission_state='persisted',result_json=?,ended_ns=? "
                "WHERE delivery_id=? AND attempt_no=? "
                "AND admission_state='wire_sent'",
                (
                    _canonical_json(authoritative_evidence),
                    now,
                    delivery_id,
                    int(row["attempts"]),
                ),
            )
            if {delivery_update.rowcount, cursor_update.rowcount, attempt_update.rowcount} != {1}:
                raise JournalStateError("atomic admission transaction did not update every record")
            self._record_transition(
                con,
                "delivery",
                delivery_id,
                "submitting",
                "persisted",
                {
                    "cursor_from": expected_cursor,
                    "cursor_to": int(row["local_event_id"]),
                    "admission": authoritative_evidence,
                },
            )
        return self.get_delivery(delivery_id)

    def reconcile_prompt_found_and_advance(
        self,
        delivery_id: str,
        expected_cursor: int,
        evidence: dict[str, Any],
    ) -> Delivery:
        with self.transaction() as con:
            row = con.execute(
                "SELECT epoch_id,local_event_id,cursor_before_event_id,raw_event_sha256,state,"
                "attempts,prompt_id,prompt_sha256,grok_session_id "
                "FROM deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None or row["state"] != "blocked":
                raise JournalStateError("only a blocked delivery can reconcile a found prompt")
            self._validate_prompt_evidence(
                evidence,
                delivery_id,
                str(row["prompt_id"]),
                str(row["grok_session_id"]),
                str(row["prompt_sha256"]),
                "FOUND",
            )
            attempt = con.execute(
                "SELECT admission_state FROM prompt_attempts "
                "WHERE delivery_id=? AND attempt_no=?",
                (delivery_id, int(row["attempts"])),
            ).fetchone()
            if attempt is None or attempt["admission_state"] not in {"submitting", "wire_sent"}:
                raise JournalStateError(
                    "only an admission-uncertain prompt attempt can reconcile as found"
                )
            if int(row["cursor_before_event_id"]) != expected_cursor:
                raise JournalStateError("delivery was prepared against a different cursor")
            if int(row["local_event_id"]) <= expected_cursor:
                raise JournalStateError("delivery event must be newer than its prior cursor")
            cursor = con.execute(
                "SELECT committed_event_id FROM cursor WHERE epoch_id=?",
                (int(row["epoch_id"]),),
            ).fetchone()
            if cursor is None or int(cursor[0]) != expected_cursor:
                raise JournalStateError("cursor changed before found-prompt reconciliation")
            now = _now_ns()
            delivery_update = con.execute(
                "UPDATE deliveries SET state='persisted',persisted_ns=?,last_error=NULL "
                "WHERE delivery_id=? AND state='blocked'",
                (now, delivery_id),
            )
            cursor_update = con.execute(
                "UPDATE cursor SET committed_event_id=?,committed_event_sha256=?,updated_ns=? "
                "WHERE epoch_id=? AND committed_event_id=?",
                (
                    int(row["local_event_id"]),
                    str(row["raw_event_sha256"]),
                    now,
                    int(row["epoch_id"]),
                    expected_cursor,
                ),
            )
            attempt_update = con.execute(
                "UPDATE prompt_attempts SET admission_state='persisted',result_json=?,ended_ns=? "
                "WHERE delivery_id=? AND attempt_no=? "
                "AND admission_state IN ('submitting','wire_sent')",
                (_canonical_json(evidence), now, delivery_id, int(row["attempts"])),
            )
            if {delivery_update.rowcount, cursor_update.rowcount, attempt_update.rowcount} != {1}:
                raise JournalStateError("found-prompt reconciliation was not atomic")
            self._record_transition(
                con, "delivery", delivery_id, "blocked", "persisted", evidence
            )
        return self.get_delivery(delivery_id)

    def transition_delivery(
        self,
        delivery_id: str,
        new_state: str,
        detail: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> Delivery:
        if new_state in {
            "submitting",
            "reconciled_not_seen",
            "persisted",
            "completed",
            "finalized",
        }:
            raise JournalStateError(f"use the dedicated {new_state} transition method")
        with self.transaction() as con:
            row = con.execute(
                "SELECT state FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if row is None:
                raise JournalStateError(f"unknown delivery {delivery_id}")
            old = str(row["state"])
            if new_state not in DELIVERY_TRANSITIONS.get(old, set()):
                raise JournalStateError(f"illegal delivery transition {old} to {new_state}")
            con.execute(
                "UPDATE deliveries SET state=?,last_error=? WHERE delivery_id=?",
                (new_state, error, delivery_id),
            )
            self._record_transition(con, "delivery", delivery_id, old, new_state, detail or {})
        return self.get_delivery(delivery_id)

    def record_completion(
        self,
        delivery_id: str,
        completion: dict[str, Any],
        assistant_text: str,
        stop_reason: str,
    ) -> Delivery:
        completion_json = _canonical_json(completion)
        completion_digest = hashlib.sha256(completion_json.encode("utf-8")).hexdigest()
        with self.transaction() as con:
            row = con.execute(
                "SELECT state FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if row is None:
                raise JournalStateError(f"unknown delivery {delivery_id}")
            old = str(row["state"])
            if "completed" not in DELIVERY_TRANSITIONS.get(old, set()):
                raise JournalStateError(f"cannot complete delivery in state {old}")
            con.execute(
                "UPDATE deliveries SET state='completed',completed_ns=?,completion_json=?,"
                "completion_sha256=?,assistant_text=?,stop_reason=?,last_error=NULL "
                "WHERE delivery_id=?",
                (
                    _now_ns(),
                    completion_json,
                    completion_digest,
                    assistant_text,
                    stop_reason,
                    delivery_id,
                ),
            )
            self._record_transition(con, "delivery", delivery_id, old, "completed", {})
        return self.get_delivery(delivery_id)

    def prepare_outbound(self, delivery_id: str, phase: str, body: str) -> Outbound:
        if phase not in {"ack", "final", "inform_response"}:
            raise ValueError(f"unsupported outbox phase: {phase}")
        delivery = self.con.execute(
            "SELECT d.epoch_id,d.sender,d.intent,d.reply_ref,d.thread,d.state,e.db_uuid "
            "FROM deliveries d JOIN epochs e ON e.epoch_id=d.epoch_id "
            "WHERE d.delivery_id=?",
            (delivery_id,),
        ).fetchone()
        if delivery is None:
            raise JournalStateError(f"unknown delivery {delivery_id}")
        parent_intent = str(delivery["intent"])
        if phase in {"ack", "final"} and parent_intent != "request":
            raise JournalStateError(f"{phase} is only valid for a request")
        if phase == "inform_response" and parent_intent != "inform":
            raise JournalStateError("inform_response is only valid for an inform")
        self._require_outbound_parent_state(phase, str(delivery["state"]))
        recipient = str(delivery["sender"])
        intent = "ack" if phase == "ack" else "inform"
        reply_ref = str(delivery["reply_ref"])
        thread = delivery["thread"]
        outbound_id = f"{delivery_id}:{phase}"
        envelope = {
            "delivery_id": delivery_id,
            "phase": phase,
            "sender": "bridge",
            "recipient": recipient,
            "intent": intent,
            "reply_ref": reply_ref,
            "thread": thread,
            "body": body,
        }
        envelope_digest = hashlib.sha256(_canonical_json(envelope).encode("utf-8")).hexdigest()
        body_digest = hashlib.sha256(body.encode("utf-8", "surrogatepass")).hexdigest()
        idempotency_key = hashlib.sha256(outbound_id.encode("utf-8")).hexdigest()
        with self.transaction() as con:
            existing = con.execute(
                "SELECT * FROM outbox WHERE outbound_id=?", (outbound_id,)
            ).fetchone()
            if existing is not None:
                immutable = {
                    "source_db_uuid": str(delivery["db_uuid"]),
                    "recipient": recipient,
                    "intent": intent,
                    "reply_ref": reply_ref,
                    "thread": thread,
                    "body_sha256": body_digest,
                    "envelope_sha256": envelope_digest,
                }
                if any(existing[key] != value for key, value in immutable.items()):
                    raise JournalIdentityError("outbox envelope changed for an existing phase")
                return self._outbound_from_row(existing)
            con.execute(
                "INSERT INTO outbox(outbound_id,delivery_id,epoch_id,source_db_uuid,phase,recipient,intent,"
                "reply_ref,thread,exact_body,body_sha256,envelope_sha256,idempotency_key,state,"
                "prepared_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    outbound_id,
                    delivery_id,
                    int(delivery["epoch_id"]),
                    str(delivery["db_uuid"]),
                    phase,
                    recipient,
                    intent,
                    reply_ref,
                    thread,
                    body,
                    body_digest,
                    envelope_digest,
                    idempotency_key,
                    "prepared",
                    _now_ns(),
                ),
            )
            self._record_transition(con, "outbox", outbound_id, None, "prepared", {})
        return self.get_outbound(outbound_id)

    def _outbound_from_row(self, row: sqlite3.Row) -> Outbound:
        return Outbound(
            outbound_id=str(row["outbound_id"]),
            delivery_id=str(row["delivery_id"]),
            phase=str(row["phase"]),
            recipient=str(row["recipient"]),
            intent=str(row["intent"]),
            state=str(row["state"]),
            body=str(row["exact_body"]),
            idempotency_key=str(row["idempotency_key"]),
            envelope_sha256=str(row["envelope_sha256"]),
            source_db_uuid=str(row["source_db_uuid"]),
            hcom_event_id=int(row["hcom_event_id"]) if row["hcom_event_id"] is not None else None,
        )

    def get_outbound(self, outbound_id: str) -> Outbound:
        row = self.con.execute(
            "SELECT * FROM outbox WHERE outbound_id=?", (outbound_id,)
        ).fetchone()
        if row is None:
            raise JournalStateError(f"unknown outbox record {outbound_id}")
        return self._outbound_from_row(row)

    def start_outbox_attempt(self, outbound_id: str) -> Outbound:
        with self.transaction() as con:
            row = con.execute(
                "SELECT o.state,o.attempts,o.phase,d.state AS parent_state "
                "FROM outbox o JOIN deliveries d ON d.delivery_id=o.delivery_id "
                "WHERE o.outbound_id=?",
                (outbound_id,),
            ).fetchone()
            if row is None:
                raise JournalStateError(f"unknown outbox record {outbound_id}")
            old = str(row["state"])
            if "sending" not in OUTBOX_TRANSITIONS.get(old, set()):
                raise JournalStateError(f"cannot send outbox record in state {old}")
            self._require_outbound_parent_state(
                str(row["phase"]), str(row["parent_state"])
            )
            attempt = int(row["attempts"]) + 1
            con.execute(
                "UPDATE outbox SET state='sending',attempts=?,last_error=NULL WHERE outbound_id=?",
                (attempt, outbound_id),
            )
            con.execute(
                "INSERT INTO outbox_attempts(outbound_id,attempt_no,started_ns,result_state) "
                "VALUES(?,?,?,'sending')",
                (outbound_id, attempt, _now_ns()),
            )
            self._record_transition(
                con, "outbox", outbound_id, old, "sending", {"attempt": attempt}
            )
        return self.get_outbound(outbound_id)

    def mark_outbox_ambiguous(self, outbound_id: str, error: str) -> Outbound:
        with self.transaction() as con:
            row = con.execute(
                "SELECT state,attempts FROM outbox WHERE outbound_id=?", (outbound_id,)
            ).fetchone()
            if row is None or row["state"] != "sending":
                raise JournalStateError("only a sending outbox record can become ambiguous")
            now = _now_ns()
            con.execute(
                "UPDATE outbox SET state='ambiguous',last_error=? WHERE outbound_id=?",
                (error, outbound_id),
            )
            con.execute(
                "UPDATE outbox_attempts SET result_state='ambiguous',result_json=?,ended_ns=? "
                "WHERE outbound_id=? AND attempt_no=?",
                (_canonical_json({"error": error}), now, outbound_id, int(row["attempts"])),
            )
            self._record_transition(
                con, "outbox", outbound_id, "sending", "ambiguous", {"error": error}
            )
        return self.get_outbound(outbound_id)

    def mark_outbox_reconciled_absent(
        self, outbound_id: str, authoritative_evidence: dict[str, Any]
    ) -> Outbound:
        with self.transaction() as con:
            row = con.execute(
                "SELECT state,idempotency_key,source_db_uuid FROM outbox WHERE outbound_id=?",
                (outbound_id,),
            ).fetchone()
            if row is None or row["state"] != "ambiguous":
                raise JournalStateError("only an ambiguous outbox record can be reconciled absent")
            self._validate_outbox_evidence(
                authoritative_evidence,
                outbound_id,
                str(row["idempotency_key"]),
                str(row["source_db_uuid"]),
                "NOT_FOUND",
            )
            con.execute(
                "UPDATE outbox SET state='reconciled_absent' WHERE outbound_id=?", (outbound_id,)
            )
            self._record_transition(
                con,
                "outbox",
                outbound_id,
                "ambiguous",
                "reconciled_absent",
                authoritative_evidence,
            )
        return self.get_outbound(outbound_id)

    def mark_outbox_sent(
        self,
        outbound_id: str,
        hcom_event_id: int,
        hcom_event_sha256: str,
        result: dict[str, Any],
    ) -> Outbound:
        if type(hcom_event_id) is not int or hcom_event_id <= 0:
            raise JournalIdentityError("HCOM receipt event ID must be a positive exact integer")
        if not _is_sha256(hcom_event_sha256):
            raise JournalIdentityError("HCOM receipt requires a positive event ID and digest")
        with self.transaction() as con:
            row = con.execute(
                "SELECT state,attempts,idempotency_key,envelope_sha256,source_db_uuid FROM outbox "
                "WHERE outbound_id=?",
                (outbound_id,),
            ).fetchone()
            if row is None:
                raise JournalStateError(f"unknown outbox record {outbound_id}")
            old = str(row["state"])
            if "sent" not in OUTBOX_TRANSITIONS.get(old, set()):
                raise JournalStateError(f"cannot mark outbox record sent from state {old}")
            status = result.get("status")
            allowed_statuses = {"FOUND"} if old == "ambiguous" else {"FOUND", "INSERTED"}
            self._validate_outbox_evidence(
                result,
                outbound_id,
                str(row["idempotency_key"]),
                str(row["source_db_uuid"]),
                status if isinstance(status, str) else "",
                allowed_statuses=allowed_statuses,
                event_id=hcom_event_id,
                event_sha256=hcom_event_sha256,
                envelope_sha256_value=str(row["envelope_sha256"]),
            )
            now = _now_ns()
            try:
                con.execute(
                    "UPDATE outbox SET state='sent',hcom_event_id=?,hcom_event_sha256=?,sent_ns=?,"
                    "last_error=NULL WHERE outbound_id=?",
                    (hcom_event_id, hcom_event_sha256, now, outbound_id),
                )
            except sqlite3.IntegrityError as exc:
                raise JournalIdentityError(
                    "one HCOM event cannot satisfy multiple outbox records"
                ) from exc
            if old == "sending":
                con.execute(
                    "UPDATE outbox_attempts SET result_state='sent',result_json=?,ended_ns=? "
                    "WHERE outbound_id=? AND attempt_no=?",
                    (_canonical_json(result), now, outbound_id, int(row["attempts"])),
                )
            self._record_transition(con, "outbox", outbound_id, old, "sent", result)
        return self.get_outbound(outbound_id)

    @staticmethod
    def _validate_prompt_evidence(
        evidence: dict[str, Any],
        delivery_id: str,
        prompt_id: str,
        session_id: str,
        prompt_sha256: str,
        status: str,
    ) -> None:
        expected = {
            "adapter": "grok.prompt-state.v1",
            "delivery_id": delivery_id,
            "prompt_id": prompt_id,
            "session_id": session_id,
            "prompt_sha256": prompt_sha256,
            "status": status,
        }
        if any(evidence.get(key) != value for key, value in expected.items()):
            raise JournalIdentityError("prompt evidence does not match the delivery")
        if evidence.get("durable") is not True:
            raise JournalIdentityError("prompt evidence is not durably committed")
        queried_ns = evidence.get("queried_ns")
        if type(queried_ns) is not int or queried_ns <= 0:
            raise JournalIdentityError("prompt evidence lacks a valid query timestamp")

    @staticmethod
    def _validate_outbox_evidence(
        evidence: dict[str, Any],
        outbound_id: str,
        idempotency_key: str,
        source_db_uuid: str,
        status: str,
        *,
        allowed_statuses: set[str] | None = None,
        event_id: int | None = None,
        event_sha256: str | None = None,
        envelope_sha256_value: str | None = None,
    ) -> None:
        statuses = allowed_statuses or {status}
        if status not in statuses:
            raise JournalIdentityError("outbox evidence has an invalid status")
        expected: dict[str, Any] = {
            "adapter": "hcom.external-adapter.v1",
            "outbound_id": outbound_id,
            "idempotency_key": idempotency_key,
            "source_db_uuid": source_db_uuid,
            "status": status,
        }
        if event_id is not None:
            expected.update(
                {
                    "event_id": event_id,
                    "event_sha256": event_sha256,
                    "envelope_sha256": envelope_sha256_value,
                }
            )
        if any(evidence.get(key) != value for key, value in expected.items()):
            raise JournalIdentityError("outbox evidence does not match the journal record")
        if evidence.get("durable") is not True:
            raise JournalIdentityError("outbox evidence is not durably committed")
        if event_id is not None:
            if type(evidence.get("event_id")) is not int or evidence["event_id"] <= 0:
                raise JournalIdentityError("outbox evidence has an invalid event ID")
            for key in ("event_sha256", "envelope_sha256"):
                value = evidence.get(key)
                if not isinstance(value, str) or len(value) != 64:
                    raise JournalIdentityError(f"outbox evidence has an invalid {key}")
        queried_ns = evidence.get("queried_ns")
        if type(queried_ns) is not int or queried_ns <= 0:
            raise JournalIdentityError("outbox evidence lacks a valid query timestamp")

    @staticmethod
    def _require_outbound_parent_state(phase: str, parent_state: str) -> None:
        allowed = {
            "ack": {"persisted", "running", "completed"},
            "final": {"completed"},
            "inform_response": {"completed"},
        }
        if parent_state not in allowed.get(phase, set()):
            raise JournalStateError(
                f"cannot send {phase} while parent delivery is {parent_state}"
            )

    def finalize_delivery(self, delivery_id: str) -> Delivery:
        with self.transaction() as con:
            delivery = con.execute(
                "SELECT state,intent FROM deliveries WHERE delivery_id=?", (delivery_id,)
            ).fetchone()
            if delivery is None or delivery["state"] != "completed":
                raise JournalStateError("only a completed delivery can be finalized")
            intent = str(delivery["intent"])
            if intent == "request":
                for phase in ("ack", "final"):
                    sent = con.execute(
                        "SELECT state FROM outbox WHERE delivery_id=? AND phase=?",
                        (delivery_id, phase),
                    ).fetchone()
                    if sent is None or sent["state"] != "sent":
                        raise JournalStateError(
                            f"request cannot finalize without one sent {phase}"
                        )
            else:
                unfinished = con.execute(
                    "SELECT COUNT(*) FROM outbox WHERE delivery_id=? AND state!='sent'",
                    (delivery_id,),
                ).fetchone()[0]
                if unfinished:
                    raise JournalStateError("inform has an unsettled response")
            con.execute(
                "UPDATE deliveries SET state='finalized',finalized_ns=? WHERE delivery_id=?",
                (_now_ns(), delivery_id),
            )
            self._record_transition(
                con, "delivery", delivery_id, "completed", "finalized", {}
            )
        return self.get_delivery(delivery_id)

    def unsettled(self) -> tuple[list[Delivery], list[Outbound]]:
        delivery_rows = self.con.execute(
            "SELECT * FROM deliveries WHERE state NOT IN ('finalized','abandoned') "
            "ORDER BY local_event_id"
        ).fetchall()
        outbound_rows = self.con.execute(
            "SELECT * FROM outbox WHERE state!='sent' ORDER BY prepared_ns"
        ).fetchall()
        return (
            [self._delivery_from_row(row) for row in delivery_rows],
            [self._outbound_from_row(row) for row in outbound_rows],
        )

    def deactivate_epoch(self, epoch_id: int) -> None:
        with self.transaction() as con:
            unsettled = con.execute(
                "SELECT COUNT(*) FROM deliveries WHERE epoch_id=? "
                "AND state NOT IN ('finalized','abandoned')",
                (epoch_id,),
            ).fetchone()[0]
            outbox = con.execute(
                "SELECT COUNT(*) FROM outbox WHERE epoch_id=? AND state!='sent'", (epoch_id,)
            ).fetchone()[0]
            if unsettled or outbox:
                raise JournalStateError("cannot deactivate epoch with unsettled work")
            con.execute("UPDATE epochs SET active=0 WHERE epoch_id=?", (epoch_id,))

    def _record_transition(
        self,
        con: sqlite3.Connection,
        entity_kind: str,
        entity_id: str,
        old_state: str | None,
        new_state: str,
        detail: dict[str, Any],
    ) -> None:
        con.execute(
            "INSERT INTO transitions(entity_kind,entity_id,from_state,to_state,timestamp_ns,"
            "detail_json) VALUES(?,?,?,?,?,?)",
            (
                entity_kind,
                entity_id,
                old_state,
                new_state,
                _now_ns(),
                _canonical_json(detail),
            ),
        )
