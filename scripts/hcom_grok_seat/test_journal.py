from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from .envelope import Envelope, EventRow, HcomIdentity, classify_event
from .journal import (
    Journal,
    JournalIdentityError,
    JournalStateError,
    delivery_identity,
    stable_prompt_id,
)


def identity(path: Path, db_uuid: str = "db-1") -> HcomIdentity:
    return HcomIdentity(db_uuid, str(path), 18, 1, 2, 10, "a" * 64)


def envelope(event_id: int = 11, body: str = "do the work") -> Envelope:
    raw = {
        "from": "wexi",
        "intent": "request",
        "mentions": ["gsea"],
        "delivered_to": ["gsea"],
        "thread": "journal-test",
        "text": body,
    }
    row = EventRow(event_id, "2026-08-31T00:00:00Z", "message", "wexi", json.dumps(raw))
    return Envelope(
        row,
        "wexi",
        "request",
        body,
        str(event_id),
        None,
        "journal-test",
        None,
        "mentions",
        ("gsea",),
        ("gsea",),
        "instance",
        None,
        None,
        None,
        None,
        None,
        raw,
    )


def prompt_evidence(delivery: object, status: str) -> dict:
    return {
        "adapter": "grok.prompt-state.v1",
        "delivery_id": delivery.delivery_id,
        "prompt_id": delivery.prompt_id,
        "session_id": delivery.grok_session_id,
        "prompt_sha256": delivery.prompt_sha256,
        "status": status,
        "durable": True,
        "queried_ns": 1,
    }


def outbox_evidence(
    outgoing: object,
    status: str,
    event_id: int | None = None,
    event_sha256: str | None = None,
) -> dict:
    result = {
        "adapter": "hcom.external-adapter.v1",
        "outbound_id": outgoing.outbound_id,
        "idempotency_key": outgoing.idempotency_key,
        "source_db_uuid": outgoing.source_db_uuid,
        "status": status,
        "durable": True,
        "queried_ns": 1,
    }
    if event_id is not None:
        result.update(
            {
                "event_id": event_id,
                "event_sha256": event_sha256,
                "envelope_sha256": outgoing.envelope_sha256,
            }
        )
    return result


class JournalTests(unittest.TestCase):
    def test_permissions_and_wal_are_hardened(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "state" / "bridge.sqlite3"
            with Journal(path) as journal:
                mode = journal.con.execute("PRAGMA journal_mode").fetchone()[0]
                sync = journal.con.execute("PRAGMA synchronous").fetchone()[0]
                fullfsync = journal.con.execute("PRAGMA fullfsync").fetchone()[0]
                self.assertEqual(str(mode).lower(), "wal")
                self.assertEqual(int(sync), 2)
                self.assertEqual(int(fullfsync), 1)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
            self.assertEqual(os.stat(path.parent).st_mode & 0o777, 0o700)

    def test_identity_and_prompt_are_stable_across_format_changes(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            hcom_path = Path(td) / "hcom.db"
            ident = identity(hcom_path)
            item = envelope()
            first = delivery_identity(ident, "gsea", item)
            second = delivery_identity(ident, "gsea", item)
            self.assertEqual(first, second)
            self.assertEqual(stable_prompt_id(first), stable_prompt_id(second))

            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, item, "format version one", "sess")
                replay = journal.prepare_delivery(epoch, ident, item, "format version two", "sess")
                self.assertEqual(delivery.prompt_id, replay.prompt_id)
                self.assertEqual(replay.rendered_prompt, "format version one")

    def test_resume_rejects_replaced_database(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            state = Path(td) / "bridge.sqlite3"
            original = identity(Path(td) / "hcom.db")
            with Journal(state) as journal:
                journal.create_epoch(original, "gsea", "join-live", 10)
                replaced = HcomIdentity(
                    "db-2",
                    original.canonical_path,
                    18,
                    original.device,
                    original.inode,
                    original.anchor_event_id,
                    original.anchor_sha256,
                )
                with self.assertRaises(JournalIdentityError):
                    journal.resume_epoch(
                        replaced,
                        "gsea",
                        event_sequence=10,
                        committed_event_digest="a" * 64,
                    )

    def test_cursor_is_monotonic_and_sync_cannot_run_ahead(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                journal.commit_scanned_event(epoch.epoch_id, 10, 11, "b" * 64)
                journal.mark_hcom_synced(epoch.epoch_id, -1, 11)
                self.assertEqual(journal.cursor(epoch.epoch_id), (11, 11))
                with self.assertRaises(JournalStateError):
                    journal.commit_scanned_event(epoch.epoch_id, 11, 9, "d" * 64)
                with self.assertRaises(JournalStateError):
                    journal.mark_hcom_synced(epoch.epoch_id, 11, 12)

    def test_delivery_and_outbox_transitions_are_strict_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                journal.transition_delivery(delivery.delivery_id, "running")
                journal.record_completion(delivery.delivery_id, {"stopReason": "end_turn"}, "finished", "end_turn")
                ack = journal.prepare_outbound(delivery.delivery_id, "ack", "Received and queued.")
                journal.start_outbox_attempt(ack.outbound_id)
                journal.mark_outbox_sent(
                    ack.outbound_id,
                    76,
                    "a" * 64,
                    outbox_evidence(ack, "INSERTED", 76, "a" * 64),
                )
                outgoing = journal.prepare_outbound(delivery.delivery_id, "final", "finished")
                same = journal.prepare_outbound(delivery.delivery_id, "final", "finished")
                self.assertEqual(outgoing.idempotency_key, same.idempotency_key)
                journal.start_outbox_attempt(outgoing.outbound_id)
                journal.mark_outbox_sent(
                    outgoing.outbound_id,
                    77,
                    "b" * 64,
                    outbox_evidence(outgoing, "INSERTED", 77, "b" * 64),
                )
                journal.finalize_delivery(delivery.delivery_id)
                self.assertEqual(journal.get_outbound(outgoing.outbound_id).hcom_event_id, 77)
                with self.assertRaises(JournalStateError):
                    journal.transition_delivery(delivery.delivery_id, "running")

    def test_outbox_body_cannot_change_after_prepare(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                journal.prepare_outbound(delivery.delivery_id, "ack", "Received and queued.")
                with self.assertRaises(JournalIdentityError):
                    journal.prepare_outbound(delivery.delivery_id, "ack", "changed")

    def test_only_one_delivery_can_be_unsettled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                journal.prepare_delivery(epoch, ident, envelope(11), "one", "sess")
                with self.assertRaises(JournalStateError):
                    journal.prepare_delivery(epoch, ident, envelope(12), "two", "sess")

    def test_blocked_prompt_needs_authoritative_not_seen_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.transition_delivery(delivery.delivery_id, "blocked", error="ambiguous")
                with self.assertRaises(JournalStateError):
                    journal.begin_submission(delivery.delivery_id, "rpc-2")
                journal.mark_prompt_reconciled_not_seen(
                    delivery.delivery_id, prompt_evidence(delivery, "NOT_FOUND")
                )
                journal.begin_submission(delivery.delivery_id, "rpc-2")

    def test_finalization_requires_both_request_replies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                journal.record_completion(
                    delivery.delivery_id, {"stopReason": "end_turn"}, "done", "end_turn"
                )
                with self.assertRaises(JournalStateError):
                    journal.finalize_delivery(delivery.delivery_id)
                ack = journal.prepare_outbound(delivery.delivery_id, "ack", "Received and queued.")
                journal.start_outbox_attempt(ack.outbound_id)
                journal.mark_outbox_sent(
                    ack.outbound_id,
                    20,
                    "a" * 64,
                    outbox_evidence(ack, "INSERTED", 20, "a" * 64),
                )
                with self.assertRaises(JournalStateError):
                    journal.finalize_delivery(delivery.delivery_id)

    def test_ambiguous_outbox_cannot_retry_without_not_found_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                ack = journal.prepare_outbound(delivery.delivery_id, "ack", "Received and queued.")
                journal.start_outbox_attempt(ack.outbound_id)
                journal.mark_outbox_ambiguous(ack.outbound_id, "connection lost")
                with self.assertRaises(JournalStateError):
                    journal.start_outbox_attempt(ack.outbound_id)
                with self.assertRaises(JournalIdentityError):
                    journal.mark_outbox_reconciled_absent(ack.outbound_id, {})
                journal.mark_outbox_reconciled_absent(
                    ack.outbound_id, outbox_evidence(ack, "NOT_FOUND")
                )
                journal.start_outbox_attempt(ack.outbound_id)

    def test_remote_source_identity_survives_local_reimport(self) -> None:
        payload = {
            "from": "wexi:BOXE",
            "intent": "request",
            "mentions": ["gsea"],
            "delivered_to": ["gsea"],
            "text": "same remote task",
            "_relay": {
                "id": 42,
                "short": "BOXE",
                "device": "device-boxe",
                "reset": "reset-7",
            },
        }
        first = classify_event(
            EventRow(100, "t1", "message", "wexi:BOXE", json.dumps(payload)), "gsea"
        ).envelope
        second = classify_event(
            EventRow(900, "t2", "message", "wexi:BOXE", json.dumps(payload)), "gsea"
        ).envelope
        assert first is not None and second is not None
        a = identity(Path("/one/hcom.db"), "db-a")
        b = identity(Path("/two/hcom.db"), "db-b")
        self.assertEqual(delivery_identity(a, "gsea", first), delivery_identity(b, "gsea", second))

    def test_cursor_cannot_jump_past_unsettled_delivery_or_regress_on_persist(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(11), "prompt", "sess")
                with self.assertRaises(JournalStateError):
                    journal.commit_scanned_event(epoch.epoch_id, 10, 12, "c" * 64)
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                self.assertEqual(journal.cursor(epoch.epoch_id)[0], 11)

    def test_found_prompt_recovery_advances_cursor_in_same_transition(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(11), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.transition_delivery(delivery.delivery_id, "blocked", error="lost transport")
                recovered = journal.reconcile_prompt_found_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                self.assertEqual(recovered.state, "persisted")
                self.assertEqual(journal.cursor(epoch.epoch_id)[0], 11)

    def test_admitted_prompt_can_never_be_reconciled_not_seen_or_resubmitted(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(11), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                journal.transition_delivery(delivery.delivery_id, "blocked", error="later failure")
                with self.assertRaises(JournalStateError):
                    journal.mark_prompt_reconciled_not_seen(
                        delivery.delivery_id, prompt_evidence(delivery, "NOT_FOUND")
                    )
                with self.assertRaises(JournalStateError):
                    journal.transition_delivery(delivery.delivery_id, "reconciled_not_seen")
                with self.assertRaises(JournalStateError):
                    journal.begin_submission(delivery.delivery_id, "rpc-2")

    def test_stale_event_is_rejected_before_it_can_cross_grok_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                journal.commit_scanned_event(epoch.epoch_id, 10, 12, "c" * 64)
                with self.assertRaises(JournalStateError):
                    journal.prepare_delivery(epoch, ident, envelope(11), "stale", "sess")

    def test_existing_delivery_replays_after_cursor_advances(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            item = envelope(11)
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, item, "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )

                replay = journal.prepare_delivery(
                    epoch, ident, item, "changed prompt", "changed session"
                )

                self.assertEqual(replay.delivery_id, delivery.delivery_id)
                self.assertEqual(replay.rendered_prompt, "prompt")
                self.assertEqual(journal.cursor(epoch.epoch_id)[0], 11)

    def test_authoritative_evidence_rejects_bool_integer_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(11), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.transition_delivery(delivery.delivery_id, "blocked", error="ambiguous")
                bad = prompt_evidence(delivery, "NOT_FOUND")
                bad["durable"] = 1
                bad["queried_ns"] = True
                with self.assertRaises(JournalIdentityError):
                    journal.mark_prompt_reconciled_not_seen(delivery.delivery_id, bad)

    def test_one_hcom_event_cannot_satisfy_ack_and_final(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                delivery = journal.prepare_delivery(epoch, ident, envelope(), "prompt", "sess")
                journal.begin_submission(delivery.delivery_id, "rpc-1")
                journal.mark_wire_sent(delivery.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    delivery.delivery_id, 10, prompt_evidence(delivery, "FOUND")
                )
                ack = journal.prepare_outbound(delivery.delivery_id, "ack", "queued")
                journal.record_completion(
                    delivery.delivery_id, {"stopReason": "end_turn"}, "done", "end_turn"
                )
                final = journal.prepare_outbound(delivery.delivery_id, "final", "done")
                journal.start_outbox_attempt(ack.outbound_id)
                journal.mark_outbox_sent(
                    ack.outbound_id,
                    77,
                    "a" * 64,
                    outbox_evidence(ack, "INSERTED", 77, "a" * 64),
                )
                journal.start_outbox_attempt(final.outbound_id)
                with self.assertRaises(JournalIdentityError):
                    journal.mark_outbox_sent(
                        final.outbound_id,
                        77,
                        "b" * 64,
                        outbox_evidence(final, "INSERTED", 77, "b" * 64),
                    )

    def test_one_hcom_event_cannot_be_reused_across_bridge_epochs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            ident = identity(Path(td) / "hcom.db")
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                first_epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                first = journal.prepare_delivery(
                    first_epoch, ident, envelope(11), "first", "sess"
                )
                journal.begin_submission(first.delivery_id, "rpc-1")
                journal.mark_wire_sent(first.delivery_id, "rpc-1")
                journal.mark_persisted_and_advance(
                    first.delivery_id, 10, prompt_evidence(first, "FOUND")
                )
                first_ack = journal.prepare_outbound(first.delivery_id, "ack", "queued")
                journal.start_outbox_attempt(first_ack.outbound_id)
                journal.mark_outbox_sent(
                    first_ack.outbound_id,
                    77,
                    "a" * 64,
                    outbox_evidence(first_ack, "INSERTED", 77, "a" * 64),
                )
                journal.transition_delivery(first.delivery_id, "abandoned")
                journal.deactivate_epoch(first_epoch.epoch_id)

                second_epoch = journal.create_epoch(ident, "gsea", "join-live", 10)
                second = journal.prepare_delivery(
                    second_epoch, ident, envelope(12), "second", "sess"
                )
                journal.begin_submission(second.delivery_id, "rpc-2")
                journal.mark_wire_sent(second.delivery_id, "rpc-2")
                journal.mark_persisted_and_advance(
                    second.delivery_id, 10, prompt_evidence(second, "FOUND")
                )
                second_ack = journal.prepare_outbound(second.delivery_id, "ack", "queued")
                journal.start_outbox_attempt(second_ack.outbound_id)
                with self.assertRaises(JournalIdentityError):
                    journal.mark_outbox_sent(
                        second_ack.outbound_id,
                        77,
                        "b" * 64,
                        outbox_evidence(second_ack, "INSERTED", 77, "b" * 64),
                    )

    def test_provisional_schema_one_fails_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "bridge.sqlite3"
            con = sqlite3.connect(path)
            con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            con.execute("INSERT INTO meta(key,value) VALUES('journal_schema','1')")
            con.commit()
            con.close()
            before = sqlite3.connect(path)
            before_application = int(before.execute("PRAGMA application_id").fetchone()[0])
            before_mode = str(before.execute("PRAGMA journal_mode").fetchone()[0])
            before.close()
            with self.assertRaisesRegex(Exception, "schema 1 is provisional"):
                Journal(path)
            after = sqlite3.connect(path)
            self.assertEqual(int(after.execute("PRAGMA application_id").fetchone()[0]), before_application)
            self.assertEqual(str(after.execute("PRAGMA journal_mode").fetchone()[0]), before_mode)
            after.close()

    def test_commit_failure_rolls_back_and_does_not_wedge_connection(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            with Journal(Path(td) / "bridge.sqlite3") as journal:
                with self.assertRaises(sqlite3.IntegrityError):
                    with journal.transaction() as con:
                        con.execute("PRAGMA defer_foreign_keys=ON")
                        con.execute(
                            "INSERT INTO prompt_attempts(delivery_id,attempt_no,grok_session_id,"
                            "started_ns,admission_state) VALUES('missing',1,'sess',1,'submitting')"
                        )
                self.assertFalse(journal.con.in_transaction)
                with journal.transaction() as con:
                    self.assertEqual(con.execute("SELECT 1").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
