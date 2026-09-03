from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path

from .envelope import (
    DATABASE_UUID_KEY,
    EventRow,
    HcomReadError,
    HcomReader,
    HcomSchemaError,
    classify_event,
    prompt_text,
)


def create_hcom_db(path: Path, schema: int = 18) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute(f"PRAGMA user_version={schema}")
    con.execute(
        "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT NOT NULL,"
        "type TEXT NOT NULL,instance TEXT NOT NULL,data TEXT NOT NULL)"
    )
    con.execute("CREATE TABLE instances(name TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE kv(key TEXT PRIMARY KEY,value TEXT)")
    con.execute(
        "INSERT INTO kv(key,value) VALUES(?,?)", (DATABASE_UUID_KEY, str(uuid.uuid4()))
    )
    con.commit()
    return con


class EnvelopeTests(unittest.TestCase):
    def test_request_contract_separates_auto_reply_from_initiated_message(self) -> None:
        row = EventRow(
            1,
            "2026-08-31T00:00:00Z",
            "message",
            "kimo",
            json.dumps(
                {
                    "from": "kimo",
                    "intent": "request",
                    "mentions": ["gsea"],
                    "delivered_to": ["gsea"],
                    "thread": "contract-test",
                    "text": "Send a separate update, then report completion.",
                }
            ),
        )
        classified = classify_event(row, "gsea")
        assert classified.envelope is not None
        rendered = prompt_text(classified.envelope, "gsea", "delivery-1")
        self.assertIn("bridge returns that response", rendered)
        self.assertIn("do not send a duplicate reply", rendered)
        self.assertIn("initiate a separate HCOM message", rendered)
        self.assertIn("hcom send --name gsea", rendered)
        self.assertIn("Keep the completion summary concise", rendered)

    def test_remote_request_preserves_original_reply_reference(self) -> None:
        payload = {
            "from": "wexi:BOXE",
            "intent": "request",
            "mentions": ["gsea"],
            "delivered_to": ["gsea"],
            "thread": "remote-test",
            "bundle_id": "bundle-1",
            "scope": "mentions",
            "sender_kind": "instance",
            "text": "calculate Ω\nthen reply",
            "_relay": {
                "id": 42,
                "short": "BOXE",
                "device": "device-boxe",
                "reset": "reset-7",
            },
            "future_extension": {"kept": True},
        }
        row = EventRow(900, "2026-08-31T00:00:00Z", "message", "wexi:BOXE", json.dumps(payload))
        result = classify_event(row, "gsea")
        self.assertEqual(result.disposition, "request")
        assert result.envelope is not None
        self.assertEqual(result.envelope.reply_ref, "42:BOXE")
        self.assertEqual(result.envelope.relay_device, "device-boxe")
        self.assertEqual(result.envelope.relay_reset_generation, "reset-7")
        self.assertEqual(result.envelope.thread, "remote-test")
        self.assertEqual(result.envelope.bundle_id, "bundle-1")
        self.assertEqual(result.envelope.raw_object["future_extension"], {"kept": True})

    def test_ack_is_classified_without_model_delivery(self) -> None:
        row = EventRow(
            2,
            "t",
            "message",
            "wexi",
            json.dumps(
                {
                    "from": "wexi",
                    "intent": "ack",
                    "mentions": ["gsea"],
                    "delivered_to": ["gsea"],
                    "text": "ok",
                }
            ),
        )
        result = classify_event(row, "gsea")
        self.assertEqual(result.disposition, "ack")

    def test_hostile_routing_text_remains_data(self) -> None:
        hostile = "$(touch /tmp/nope); ' ; `uname`"
        row = EventRow(
            3,
            "t",
            "message",
            hostile,
            json.dumps(
                {
                    "from": hostile,
                    "intent": "request",
                    "mentions": ["gsea"],
                    "delivered_to": ["gsea"],
                    "thread": hostile,
                    "text": "plain body",
                }
            ),
        )
        result = classify_event(row, "gsea")
        assert result.envelope is not None
        self.assertEqual(result.envelope.sender, hostile)
        self.assertEqual(result.envelope.thread, hostile)

    def test_addressed_malformed_message_is_quarantined(self) -> None:
        row = EventRow(
            4,
            "t",
            "message",
            "wexi",
            json.dumps({"from": "wexi", "mentions": ["gsea"], "text": ""}),
        )
        result = classify_event(row, "gsea")
        self.assertEqual(result.disposition, "quarantine")

    def test_remote_without_reset_generation_is_quarantined(self) -> None:
        row = EventRow(
            5,
            "t",
            "message",
            "wexi:BOXE",
            json.dumps(
                {
                    "from": "wexi:BOXE",
                    "intent": "request",
                    "mentions": ["gsea"],
                    "delivered_to": ["gsea"],
                    "text": "work",
                    "_relay": {"id": 42, "short": "BOXE", "device": "device-boxe"},
                }
            ),
        )
        self.assertEqual(classify_event(row, "gsea").disposition, "quarantine")

    def test_present_but_malformed_relay_is_never_downgraded_to_local(self) -> None:
        malformed = (
            None,
            {},
            {"id": 42, "device": "device-boxe", "reset": "reset-7"},
            {"short": "BOXE", "device": "device-boxe", "reset": "reset-7"},
            {"id": True, "short": "BOXE", "device": "device-boxe", "reset": "reset-7"},
        )
        for index, relay in enumerate(malformed, 1):
            with self.subTest(relay=relay):
                row = EventRow(
                    100 + index,
                    "t",
                    "message",
                    "wexi:BOXE",
                    json.dumps(
                        {
                            "from": "wexi:BOXE",
                            "intent": "request",
                            "mentions": ["gsea"],
                            "delivered_to": ["gsea"],
                            "text": "remote task",
                            "_relay": relay,
                        }
                    ),
                )
                self.assertEqual(classify_event(row, "gsea").disposition, "quarantine")

    def test_broadcast_is_delivered_without_local_recipient_list(self) -> None:
        row = EventRow(
            6,
            "t",
            "message",
            "wexi",
            json.dumps(
                {
                    "from": "wexi",
                    "intent": "inform",
                    "scope": "broadcast",
                    "text": "fleet update",
                }
            ),
        )
        self.assertEqual(classify_event(row, "gsea").disposition, "inform")

    def test_same_base_remote_sender_is_not_suppressed_as_own(self) -> None:
        row = EventRow(
            7,
            "t",
            "message",
            "gsea:BOXE",
            json.dumps(
                {
                    "from": "gsea:BOXE",
                    "intent": "inform",
                    "mentions": ["gsea"],
                    "delivered_to": ["gsea"],
                    "text": "remote namesake",
                    "_relay": {
                        "id": 77,
                        "short": "BOXE",
                        "device": "device-boxe",
                        "reset": "reset-7",
                    },
                }
            ),
        )
        self.assertEqual(classify_event(row, "gsea").disposition, "inform")

    def test_reader_requires_exact_schema_and_database_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = create_hcom_db(db)
            con.close()
            identity = HcomReader(db).identity()
            self.assertEqual(identity.schema_version, 18)
            self.assertTrue(identity.db_uuid)

            con = sqlite3.connect(db)
            con.execute("PRAGMA user_version=19")
            con.commit()
            con.close()
            with self.assertRaises(HcomSchemaError):
                HcomReader(db).identity()

    def test_reader_rejects_missing_database_uuid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = create_hcom_db(db)
            con.execute("DELETE FROM kv WHERE key=?", (DATABASE_UUID_KEY,))
            con.commit()
            con.close()
            with self.assertRaises(HcomReadError):
                HcomReader(db).identity()

    def test_event_sequence_survives_deleted_latest_row(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = create_hcom_db(db)
            con.execute(
                "INSERT INTO events(timestamp,type,instance,data) VALUES(?,?,?,?)",
                ("t", "life", "gsea", "{}"),
            )
            event_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
            row = con.execute(
                "SELECT id,timestamp,type,instance,data FROM events WHERE id=?", (event_id,)
            ).fetchone()
            digest = EventRow(int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4])).sha256
            con.commit()
            con.close()
            reader = HcomReader(db)
            self.assertEqual(reader.event_sequence(), event_id)
            self.assertEqual(reader.validate_committed_cursor(event_id, digest)[0], True)
            con = sqlite3.connect(db)
            con.execute("DELETE FROM events WHERE id=?", (event_id,))
            con.commit()
            con.close()
            self.assertEqual(reader.event_sequence(), event_id)
            self.assertEqual(reader.validate_committed_cursor(event_id, digest)[0], False)


if __name__ == "__main__":
    unittest.main()
