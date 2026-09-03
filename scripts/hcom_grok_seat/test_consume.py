"""Drive shipped consume.py against a temp sqlite events row."""
from __future__ import annotations

import ast
import json
import secrets
import sqlite3
import tempfile
import unittest
from pathlib import Path

from . import consume


class FakeAcp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def rpc(self, method: str, params: dict, wait: float = 0) -> dict:
        self.calls.append((method, params))
        return {"jsonrpc": "2.0", "id": 1, "result": {"stopReason": "end_turn"}}


def _open_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path))
    con.execute(
        "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, "
        "type TEXT NOT NULL, instance TEXT NOT NULL, data TEXT NOT NULL)"
    )
    return con


def _insert_message(
    con: sqlite3.Connection,
    token: str,
    to: str = "gsea",
    sender: str = "wexi",
    intent: str | None = "request",
    thread: str | None = None,
) -> int:
    payload = {
        "delivered_to": [to],
        "from": sender,
        "mentions": [to],
        "scope": "mentions",
        "sender_kind": "instance",
        "text": token,
    }
    if intent:
        payload["intent"] = intent
    if thread:
        payload["thread"] = thread
    payload_s = json.dumps(payload)
    con.execute(
        "INSERT INTO events (timestamp, type, instance, data) VALUES (?,?,?,?)",
        ("2026-08-30T00:00:00", "message", sender, payload_s),
    )
    return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])


def _write_db(path: Path, token: str, to: str = "gsea", sender: str = "wexi") -> int:
    con = _open_db(path)
    eid = _insert_message(con, token, to=to, sender=sender)
    con.execute(
        "INSERT INTO events (timestamp, type, instance, data) VALUES (?,?,?,?)",
        ("2026-08-30T00:00:01", "life", to, json.dumps({"action": "created"})),
    )
    con.commit()
    con.close()
    return eid


class ConsumeTests(unittest.TestCase):
    def test_unique_token_becomes_queued_session_prompt(self) -> None:
        token = "tok_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            eid = _write_db(db, token)
            fake = FakeAcp()
            seen: set[int] = set()
            last, results = consume.consume_once(
                db, 0, "gsea", "sess-test-uuid", fake.rpc, seen
            )
            self.assertGreaterEqual(last, eid)
            self.assertEqual(len(fake.calls), 1)
            method, params = fake.calls[0]
            self.assertEqual(method, "session/prompt")
            self.assertFalse(params["_meta"]["sendNow"])
            self.assertIs(params["_meta"]["sendNow"], False)
            self.assertIn(str(eid), params["_meta"]["promptId"])
            self.assertTrue(params["_meta"]["promptId"].startswith("hcom:gseat:gsea:"))
            body = params["prompt"][0]["text"]
            self.assertIn(token, body)
            self.assertIn("[HCOM]", body)
            self.assertEqual(results[0]["event_id"], eid)

    def test_skips_own_outbound_and_does_not_prompt(self) -> None:
        token = "own_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            _write_db(db, token, to="wexi", sender="gsea")
            fake = FakeAcp()
            consume.consume_once(db, 0, "gsea", "sess", fake.rpc, set())
            self.assertEqual(fake.calls, [])

    def test_flush_old_news_only_newer_token_is_prompted(self) -> None:
        old_tok = "old_" + secrets.token_hex(8)
        new_tok = "new_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = _open_db(db)
            old_id = _insert_message(con, old_tok)
            con.execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES (?,?,?,?)",
                ("2026-08-30T00:00:01", "life", "gsea", json.dumps({"action": "created"})),
            )
            new_id = _insert_message(con, new_tok)
            con.commit()
            con.close()
            self.assertLess(old_id, new_id)
            fake = FakeAcp()
            last, results = consume.consume_once(
                db, old_id, "gsea", "sess-flush", fake.rpc, set()
            )
            self.assertEqual(len(fake.calls), 1)
            method, params = fake.calls[0]
            self.assertEqual(method, "session/prompt")
            self.assertIs(params["_meta"]["sendNow"], False)
            self.assertIn(str(new_id), params["_meta"]["promptId"])
            body = params["prompt"][0]["text"]
            self.assertIn(new_tok, body)
            self.assertNotIn(old_tok, body)
            self.assertEqual(results[0]["event_id"], new_id)
            self.assertGreaterEqual(last, new_id)

    def test_incoming_ack_is_flushed_without_prompt(self) -> None:
        token = "ack_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = _open_db(db)
            eid = _insert_message(con, token, intent="ack")
            con.commit()
            con.close()
            fake = FakeAcp()
            last, results = consume.consume_once(db, 0, "gsea", "sess", fake.rpc, set())
            self.assertEqual(fake.calls, [])
            self.assertEqual(results, [])
            self.assertGreaterEqual(last, eid)
            self.assertNotIn(token, json.dumps(results))

    def test_request_letter_keeps_envelope(self) -> None:
        token = "req_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            con = _open_db(db)
            eid = _insert_message(con, token, intent="request", thread="grokhatch")
            con.commit()
            con.close()
            fake = FakeAcp()
            consume.consume_once(db, 0, "gsea", "sess", fake.rpc, set())
            body = fake.calls[0][1]["prompt"][0]["text"]
            self.assertIn(token, body)
            self.assertIn("Intent: request", body)
            self.assertIn(f"--reply-to {eid}", body)
            self.assertIn("--thread grokhatch", body)
            self.assertIn("--intent ack", body)

    def test_failed_prompt_does_not_flush_the_letter(self) -> None:
        token = "retry_" + secrets.token_hex(8)

        class BoomAcp(FakeAcp):
            def rpc(self, method: str, params: dict, wait: float = 0) -> dict:
                self.calls.append((method, params))
                return {"error": {"code": -1, "message": "boom"}}

        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            eid = _write_db(db, token)
            fake = BoomAcp()
            last, results = consume.consume_once(db, 0, "gsea", "sess", fake.rpc, set())
            self.assertEqual(last, 0)
            self.assertEqual(len(fake.calls), 1)
            self.assertIn(token, fake.calls[0][1]["prompt"][0]["text"])
            self.assertEqual(results[0]["event_id"], eid)

    def test_production_supervisor_does_not_import_legacy_consumer(self) -> None:
        src = (Path(__file__).resolve().parent / "supervisor.py").read_text()
        tree = ast.parse(src)
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module.rsplit(".", 1)[-1])
        self.assertNotIn("consume", imported)
        self.assertIn("save_cursor", src)

    def test_restart_after_commit_does_not_reprompt(self) -> None:
        token = "rst_" + secrets.token_hex(8)
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "hcom.db"
            cursor = Path(td) / "gseat.last_id"
            eid = _write_db(db, token)
            first = FakeAcp()
            last, _ = consume.consume_once(db, 0, "gsea", "sess", first.rpc, set())
            consume.commit_cursor(cursor, last)
            saved = consume.load_cursor(cursor)
            self.assertEqual(saved, last)
            self.assertGreaterEqual(saved, eid)
            second = FakeAcp()
            consume.consume_once(db, saved, "gsea", "sess-restart", second.rpc, set())
            self.assertEqual(len(first.calls), 1)
            self.assertEqual(first.calls[0][0], "session/prompt")
            self.assertIn(token, first.calls[0][1]["prompt"][0]["text"])
            self.assertEqual(second.calls, [])

    def test_source_never_invokes_hcom_listen(self) -> None:
        src = Path(consume.__file__).read_text()
        tree = ast.parse(src)
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module.split(".")[0])
        self.assertNotIn("subprocess", imported)
        self.assertNotIn("os.system", src)
        # The consume path is sqlite + acp_rpc only.
        self.assertIn("session/prompt", src)
        self.assertIn("sendNow", src)


if __name__ == "__main__":
    unittest.main()
