from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from .grok_updates import GrokUpdateError, scan_prompt_updates


def update(
    session: str,
    event: str,
    prompt: str,
    kind: str,
    *,
    method: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    body = {"sessionUpdate": kind, **extra}
    return {
        "method": method
        or ("session/update" if kind == "agent_message_chunk" else "_x.ai/session/update"),
        "params": {
            "sessionId": session,
            "_meta": {"eventId": event, "promptId": prompt},
            "update": body,
        },
        "timestamp": 1,
    }


def live(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "method": record["method"],
        "params": record["params"],
    }


def admission(event: str = "event-1", prompt: str = "p") -> dict[str, Any]:
    return update(
        "s",
        event,
        prompt,
        "hook_execution",
        event_name="user_prompt_submit",
        prompt_id=prompt,
    )


def completion(
    event: str = "event-4",
    prompt: str = "p",
    reason: str = "end_turn",
) -> dict[str, Any]:
    return update(
        "s",
        event,
        prompt,
        "turn_completed",
        prompt_id=prompt,
        stop_reason=reason,
    )


def write_records(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_text("".join(json.dumps(item) + "\n" for item in records))


class GrokUpdateTests(unittest.TestCase):
    def test_scanner_correlates_admission_text_and_completion(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            records = [
                admission(),
                update(
                    "s",
                    "event-2",
                    "p",
                    "agent_message_chunk",
                    content={"type": "text", "text": "hello "},
                ),
                update(
                    "s",
                    "event-3",
                    "p",
                    "agent_message_chunk",
                    content={"type": "text", "text": "world"},
                ),
                completion(),
            ]
            write_records(path, records)
            evidence = scan_prompt_updates(path, "s", "p", buffered_live=())
            self.assertTrue(evidence.admitted)
            self.assertTrue(evidence.completed)
            self.assertEqual(evidence.assistant_text, "hello world")
            self.assertEqual(evidence.stop_reason, "end_turn")
            self.assertEqual(evidence.admission_count, 1)
            self.assertEqual(evidence.completion_count, 1)
            self.assertFalse(evidence.not_found)

    def test_trailing_partial_is_inconclusive_and_complete_corruption_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            path.write_bytes((json.dumps(admission()) + "\n{").encode())
            evidence = scan_prompt_updates(path, "s", "p", buffered_live=())
            self.assertTrue(evidence.partial_tail)
            self.assertFalse(evidence.not_found)
            path.write_bytes(b"not-json\n")
            with self.assertRaises(GrokUpdateError):
                scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_identical_duplicate_event_is_ignored_but_conflict_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            record = admission()
            write_records(path, [record, record])
            evidence = scan_prompt_updates(path, "s", "p", buffered_live=())
            self.assertEqual(evidence.admission_count, 1)

            conflict = completion(event="event-1")
            write_records(path, [record, conflict])
            with self.assertRaisesRegex(GrokUpdateError, "contradictory"):
                scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_disk_and_buffered_live_are_both_reconciled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            first = admission()
            write_records(path, [first])
            live_chunk = update(
                "s",
                "event-2",
                "p",
                "agent_message_chunk",
                content={"type": "text", "text": "answer"},
            )
            evidence = scan_prompt_updates(
                path,
                "s",
                "p",
                buffered_live=(live(first), live(live_chunk), live(completion())),
            )
            self.assertTrue(evidence.completed)
            self.assertEqual(evidence.assistant_text, "answer")
            self.assertEqual(evidence.live_event_count, 2)
            self.assertEqual(
                evidence.event_ids, ("event-1", "event-2", "event-4")
            )

    def test_missing_file_cannot_prove_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "missing.jsonl"
            with self.assertRaisesRegex(GrokUpdateError, "missing"):
                scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_complete_empty_disk_and_live_cut_can_prove_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            path.write_bytes(b"")
            evidence = scan_prompt_updates(path, "s", "p", buffered_live=())
            self.assertTrue(evidence.not_found)

    def test_missing_or_wrong_session_and_unknown_method_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            missing_session = admission()
            del missing_session["params"]["sessionId"]
            wrong_session = admission()
            wrong_session["params"]["sessionId"] = "other"
            unknown_method = admission()
            unknown_method["method"] = "x.ai/unknown"
            for record in (missing_session, wrong_session, unknown_method):
                with self.subTest(record=record):
                    write_records(path, [record])
                    with self.assertRaises(GrokUpdateError):
                        scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_conflicting_prompt_ids_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            record = admission()
            record["params"]["_meta"]["promptId"] = "other"
            write_records(path, [record])
            with self.assertRaisesRegex(GrokUpdateError, "conflicting prompt IDs"):
                scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_duplicate_admission_and_completion_block(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            cases = (
                [admission(), admission("event-2")],
                [admission(), completion("event-2"), completion("event-3")],
            )
            for records in cases:
                with self.subTest(records=records):
                    write_records(path, records)
                    with self.assertRaises(GrokUpdateError):
                        scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_bad_prompt_event_order_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            chunk = update(
                "s",
                "event-2",
                "p",
                "agent_message_chunk",
                content={"type": "text", "text": "early"},
            )
            late = update(
                "s",
                "event-5",
                "p",
                "agent_message_chunk",
                content={"type": "text", "text": "late"},
            )
            for records in ([chunk, admission()], [admission(), completion(), late]):
                with self.subTest(records=records):
                    write_records(path, list(records))
                    with self.assertRaisesRegex(GrokUpdateError, "interval"):
                        scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_replay_cannot_cross_buffer_fence(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            path.write_bytes(b"")
            replay = admission()
            replay["params"]["_meta"]["isReplay"] = True
            with self.assertRaisesRegex(GrokUpdateError, "replay"):
                scan_prompt_updates(path, "s", "p", buffered_live=(live(replay),))

    def test_file_and_line_bounds_are_enforced_streaming(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            encoded = (json.dumps(admission()) + "\n").encode()
            path.write_bytes(encoded)
            with self.assertRaisesRegex(GrokUpdateError, "journal exceeds"):
                scan_prompt_updates(
                    path,
                    "s",
                    "p",
                    buffered_live=(),
                    max_file_bytes=len(encoded) - 1,
                )
            with self.assertRaisesRegex(GrokUpdateError, "line 1 exceeds"):
                scan_prompt_updates(
                    path,
                    "s",
                    "p",
                    buffered_live=(),
                    max_line_bytes=32,
                )

    def test_live_and_assistant_buffers_are_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            write_records(path, [admission()])
            chunk = live(
                update(
                    "s",
                    "event-2",
                    "p",
                    "agent_message_chunk",
                    content={"type": "text", "text": "answer"},
                )
            )
            with self.assertRaisesRegex(GrokUpdateError, "events"):
                scan_prompt_updates(
                    path,
                    "s",
                    "p",
                    buffered_live=(chunk, live(completion())),
                    max_live_events=1,
                )
            with self.assertRaisesRegex(GrokUpdateError, "live updates exceed 1 bytes"):
                scan_prompt_updates(
                    path,
                    "s",
                    "p",
                    buffered_live=(chunk,),
                    max_live_bytes=1,
                )
            with self.assertRaisesRegex(GrokUpdateError, "assistant text exceeds"):
                scan_prompt_updates(
                    path,
                    "s",
                    "p",
                    buffered_live=(chunk,),
                    max_assistant_bytes=1,
                )

    def test_persisted_envelope_and_finite_json_are_exact(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "updates.jsonl"
            wrong_envelope = admission()
            wrong_envelope["jsonrpc"] = "2.0"
            write_records(path, [wrong_envelope])
            with self.assertRaisesRegex(GrokUpdateError, "exact persisted"):
                scan_prompt_updates(path, "s", "p", buffered_live=())

            path.write_text(json.dumps(admission()).replace('"timestamp": 1', '"timestamp": NaN') + "\n")
            with self.assertRaisesRegex(GrokUpdateError, "malformed complete"):
                scan_prompt_updates(path, "s", "p", buffered_live=())

    def test_symlink_journal_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            target = root / "target.jsonl"
            target.write_bytes(b"")
            link = root / "updates.jsonl"
            link.symlink_to(target)
            with self.assertRaisesRegex(GrokUpdateError, "regular file"):
                scan_prompt_updates(link, "s", "p", buffered_live=())


if __name__ == "__main__":
    unittest.main()
