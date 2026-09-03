"""Focused tests for room-scoped HCOM Grok seat registration."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from .registry import SeatRegistry, normalize_seat


class SeatRegistryTests(unittest.TestCase):
    def test_registers_independent_seats_and_tracks_latest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {
                "HCOM_GROK_MANAGED_STATE_ROOT": str(root / "state"),
                "HCOM_GROK_LOG_ROOT": str(root / "logs"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                registry = SeatRegistry(root / "room" / "hcom.db")
                first = registry.register("mugi", root / "project-a")
                second = registry.register("luna", root / "project-b")

                self.assertNotEqual(first["state_root"], second["state_root"])
                self.assertEqual(registry.get()["name"], "luna")
                self.assertEqual(registry.get("@mugi")["name"], "mugi")
                registry.touch("mugi")
                self.assertEqual(registry.get()["name"], "mugi")
                self.assertEqual([item["name"] for item in registry.list()], ["mugi", "luna"])
                self.assertEqual(registry.index_path.stat().st_mode & 0o777, 0o600)
                self.assertEqual(registry.root.stat().st_mode & 0o777, 0o700)

    def test_same_seat_name_is_isolated_between_hcom_rooms(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {
                "HCOM_GROK_MANAGED_STATE_ROOT": str(root / "state"),
                "HCOM_GROK_LOG_ROOT": str(root / "logs"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                date = SeatRegistry(root / "date-room" / "hcom.db")
                dna = SeatRegistry(root / "dna-room" / "hcom.db")
                date_record = date.register("mugi", root / "date-project")
                dna_record = dna.register("mugi", root / "dna-project")

                self.assertNotEqual(date.root, dna.root)
                self.assertNotEqual(date_record["state_root"], dna_record["state_root"])
                self.assertEqual(date.get("mugi")["project"], str((root / "date-project").resolve()))
                self.assertEqual(dna.get("mugi")["project"], str((root / "dna-project").resolve()))

    def test_latest_resume_skips_newer_seat_without_a_saved_session(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {
                "HCOM_GROK_MANAGED_STATE_ROOT": str(root / "state"),
                "HCOM_GROK_LOG_ROOT": str(root / "logs"),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                registry = SeatRegistry(root / "room" / "hcom.db")
                resumable = registry.register("mugi", root / "project-a")
                state_root = Path(resumable["state_root"])
                state_root.mkdir(parents=True)
                (state_root / "session.json").write_text(
                    json.dumps({"session_id": "session-1", "project": resumable["project"]})
                )
                registry.register("rovi", root / "project-b")

                self.assertEqual(registry.get()["name"], "rovi")
                self.assertEqual(registry.get_resumable()["name"], "mugi")
                self.assertEqual(registry.get_resumable("rovi")["name"], "rovi")

    def test_imports_matching_legacy_seat_without_moving_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            home = root / "home"
            legacy = home / ".local/state/hcom-grok/current"
            legacy.mkdir(parents=True)
            project = root / "project"
            project.mkdir()
            hcom_db = home / ".hcom/hcom.db"
            hcom_db.parent.mkdir(parents=True)
            (legacy / "run.json").write_text(
                json.dumps({"seat": "gsea", "project": str(project), "started_ns": 42})
            )
            (legacy / "session.json").write_text(
                json.dumps({"session_id": "session-1", "project": str(project)})
            )
            (legacy / "cursor.json").write_text(json.dumps({"db_path": str(hcom_db)}))
            env = {
                "HCOM_GROK_MANAGED_STATE_ROOT": str(root / "managed"),
                "HCOM_GROK_LOG_ROOT": str(root / "logs"),
            }
            with mock.patch.dict(os.environ, env, clear=False), mock.patch(
                "scripts.hcom_grok_seat.registry.Path.home", return_value=home
            ):
                registry = SeatRegistry(hcom_db)
                record = registry.get("gsea")

            self.assertTrue(record["legacy"])
            self.assertEqual(record["state_root"], str(legacy.resolve()))
            self.assertTrue((legacy / "session.json").is_file())

    def test_normalize_seat_accepts_hcom_mentions_and_rejects_bad_names(self) -> None:
        self.assertEqual(normalize_seat("@mugi"), "mugi")
        with self.assertRaises(ValueError):
            normalize_seat("bad seat")


if __name__ == "__main__":
    unittest.main()
