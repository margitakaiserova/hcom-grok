"""Synthetic-home tests for the visible-session observer (Task 1)."""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch
from urllib.parse import quote

from .acp_session import TESTED_GROK_VERSIONS
from . import visible_session
from .pager_status import (
    PagerStatusRead,
    PagerStatusSample,
    _encode_authenticated_record,
    cleanup_pager_status,
    prepare_pager_status,
    stage_pager_status,
)
from .visible_session import (
    observe_pager_session,
    observe_visible_session,
    session_directory_for,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _summary(
    *,
    session_id: str,
    cwd: str,
    grok_home: str,
    created_at: str = "2026-09-03T19:52:43.454149Z",
    git_root_dir: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "chat_format_version": 1,
        "created_at": created_at,
        "info": {"id": session_id, "cwd": cwd},
        "grok_home": grok_home,
        # Content-like fields must be ignored by the observer.
        "session_summary": "do-not-read",
        "last_turn_summary": "do-not-read",
        "generated_title": "do-not-read",
        "last_recap": "do-not-read",
    }
    if git_root_dir is not None:
        payload["git_root_dir"] = git_root_dir
    return payload


def _row(
    *,
    pid: int,
    cwd: str,
    session_id: str,
    opened_at: str = "2026-09-03T19:52:44.320217Z",
) -> dict[str, object]:
    return {
        "pid": pid,
        "cwd": cwd,
        "session_id": session_id,
        "opened_at": opened_at,
    }


class VisibleSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.grok_home = self.root / "seat-home" / ".grok"
        self.grok_home.mkdir(parents=True)
        self.pid = 4242
        self.session_a = str(uuid.uuid4())
        self.session_b = str(uuid.uuid4())
        self.alive = {self.pid}

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _pid_alive(self, pid: int) -> bool:
        return pid in self.alive

    def _install_session(
        self,
        session_id: str,
        *,
        cwd: str | None = None,
        grok_home: str | None = None,
        git_root_dir: str | None = None,
        summary: dict[str, object] | None = None,
    ) -> Path:
        cwd_value = cwd if cwd is not None else str(self.project)
        home_value = grok_home if grok_home is not None else str(self.grok_home)
        session_dir = session_directory_for(self.grok_home, self.project, session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        payload = summary or _summary(
            session_id=session_id,
            cwd=cwd_value,
            grok_home=home_value,
            git_root_dir=git_root_dir
            if git_root_dir is not None
            else str(self.project) + "/",
        )
        _write_json(session_dir / "summary.json", payload)
        return session_dir

    def _write_active(self, rows: list[dict[str, object]]) -> None:
        _write_json(self.grok_home / "active_sessions.json", rows)

    def _observe(self, bound: str, **kwargs: object):
        params = {
            "grok_home": self.grok_home,
            "project": self.project,
            "tui_pid": self.pid,
            "bound_session_id": bound,
            "agent_version": "1.0.13",
            "pid_alive": self._pid_alive,
        }
        params.update(kwargs)
        return observe_visible_session(**params)  # type: ignore[arg-type]

    def test_aligned_same_session(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "aligned")
        self.assertEqual(result.session_id, self.session_a)
        self.assertEqual(result.pid, self.pid)

    def test_registry_visible_change_for_clear_or_new(self) -> None:
        self._install_session(self.session_a)
        self._install_session(self.session_b)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project) + "/", session_id=self.session_b)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "visible-change")
        self.assertEqual(result.session_id, self.session_b)
        self.assertTrue(result.cwd.endswith("project"))

    def test_trailing_slash_paths_remain_aligned(self) -> None:
        self._install_session(
            self.session_a,
            cwd=str(self.project) + "/",
            grok_home=str(self.grok_home) + "/",
            git_root_dir=str(self.project) + "/",
        )
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project) + "/", session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "aligned")

    def test_grok_home_override_is_used_not_path_home_dot_grok(self) -> None:
        other_home = Path.home() / ".grok"
        # Poison a Path.home()/.grok active file if writable; observer must ignore it.
        # Prefer a private decoy under the temp root instead of touching the real home.
        decoy = self.root / "decoy-home" / ".grok"
        decoy.mkdir(parents=True)
        poison_id = str(uuid.uuid4())
        poison_dir = decoy / "sessions" / quote(str(self.project.resolve()), safe="") / poison_id
        poison_dir.mkdir(parents=True)
        _write_json(
            poison_dir / "summary.json",
            _summary(
                session_id=poison_id,
                cwd=str(self.project),
                grok_home=str(decoy),
            ),
        )
        _write_json(
            decoy / "active_sessions.json",
            [_row(pid=self.pid, cwd=str(self.project), session_id=poison_id)],
        )

        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a, grok_home=self.grok_home)
        self.assertEqual(result.kind, "aligned")
        self.assertEqual(result.session_id, self.session_a)
        self.assertNotEqual(self.grok_home.resolve(), other_home.resolve())

    def test_wrong_summary_grok_home_is_unsafe(self) -> None:
        foreign = self.root / "other-seat" / ".grok"
        foreign.mkdir(parents=True)
        self._install_session(self.session_a, grok_home=str(foreign))
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("grok_home", result.reason)

    def test_two_seat_homes_cannot_observe_each_other(self) -> None:
        other_project = self.root / "other-project"
        other_project.mkdir()
        other_home = self.root / "other-seat" / ".grok"
        other_home.mkdir(parents=True)
        other_id = str(uuid.uuid4())
        other_dir = session_directory_for(other_home, other_project, other_id)
        other_dir.mkdir(parents=True)
        _write_json(
            other_dir / "summary.json",
            _summary(
                session_id=other_id,
                cwd=str(other_project),
                grok_home=str(other_home),
            ),
        )
        _write_json(
            other_home / "active_sessions.json",
            [_row(pid=self.pid, cwd=str(other_project), session_id=other_id)],
        )

        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        mine = self._observe(self.session_a)
        theirs = observe_visible_session(
            grok_home=other_home,
            project=other_project,
            tui_pid=self.pid,
            bound_session_id=other_id,
            agent_version="1.0.13",
            pid_alive=self._pid_alive,
        )
        self.assertEqual(mine.kind, "aligned")
        self.assertEqual(mine.session_id, self.session_a)
        self.assertEqual(theirs.kind, "aligned")
        self.assertEqual(theirs.session_id, other_id)
        self.assertNotEqual(mine.session_directory, theirs.session_directory)

    def test_zero_rows_for_pid_is_transient(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=9999, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "transient-missing")

    def test_duplicate_rows_for_pid_are_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._install_session(self.session_b)
        self._write_active(
            [
                _row(pid=self.pid, cwd=str(self.project), session_id=self.session_a),
                _row(pid=self.pid, cwd=str(self.project), session_id=self.session_b),
            ]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("multiple", result.reason)

    def test_wrong_cwd_is_unsafe(self) -> None:
        other = self.root / "elsewhere"
        other.mkdir()
        self._install_session(self.session_a, cwd=str(other))
        # Row cwd wrong relative to configured project.
        self._write_active(
            [_row(pid=self.pid, cwd=str(other), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("cwd", result.reason)

    def test_invalid_uuid_and_timestamp_are_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [
                {
                    "pid": self.pid,
                    "cwd": str(self.project),
                    "session_id": "not-a-uuid",
                    "opened_at": "2026-09-03T19:52:44.320217Z",
                }
            ]
        )
        bad_uuid = self._observe(self.session_a)
        self.assertEqual(bad_uuid.kind, "unsafe")

        self._write_active(
            [
                {
                    "pid": self.pid,
                    "cwd": str(self.project),
                    "session_id": self.session_a,
                    "opened_at": "yesterday",
                }
            ]
        )
        bad_time = self._observe(self.session_a)
        self.assertEqual(bad_time.kind, "unsafe")

    def test_object_top_level_active_sessions_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        _write_json(
            self.grok_home / "active_sessions.json",
            _row(pid=self.pid, cwd=str(self.project), session_id=self.session_a),
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("array", result.reason)

    def test_absent_session_directory_is_unsafe(self) -> None:
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("absent", result.reason)

    def test_symlink_escape_session_path_is_unsafe(self) -> None:
        outside = self.root / "outside-session"
        outside.mkdir()
        _write_json(
            outside / "summary.json",
            _summary(
                session_id=self.session_a,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
            ),
        )
        sessions_root = self.grok_home / "sessions"
        project_key = quote(str(self.project.resolve()), safe="")
        link_parent = sessions_root / project_key
        link_parent.mkdir(parents=True, exist_ok=True)
        link = link_parent / self.session_a
        link.symlink_to(outside, target_is_directory=True)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("escapes", result.reason)

    def test_dead_pid_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        self.alive.clear()
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("not alive", result.reason)

    def test_untested_agent_version_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a, agent_version="9.9.9")
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("allowlist", result.reason)
        self.assertIn("1.0.13", TESTED_GROK_VERSIONS)

    def test_missing_active_sessions_is_transient(self) -> None:
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "transient-missing")

    def test_git_root_trailing_slash_is_diagnostic_only(self) -> None:
        self._install_session(self.session_a, git_root_dir=str(self.project) + "/")
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "aligned")
        self.assertTrue(str(result.git_root_dir or "").endswith("/"))

    def test_does_not_require_content_fields(self) -> None:
        session_dir = session_directory_for(self.grok_home, self.project, self.session_a)
        session_dir.mkdir(parents=True)
        _write_json(
            session_dir / "summary.json",
            {
                "chat_format_version": 1,
                "created_at": "2026-09-03T19:52:43.454149Z",
                "info": {"id": self.session_a, "cwd": str(self.project)},
                "grok_home": str(self.grok_home),
            },
        )
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "aligned")

    def test_malformed_foreign_row_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [
                {
                    "pid": 9999,
                    "cwd": str(self.project),
                    "session_id": "bad",
                    "opened_at": "2026-09-03T19:52:44.320217Z",
                },
                _row(pid=self.pid, cwd=str(self.project), session_id=self.session_a),
            ]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("UUID", result.reason)

    def test_naive_timestamp_without_timezone_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [
                {
                    "pid": self.pid,
                    "cwd": str(self.project),
                    "session_id": self.session_a,
                    "opened_at": "2026-09-03T19:52:44.320217",
                }
            ]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("timezone-bearing", result.reason)

    def test_naive_summary_created_at_is_unsafe(self) -> None:
        self._install_session(
            self.session_a,
            summary=_summary(
                session_id=self.session_a,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
                created_at="2026-09-03T19:52:43.454149",
            ),
        )
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("created_at", result.reason)

    def test_chat_format_version_must_be_one(self) -> None:
        self._install_session(
            self.session_a,
            summary=_summary(
                session_id=self.session_a,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
            )
            | {"chat_format_version": 2},
        )
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("chat_format_version", result.reason)

    def test_active_sessions_symlink_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        real = self.root / "real-active.json"
        _write_json(
            real,
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)],
        )
        target = self.grok_home / "active_sessions.json"
        target.symlink_to(real)
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("symlink", result.reason)

    def test_summary_symlink_is_unsafe(self) -> None:
        session_dir = session_directory_for(self.grok_home, self.project, self.session_a)
        session_dir.mkdir(parents=True)
        real = self.root / "real-summary.json"
        _write_json(
            real,
            _summary(
                session_id=self.session_a,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
            ),
        )
        (session_dir / "summary.json").symlink_to(real)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("symlink", result.reason)

    def test_sessions_root_symlink_is_unsafe(self) -> None:
        real_sessions = self.root / "real-sessions"
        real_sessions.mkdir()
        (self.grok_home / "sessions").symlink_to(real_sessions)
        # Still plant a session under the real tree and point active at it.
        session_dir = session_directory_for(self.grok_home, self.project, self.session_a)
        # session_directory_for resolves home; create under symlink destination.
        session_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            session_dir / "summary.json",
            _summary(
                session_id=self.session_a,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
            ),
        )
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("sessions root", result.reason)

    def test_lock_file_is_never_created(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        lock_path = self.grok_home / "active_sessions.lock"
        self.assertFalse(lock_path.exists())
        result = self._observe(self.session_a)
        self.assertEqual(result.kind, "aligned")
        self.assertFalse(lock_path.exists())

    def test_lock_contention_is_transient(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        with patch.object(visible_session, "_try_shared_lock", return_value="contended"):
            result = self._observe(self.session_a)
        self.assertEqual(result.kind, "transient-missing")
        self.assertIn("contended", result.reason)

    def test_active_file_identity_change_is_transient(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        original = visible_session._read_regular_file
        active_reads = 0

        def unstable(path: Path, *, label: str):
            nonlocal active_reads
            result = original(path, label=label)
            if label == "active_sessions.json" and not isinstance(result, str):
                active_reads += 1
                if active_reads == 2:
                    data, identity = result
                    result = data, (
                        identity[0],
                        identity[1],
                        identity[2] + 1,
                        identity[3],
                    )
            return result

        with patch.object(visible_session, "_read_regular_file", side_effect=unstable):
            result = self._observe(self.session_a)
        self.assertEqual(result.kind, "transient-missing")
        self.assertIn("changed", result.reason)

    def test_pid_rechecked_before_success(self) -> None:
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        calls = {"n": 0}

        def flaky(pid: int) -> bool:
            calls["n"] += 1
            if calls["n"] == 1:
                return True
            return False

        result = self._observe(self.session_a, pid_alive=flaky)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("not alive", result.reason)
        self.assertGreaterEqual(calls["n"], 2)

    def test_same_project_and_pid_two_homes_stay_isolated(self) -> None:
        other_home = self.root / "seat-two" / ".grok"
        other_home.mkdir(parents=True)
        other_id = str(uuid.uuid4())
        other_dir = session_directory_for(other_home, self.project, other_id)
        other_dir.mkdir(parents=True)
        _write_json(
            other_dir / "summary.json",
            _summary(
                session_id=other_id,
                cwd=str(self.project),
                grok_home=str(other_home),
            ),
        )
        _write_json(
            other_home / "active_sessions.json",
            [_row(pid=self.pid, cwd=str(self.project), session_id=other_id)],
        )
        self._install_session(self.session_a)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        mine = self._observe(self.session_a)
        theirs = observe_visible_session(
            grok_home=other_home,
            project=self.project,
            tui_pid=self.pid,
            bound_session_id=other_id,
            agent_version="1.0.13",
            pid_alive=self._pid_alive,
        )
        self.assertEqual(mine.kind, "aligned")
        self.assertEqual(mine.session_id, self.session_a)
        self.assertEqual(theirs.kind, "aligned")
        self.assertEqual(theirs.session_id, other_id)
        self.assertNotEqual(mine.grok_home, theirs.grok_home)


class PagerVisibleSessionTests(unittest.TestCase):
    """Pager focus is authoritative even when the legacy PID map is stale."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.project = self.root / "project"
        self.project.mkdir()
        self.home = self.root / "seat-home"
        self.grok_home = self.home / ".grok"
        self.state_root = self.home / ".local/state/hcom-grok/current"
        self.host_home = self.root / "host-home"
        for path in (self.home, self.grok_home, self.state_root, self.host_home):
            path.mkdir(parents=True, exist_ok=True)
            os.chmod(path, 0o700)
        self.pager = prepare_pager_status(
            state_root=self.state_root,
            launch_home=self.home,
            grok_home=self.grok_home,
            isolated_home=True,
            environment={},
            host_home=self.host_home,
            system_grok_root=self.root / "etc-grok",
        )
        self.assertTrue(self.pager.enabled, self.pager.reason)
        self.assertIsNone(stage_pager_status(self.pager))
        self.pid = 4242
        self.session_a = str(uuid.uuid4())
        self.session_b = str(uuid.uuid4())
        self.alive = {self.pid}

    def tearDown(self) -> None:
        cleanup_pager_status(self.pager)
        self._tmp.cleanup()

    def _pid_alive(self, pid: int) -> bool:
        return pid in self.alive

    def _install_session(self, session_id: str) -> Path:
        session_dir = session_directory_for(
            self.grok_home, self.project, session_id
        )
        session_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            session_dir / "summary.json",
            _summary(
                session_id=session_id,
                cwd=str(self.project),
                grok_home=str(self.grok_home),
                git_root_dir=str(self.project),
            ),
        )
        return session_dir

    def _write_active(self, rows: list[dict[str, object]]) -> None:
        _write_json(self.grok_home / "active_sessions.json", rows)

    def _observe_registry(self, bound: str):
        return observe_visible_session(
            grok_home=self.grok_home,
            project=self.project,
            tui_pid=self.pid,
            bound_session_id=bound,
            agent_version="1.0.13",
            pid_alive=self._pid_alive,
        )

    def _write_status(
        self,
        session_id: str,
        *,
        cwd: str | None = None,
        captured_monotonic_ns: int | None = None,
        trigger: str = "state",
    ) -> int:
        captured = captured_monotonic_ns or time.monotonic_ns()
        record = {
                "schema": 1,
                "session_id": session_id,
                "cwd": cwd or str(self.project),
                "status_schema_version": 1,
                "grok_version": "1.0.13",
                "trigger": trigger,
                "tui_pid": self.pid,
                "captured_monotonic_ns": captured,
                "captured_wall_ns": time.time_ns(),
                "state_session_id": session_id,
                "state_cwd": cwd or str(self.project),
                "state_observed_monotonic_ns": captured,
            }
        self.pager.status_path.write_bytes(
            _encode_authenticated_record(record, self.pager.token)
        )
        os.chmod(self.pager.status_path, 0o600)
        return captured

    def _observe_pager(self, bound: str, **kwargs: object):
        params = {
            "setup": self.pager,
            "grok_home": self.grok_home,
            "project": self.project,
            "tui_pid": self.pid,
            "bound_session_id": bound,
            "agent_version": "1.0.13",
            "pid_alive": self._pid_alive,
        }
        params.update(kwargs)
        return observe_pager_session(**params)  # type: ignore[arg-type]

    def test_pager_aligned_sample_is_focus_authority(self) -> None:
        self._install_session(self.session_a)
        captured = self._write_status(self.session_a)
        result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "aligned")
        self.assertEqual(result.focus_source, "pager-status")
        self.assertEqual(result.status_trigger, "state")
        self.assertGreaterEqual(result.status_sample_monotonic_ns or 0, captured)

    def test_resume_follows_pager_while_active_registry_stays_stale(self) -> None:
        self._install_session(self.session_a)
        self._install_session(self.session_b)
        self._write_active(
            [_row(pid=self.pid, cwd=str(self.project), session_id=self.session_a)]
        )
        self._write_status(self.session_b)
        result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "visible-change")
        self.assertEqual(result.session_id, self.session_b)
        registry = self._observe_registry(self.session_a)
        self.assertEqual(registry.kind, "aligned")
        self.assertEqual(registry.session_id, self.session_a)

    def test_duplicate_active_registry_rows_do_not_override_pager(self) -> None:
        self._install_session(self.session_a)
        self._install_session(self.session_b)
        self._write_active(
            [
                _row(pid=self.pid, cwd=str(self.project), session_id=self.session_a),
                _row(pid=self.pid, cwd=str(self.project), session_id=self.session_b),
            ]
        )
        self._write_status(self.session_b)
        result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "visible-change")
        self.assertEqual(result.session_id, self.session_b)

    def test_delivery_gate_requires_a_post_gate_sample(self) -> None:
        self._install_session(self.session_a)
        captured = self._write_status(self.session_a)
        result = self._observe_pager(
            self.session_a,
            minimum_monotonic_ns=captured + 1,
        )
        self.assertEqual(result.kind, "transient-missing")
        self.assertIn("predates delivery gate", result.reason)

    def test_wrong_pager_cwd_is_unsafe(self) -> None:
        self._install_session(self.session_a)
        elsewhere = self.root / "elsewhere"
        elsewhere.mkdir()
        self._write_status(self.session_a, cwd=str(elsewhere))
        result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("cwd", result.reason)

    def test_summary_creation_race_is_transient(self) -> None:
        session_dir = session_directory_for(
            self.grok_home, self.project, self.session_a
        )
        session_dir.mkdir(parents=True)
        self._write_status(self.session_a)
        result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "transient-missing")
        self.assertIn("summary.json", result.reason)

    def test_rapid_pager_switch_is_never_committed(self) -> None:
        self._install_session(self.session_b)
        session_c = str(uuid.uuid4())
        self._install_session(session_c)
        now = time.monotonic_ns()
        first = PagerStatusRead(
            "valid",
            "first",
            PagerStatusSample(
                self.session_b,
                str(self.project),
                1,
                "1.0.13",
                "state",
                self.pid,
                now,
                time.time_ns(),
                self.session_b,
                str(self.project),
                now,
            ),
        )
        second = PagerStatusRead(
            "valid",
            "second",
            PagerStatusSample(
                session_c,
                str(self.project),
                1,
                "1.0.13",
                "state",
                self.pid,
                now + 1,
                time.time_ns(),
                session_c,
                str(self.project),
                now + 1,
            ),
        )
        with patch.object(
            visible_session,
            "read_pager_status",
            side_effect=(first, second),
        ):
            result = self._observe_pager(self.session_a)
        self.assertEqual(result.kind, "transient-missing")
        self.assertIn("changed during confirmation", result.reason)


if __name__ == "__main__":
    unittest.main()
