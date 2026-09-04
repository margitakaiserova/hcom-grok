"""Focused lifecycle and safety tests for the hcom-grok operator."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest import mock

from . import operator


def _config(root: Path) -> operator.Config:
    state = root / "state" / "current"
    return operator.Config(
        state_root=state,
        log_root=root / "logs",
        release_root=root / "releases",
        bin_root=root / "bin",
        hcom_db=root / "hcom.db",
        cursor_path=state / "cursor.json",
        grok_home=root / "grok-home",
        project=root / "project",
        seat="gsea",
        grok_bin="grok-test",
        hcom_bin="hcom-test",
    )


def _events_db(path: Path, count: int = 3) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA user_version=18")
        connection.execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, "
            "type TEXT NOT NULL, instance TEXT NOT NULL, data TEXT NOT NULL)"
        )
        connection.execute("CREATE TABLE instances (name TEXT PRIMARY KEY)")
        connection.execute("CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        for number in range(1, count + 1):
            connection.execute(
                "INSERT INTO events(id,timestamp,type,instance,data) VALUES (?,?,?,?,?)",
                (number, "2026-08-31T00:00:00", "message", "sender", json.dumps({"text": f"message-{number}"})),
            )
        connection.commit()
    finally:
        connection.close()


def _run_state(root: Path, pid: int = 4321) -> dict[str, object]:
    return {
        "supervisor_pid": pid,
        "tui_pid": 4322,
        "socket_path": str(root / "control.sock"),
        "session_id": "session-test",
        "project": str(root / "project"),
        "seat": "gsea",
        "started_ns": 123,
        "release": str(root / "release-test"),
        "run_token": "run-token-test",
        "background_tui": False,
        "ready": True,
        "supervisor_executable": str(Path(operator.sys.executable).resolve()),
        "argv_marker": "hcom-grok:0123456789abcdef",
    }


class OperatorTests(unittest.TestCase):
    def test_environment_overrides_all_external_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = {
                "HCOM_GROK_STATE_ROOT": str(root / "s"),
                "HCOM_GROK_LOG_ROOT": str(root / "l"),
                "HCOM_GROK_RELEASE_ROOT": str(root / "r"),
                "HCOM_GROK_BIN_ROOT": str(root / "b"),
                "HCOM_GROK_HCOM_DB": str(root / "db"),
                "HCOM_GROK_CURSOR": str(root / "c"),
                "GROK_HOME": str(root / "grok-home"),
                "HCOM_GROK_PROJECT": str(root / "p"),
                "HCOM_GROK_SEAT": "seat-test",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                config = operator.load_config()
            self.assertEqual(config.state_root, (root / "s").resolve())
            self.assertEqual(config.log_root, (root / "l").resolve())
            self.assertEqual(config.release_root, (root / "r").resolve())
            self.assertEqual(config.bin_root, (root / "b").resolve())
            self.assertEqual(config.hcom_db, (root / "db").resolve())
            self.assertEqual(config.cursor_path, (root / "c").resolve())
            self.assertEqual(config.grok_home, (root / "grok-home").resolve())
            self.assertEqual(config.project, (root / "p").resolve())
            self.assertEqual(config.seat, "seat-test")

    def test_saved_project_and_seat_are_reused_without_environment_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            project = root / "persistent-project"
            state.mkdir()
            project.mkdir()
            (state / "session.json").write_text(
                json.dumps({"project": str(project), "session_id": "session-1"})
            )
            (state / "run.json").write_text(json.dumps({"seat": "saved-seat"}))
            with mock.patch.dict(
                os.environ,
                {"HCOM_GROK_STATE_ROOT": str(state)},
                clear=True,
            ):
                config = operator.load_config()
            self.assertEqual(config.project, project.resolve())
            self.assertEqual(config.seat, "saved-seat")

    def test_grok_home_defaults_to_home_dot_grok(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with mock.patch.dict(
                os.environ,
                {
                    "HOME": str(root / "home"),
                    "HCOM_GROK_STATE_ROOT": str(root / "state"),
                },
                clear=True,
            ):
                config = operator.load_config()
            self.assertEqual(config.grok_home, (root / "home" / ".grok").resolve())

    def test_supervisor_command_exposes_full_runtime_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = _config(Path(temporary))
            command = operator.supervisor_command(config, "hcom-grok:marker", True)
            self.assertEqual(command[:4], [operator.sys.executable, "-m", operator.SUPERVISOR_MODULE, "run"])
            self.assertIn("--operator-marker", command)
            self.assertIn("hcom-grok:marker", command)
            self.assertIn("--background-tui", command)

    def test_start_exports_the_package_root_for_supervisor_imports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            captured: dict[str, str] = {}

            class Finished:
                returncode = 0

            def fake_run(command: list[str], **kwargs: object) -> Finished:
                captured.update(kwargs["env"])  # type: ignore[arg-type]
                return Finished()

            with mock.patch.object(operator.subprocess, "run", side_effect=fake_run):
                result = operator.start(config, background=False, session_mode="new")
            self.assertTrue(result["ok"])
            package_root = str(Path(operator.__file__).resolve().parents[2])
            self.assertEqual(captured["PYTHONPATH"].split(os.pathsep)[0], package_root)
            self.assertEqual(captured["HCOM_GROK_SESSION_MODE"], "new")
            self.assertEqual(captured["GROK_HOME"], str(config.grok_home))

    def test_fresh_config_uses_current_project_instead_of_saved_project(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            saved = root / "saved"
            current = root / "current"
            state.mkdir()
            saved.mkdir()
            current.mkdir()
            (state / "session.json").write_text(
                json.dumps({"project": str(saved), "session_id": "session-1"})
            )
            with mock.patch.dict(
                os.environ,
                {"HCOM_GROK_STATE_ROOT": str(state)},
                clear=True,
            ), mock.patch.object(operator.Path, "cwd", return_value=current):
                config = operator.load_config(fresh_project=True)
            self.assertEqual(config.project, current.resolve())

    def test_resume_without_saved_session_has_actionable_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = _config(Path(temporary))
            with self.assertRaisesRegex(RuntimeError, "Run hcom-grok to start fresh"):
                operator.require_resumable_session(config)

    def test_resume_rejects_stale_saved_session_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.project.mkdir()
            config.state_root.mkdir(parents=True)
            config.session_path.write_text(
                json.dumps({"project": str(config.project), "session_id": "missing-session"})
            )
            with mock.patch.object(
                operator,
                "_grok_session_directory",
                return_value=root / "missing-grok-session",
            ):
                with self.assertRaisesRegex(RuntimeError, "no longer available"):
                    operator.require_resumable_session(config)

    def test_resume_uses_configured_grok_home_instead_of_path_home(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            configured_home = root / "configured-grok-home"
            decoy_home = root / "decoy-home"
            project = root / "project"
            state = root / "state"
            session_id = "session-in-configured-home"
            project.mkdir()
            state.mkdir()
            (state / "session.json").write_text(
                json.dumps({"project": str(project), "session_id": session_id})
            )
            session_dir = operator._grok_session_directory(
                configured_home.resolve(), project.resolve(), session_id
            )
            session_dir.mkdir(parents=True)
            env = {
                "HOME": str(decoy_home),
                "GROK_HOME": str(configured_home),
                "HCOM_GROK_STATE_ROOT": str(state),
                "HCOM_GROK_PROJECT": str(project),
            }
            with mock.patch.dict(os.environ, env, clear=True):
                config = operator.load_config()
                saved = operator.require_resumable_session(config)
            self.assertEqual(config.grok_home, configured_home.resolve())
            self.assertNotEqual(config.grok_home, (decoy_home / ".grok").resolve())
            self.assertEqual(saved["session_id"], session_id)

    def test_start_refuses_live_pid_with_incomplete_run_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.project.mkdir()
            config.current_state.mkdir(parents=True)
            os.chmod(config.current_state, 0o700)
            config.run_path.write_text(json.dumps({"supervisor_pid": 4321}))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator.subprocess, "run"
            ) as run:
                with self.assertRaisesRegex(RuntimeError, "invalid run state"):
                    operator.start(config)
            run.assert_not_called()

    def test_fresh_start_refuses_to_replace_owned_live_session(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.project.mkdir()
            config.current_state.mkdir(parents=True)
            os.chmod(config.current_state, 0o700)
            config.run_path.write_text(json.dumps(_run_state(root)))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ), mock.patch.object(operator.subprocess, "run") as run:
                with self.assertRaisesRegex(RuntimeError, "hcom-grok stop"):
                    operator.start(config, session_mode="new")
            run.assert_not_called()

    def test_resume_of_owned_live_session_returns_complete_status(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.project.mkdir()
            config.current_state.mkdir(parents=True)
            os.chmod(config.current_state, 0o700)
            state = _run_state(root)
            state["launch_mode"] = "resumed"
            state["busy"] = False
            config.run_path.write_text(json.dumps(state))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ):
                result = operator.start(config, session_mode="resume")
            self.assertTrue(result["running"])
            self.assertTrue(result["already_running"])
            self.assertEqual(result["launch_mode"], "resumed")
            self.assertEqual(result["session_id"], "session-test")

    def test_starting_run_state_allows_null_tui_until_ready(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = _run_state(Path(temporary))
            state["ready"] = False
            state["tui_pid"] = None
            state["starting"] = True
            self.assertEqual(operator.validate_run_state(state), [])

    def test_status_exposes_binding_alignment_for_new_and_legacy_run_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            state = _run_state(root)
            config.run_path.write_text(json.dumps(state))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ):
                legacy = operator.status(config)
            self.assertEqual(legacy["bound_session_id"], "session-test")
            self.assertIsNone(legacy["visible_session_id"])
            self.assertEqual(legacy["session_alignment"], "unknown")
            self.assertEqual(legacy["binding_generation"], 0)
            self.assertFalse(legacy["pager_status_enabled"])
            self.assertIsNone(legacy["focus_source"])

            state.update(
                {
                    "bound_session_id": "session-new",
                    "visible_session_id": "session-visible",
                    "session_alignment": "divergent",
                    "binding_generation": 4,
                    "bridge_state": "DEGRADED",
                    "degraded_reason": "visible mismatch",
                    "grok_home": str(config.grok_home),
                    "focus_source": "pager-status",
                    "status_trigger": "state",
                    "status_sample_monotonic_ns": 123,
                    "pager_status_enabled": True,
                    "pager_status_reason": "configured",
                    "pager_status_path": str(root / "pager-status.json"),
                    "registry_session_id": "session-stale",
                    "registry_observation_reason": "diagnostic only",
                }
            )
            config.run_path.write_text(json.dumps(state))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ):
                current = operator.status(config)
            self.assertEqual(current["bound_session_id"], "session-new")
            self.assertEqual(current["binding_generation"], 4)
            self.assertEqual(current["degraded_reason"], "visible mismatch")
            self.assertEqual(current["focus_source"], "pager-status")
            self.assertEqual(current["registry_session_id"], "session-stale")

    def test_doctor_fails_a_known_running_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.project.mkdir()
            config.state_root.mkdir(parents=True)
            config.log_root.mkdir()
            config.release_root.mkdir()
            release = config.release_root / "release"
            release.mkdir()
            (config.release_root / "current").symlink_to(release)
            runtime = {
                "running": True,
                "session_alignment": "divergent",
                "bridge_state": "DEGRADED",
                "degraded_reason": "visible mismatch",
                "grok_home": str(config.grok_home),
            }
            with mock.patch.object(operator.shutil, "which", return_value="/bin/tool"), mock.patch.object(
                operator, "_db_summary", return_value={"exists": True}
            ), mock.patch.object(operator, "status", return_value=runtime):
                result = operator.doctor(config)
            alignment = next(
                check for check in result["checks"] if check["name"] == "session_alignment"
            )
            self.assertFalse(result["ok"])
            self.assertFalse(alignment["ok"])
            self.assertIn("visible mismatch", alignment["detail"])

    def test_status_refuses_to_call_unmarked_pid_running(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            state = _run_state(root)
            del state["argv_marker"]
            config.run_path.write_text(json.dumps(state))
            with mock.patch.object(operator, "pid_alive", return_value=True):
                result = operator.status(config)
            self.assertFalse(result["running"])
            self.assertIn("missing argv_marker", result["problems"])

    def test_process_ownership_requires_exact_recorded_marker_and_module(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            state = _run_state(Path(temporary))
            argv = [
                str(Path(operator.sys.executable).resolve()),
                "-m",
                operator.SUPERVISOR_MODULE,
                "run",
                "--operator-marker",
                str(state["argv_marker"]),
            ]
            with mock.patch.object(operator, "process_argv", return_value=argv):
                owned, detail = operator.verify_process_owner(state)
            self.assertTrue(owned, detail)
            argv[-1] = "hcom-grok:different"
            with mock.patch.object(operator, "process_argv", return_value=argv):
                owned, detail = operator.verify_process_owner(state)
            self.assertFalse(owned)
            self.assertIn("marker", detail)

    def test_stop_never_signals_pid_when_ownership_mismatches(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            config.run_path.write_text(json.dumps(_run_state(root)))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(False, "marker mismatch")
            ), mock.patch.object(operator.os, "kill") as kill:
                with self.assertRaisesRegex(RuntimeError, "refusing to signal"):
                    operator.stop(config, timeout=0.1)
            kill.assert_not_called()

    def test_stop_refuses_live_pid_with_incomplete_run_state(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            config.run_path.write_text(json.dumps({"supervisor_pid": 4321}))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator.os, "kill"
            ) as kill:
                with self.assertRaisesRegex(RuntimeError, "invalid run state"):
                    operator.stop(config)
            kill.assert_not_called()

    def test_stop_verifies_again_then_signals_supervisor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            config.run_path.write_text(json.dumps(_run_state(root)))
            alive = iter([True, False, False])
            with mock.patch.object(operator, "pid_alive", side_effect=lambda _pid: next(alive, False)), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ) as verify, mock.patch.object(operator.os, "kill") as kill:
                result = operator.stop(config, timeout=0.1)
            self.assertTrue(result["stopped"])
            self.assertEqual(verify.call_count, 2)
            kill.assert_called_once_with(4321, operator.signal.SIGTERM)

    def test_clear_pending_advances_cursor_and_preserves_every_event(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            _events_db(config.hcom_db, count=7)
            result = operator.clear_pending(config)
            self.assertEqual(result["cursor"], 7)
            self.assertFalse(result["history_deleted"])
            cursor = json.loads(config.cursor_path.read_text())
            self.assertEqual(cursor["last_event_id"], 7)
            self.assertIsNone(cursor["pending_reply"])
            self.assertEqual(cursor["db_path"], str(config.hcom_db))
            self.assertEqual(len(cursor["last_event_sha256"]), 64)
            self.assertEqual(config.cursor_path.stat().st_mode & 0o777, 0o600)
            connection = sqlite3.connect(config.hcom_db)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM events").fetchone()[0], 7)
            finally:
                connection.close()

    def test_clear_pending_never_moves_cursor_backward(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            _events_db(config.hcom_db, count=2)
            config.cursor_path.parent.mkdir(parents=True)
            config.cursor_path.write_text(json.dumps({"last_event_id": 8}))
            with self.assertRaisesRegex(RuntimeError, "refusing to move backward"):
                operator.clear_pending(config)
            self.assertEqual(json.loads(config.cursor_path.read_text())["last_event_id"], 8)

    def test_clear_pending_refuses_while_supervisor_is_live(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = _config(root)
            config.current_state.mkdir(parents=True)
            config.run_path.write_text(json.dumps(_run_state(root)))
            with mock.patch.object(operator, "pid_alive", return_value=True), mock.patch.object(
                operator, "verify_process_owner", return_value=(True, "ok")
            ):
                with self.assertRaisesRegex(RuntimeError, "requires the supervisor to be stopped"):
                    operator.clear_pending(config)

    def test_parser_has_every_requested_command(self) -> None:
        parser = operator.build_parser()
        choices = next(action for action in parser._actions if action.dest == "command").choices
        self.assertEqual(
            set(choices),
            {"resume", "list", "status", "doctor", "logs", "stop", "restart", "inspect", "clear-pending", "rollback", "install"},
        )

    def test_parser_defaults_to_fresh_and_supports_continue(self) -> None:
        parser = operator.build_parser()
        fresh = parser.parse_args([])
        continued = parser.parse_args(["-c"])
        named = parser.parse_args(["-c", "mugi"])
        resumed = parser.parse_args(["resume", "--background"])
        self.assertIsNone(fresh.command)
        self.assertIsNone(fresh.continue_seat)
        self.assertEqual(continued.continue_seat, "")
        self.assertEqual(named.continue_seat, "mugi")
        self.assertEqual(resumed.command, "resume")
        self.assertTrue(resumed.background)

    def test_hcom_allocator_uses_room_and_accepts_repeated_single_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            project = root / "project"
            project.mkdir()
            hcom_db = root / "room" / "hcom.db"
            completed = operator.subprocess.CompletedProcess(
                ["hcom", "start"], 0, "[hcom:mugi]\n[hcom:mugi]\n", ""
            )
            inherited = {
                "HCOM_INSTANCE_NAME": "kimo",
                "HCOM_PROCESS_ID": "parent",
                "CODEX_SESSION_ID": "codex-parent",
                "CLAUDECODE": "1",
                "OPENCODE": "1",
                "KILO": "1",
                "KIMI_CODE_CLI": "1",
            }
            with mock.patch.dict(os.environ, inherited, clear=False), mock.patch.object(
                operator.subprocess, "run", return_value=completed
            ) as run:
                seat = operator.allocate_hcom_seat(hcom_db, "hcom", project)

            self.assertEqual(seat, "mugi")
            call = run.call_args
            self.assertEqual(call.args[0], ["hcom", "start"])
            self.assertEqual(call.kwargs["cwd"], str(project))
            self.assertEqual(call.kwargs["env"]["HCOM_DIR"], str(hcom_db.parent))
            self.assertNotIn("HCOM_INSTANCE_NAME", call.kwargs["env"])
            self.assertNotIn("CODEX_SESSION_ID", call.kwargs["env"])
            self.assertNotIn("CLAUDECODE", call.kwargs["env"])
            self.assertNotIn("OPENCODE", call.kwargs["env"])
            self.assertNotIn("KILO", call.kwargs["env"])
            self.assertNotIn("KIMI_CODE_CLI", call.kwargs["env"])

    def test_hcom_allocator_rejects_ambiguous_markers(self) -> None:
        completed = operator.subprocess.CompletedProcess(
            ["hcom", "start"], 0, "[hcom:mugi]\n[hcom:luna]\n", ""
        )
        with mock.patch.object(operator.subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(RuntimeError, "no unique"):
                operator.allocate_hcom_seat(Path("/tmp/room/hcom.db"), "hcom", Path("/tmp"))

    def test_deprecated_start_is_hidden_resume_alias(self) -> None:
        normalized, deprecated = operator.normalize_argv(["--json", "start", "--background"])
        self.assertTrue(deprecated)
        self.assertEqual(normalized, ["--json", "resume", "--background"])

    def test_human_status_includes_mode_project_and_busy_state(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            operator._print_result(
                {
                    "running": True,
                    "ready": True,
                    "busy": True,
                    "launch_mode": "new",
                    "seat": "gsea",
                    "supervisor_pid": 123,
                    "session_id": "session-1",
                    "project": "/tmp/project",
                },
                False,
            )
        rendered = output.getvalue()
        self.assertIn("busy: mode=NEW", rendered)
        self.assertIn("session=session-1", rendered)
        self.assertIn("project=/tmp/project", rendered)

    def test_main_routes_bare_to_new_and_continue_to_resume(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = _config(Path(temporary))
            with mock.patch.object(operator, "managed_mode", return_value=False), mock.patch.object(
                operator, "load_config", return_value=config
            ) as load, mock.patch.object(
                operator,
                "start",
                return_value={"ok": True},
            ) as start, mock.patch.object(operator, "_print_result"):
                self.assertEqual(operator.main([]), 0)
                load.assert_called_once_with(fresh_project=True)
                start.assert_called_once_with(config, background=False, session_mode="new")

            with mock.patch.object(operator, "managed_mode", return_value=False), mock.patch.object(
                operator, "load_config", return_value=config
            ) as load, mock.patch.object(
                operator,
                "start",
                return_value={"ok": True},
            ) as start, mock.patch.object(operator, "_print_result"):
                self.assertEqual(operator.main(["-c"]), 0)
                load.assert_called_once_with(fresh_project=False)
                start.assert_called_once_with(config, background=False, session_mode="resume")

    def test_managed_main_allocates_fresh_then_continues_named_seat(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            project = root / "project"
            project.mkdir()
            hcom_dir = root / "hcom-room"
            env = {
                "HCOM_DIR": str(hcom_dir),
                "HCOM_GROK_PROJECT": str(project),
                "HCOM_GROK_MANAGED_STATE_ROOT": str(root / "managed"),
                "HCOM_GROK_LOG_ROOT": str(root / "logs"),
            }
            with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
                operator, "allocate_hcom_seat", return_value="mugi"
            ), mock.patch.object(
                operator, "start", return_value={"ok": True}
            ) as start, mock.patch.object(operator, "_print_result"):
                self.assertEqual(operator.main([]), 0)
                fresh_config = start.call_args.args[0]
                self.assertEqual(fresh_config.seat, "mugi")
                self.assertEqual(fresh_config.project, project.resolve())
                self.assertEqual(start.call_args.kwargs["session_mode"], "new")

            with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
                operator, "start", return_value={"ok": True}
            ) as start, mock.patch.object(operator, "_print_result"):
                self.assertEqual(operator.main(["-c", "mugi"]), 0)
                resumed_config = start.call_args.args[0]
                self.assertEqual(resumed_config.seat, "mugi")
                self.assertEqual(resumed_config.state_root, fresh_config.state_root)
                self.assertEqual(start.call_args.kwargs["session_mode"], "resume")


if __name__ == "__main__":
    unittest.main()
