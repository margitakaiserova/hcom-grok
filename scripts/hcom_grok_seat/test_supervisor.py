from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from .envelope import EventRow, classify_event
from .supervisor import (
    Config,
    Supervisor,
    TurnCollector,
    atomic_json,
    clean_child_env,
    install_shutdown_handlers,
    live_process_executable,
)


def make_config(
    root: Path,
    *,
    background: bool = True,
    session_mode: str = "resume",
) -> Config:
    hcom_dir = root / "hcom"
    hcom_dir.mkdir()
    return Config(
        state_root=root / "state",
        log_root=root / "logs",
        project=root / "project",
        hcom_dir=hcom_dir,
        hcom_db=hcom_dir / "hcom.db",
        grok_bin="grok",
        hcom_bin="hcom",
        seat="gsea",
        socket_path=str(root / "leader.sock"),
        background_tui=background,
        session_mode=session_mode,
    )


def create_hcom_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute("PRAGMA user_version=18")
    con.execute(
        "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT NOT NULL,"
        "type TEXT NOT NULL,instance TEXT NOT NULL,data TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE instances(name TEXT PRIMARY KEY,pid INTEGER,status TEXT,"
        "status_context TEXT,status_detail TEXT,status_time INTEGER DEFAULT 0,"
        "last_seen INTEGER DEFAULT 0,session_id TEXT,directory TEXT)"
    )
    con.execute(
        "CREATE TABLE process_bindings(process_id TEXT,session_id TEXT,"
        "instance_name TEXT,updated_at REAL,PRIMARY KEY(process_id,instance_name))"
    )
    con.execute(
        "CREATE TABLE session_bindings(session_id TEXT PRIMARY KEY,"
        "instance_name TEXT,created_at REAL)"
    )
    con.execute("CREATE TABLE kv(key TEXT PRIMARY KEY,value TEXT)")
    con.commit()
    return con


def insert_message(
    con: sqlite3.Connection,
    *,
    intent: str,
    text: str,
    sender: str = "kimo",
) -> int:
    payload = {
        "from": sender,
        "intent": intent,
        "mentions": ["gsea"],
        "delivered_to": ["gsea"],
        "thread": "feature-test",
        "text": text,
    }
    con.execute(
        "INSERT INTO events(timestamp,type,instance,data) VALUES(?,?,?,?)",
        ("2026-08-31T00:00:00Z", "message", sender, json.dumps(payload)),
    )
    con.commit()
    return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])


class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_hangup_handler_runs_normal_cleanup_and_native_hcom_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.project.mkdir()
            supervisor = Supervisor(config, "hcom-grok:test")
            handlers: dict[int, object] = {}

            class FakeLoop:
                def add_signal_handler(self, signum: int, callback: object) -> None:
                    handlers[signum] = callback

            install_shutdown_handlers(FakeLoop(), supervisor.stop_event)  # type: ignore[arg-type]
            self.assertIn(signal.SIGHUP, handlers)

            supervisor.acquire_lock = Mock()  # type: ignore[method-assign]
            supervisor.load_cursor = Mock(return_value={"last_event_id": 0})  # type: ignore[method-assign]
            supervisor.load_session = Mock(return_value=("session-1", False))  # type: ignore[method-assign]
            supervisor.spawn_leader = AsyncMock()  # type: ignore[method-assign]
            supervisor.spawn_tui = AsyncMock()  # type: ignore[method-assign]
            supervisor.wait_session = AsyncMock()  # type: ignore[method-assign]
            supervisor.connect_sidecar = AsyncMock(return_value=TurnCollector("session-1"))  # type: ignore[method-assign]
            supervisor.register_hcom = AsyncMock()  # type: ignore[method-assign]
            supervisor.retry_pending_reply = AsyncMock()  # type: ignore[method-assign]
            supervisor.write_run_state = Mock()  # type: ignore[method-assign]
            supervisor.log = Mock()  # type: ignore[method-assign]
            hcom_calls: list[list[str]] = []

            async def fake_hcom(
                args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                hcom_calls.append(args)
                return subprocess.CompletedProcess(args, 0, "stopped", "")

            supervisor.hcom_command = fake_hcom  # type: ignore[method-assign]
            callback = handlers[signal.SIGHUP]
            assert callable(callback)
            callback()

            with patch("builtins.print"):
                self.assertEqual(await asyncio.wait_for(supervisor.run(), timeout=1), 0)
            self.assertIn(["stop", "gsea"], hcom_calls)
            self.assertTrue(supervisor.stop_event.is_set())

    async def test_unregister_uses_native_hcom_stop(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor = Supervisor(make_config(Path(td)), "hcom-grok:test")
            calls: list[list[str]] = []

            async def fake_hcom(
                args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                calls.append(args)
                return subprocess.CompletedProcess(args, 0, "stopped", "")

            supervisor.hcom_command = fake_hcom  # type: ignore[method-assign]
            with patch.object(supervisor, "unregister_hcom_fallback") as fallback:
                await supervisor.unregister_hcom()

            self.assertEqual(calls, [["stop", "gsea"]])
            fallback.assert_not_called()

    async def test_unregister_falls_back_when_native_stop_fails(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor = Supervisor(make_config(Path(td)), "hcom-grok:test")

            async def fake_hcom(
                args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess(args, 2, "", "stop failed")

            supervisor.hcom_command = fake_hcom  # type: ignore[method-assign]
            with patch.object(supervisor, "unregister_hcom_fallback") as fallback:
                await supervisor.unregister_hcom()

            fallback.assert_called_once_with()

    async def test_turn_collector_returns_only_the_last_assistant_message(self) -> None:
        collector = TurnCollector("session-1")
        await collector.begin("prompt-1")
        for stream_start, text in (
            (1000, "Checking the project first."),
            (2000, "Creating the requested file."),
            (3000, "Complete. `/tmp/project/index.html`"),
        ):
            await collector(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": "session-1",
                        "_meta": {
                            "promptId": "prompt-1",
                            "streamStartMs": stream_start,
                        },
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": text},
                        },
                    },
                }
            )
        text, _reason = await collector.finish("prompt-1")
        self.assertEqual(text, "Complete. `/tmp/project/index.html`")

    def test_new_mode_mints_and_persists_a_fresh_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.project.mkdir()
            config.state_root.mkdir()
            config.session_path.write_text(
                json.dumps({"session_id": "old-session", "project": str(config.project)})
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            session_id, resume = supervisor.load_session()
            saved = json.loads(config.session_path.read_text())
            self.assertFalse(resume)
            self.assertNotEqual(session_id, "old-session")
            self.assertEqual(saved["session_id"], session_id)
            self.assertEqual(saved["project"], str(config.project))

    def test_resume_mode_requires_existing_saved_session_folder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="resume")
            config.project.mkdir()
            config.state_root.mkdir()
            config.session_path.write_text(
                json.dumps({"session_id": "session-1", "project": str(config.project)})
            )
            existing = root / "grok-session"
            existing.mkdir()
            supervisor = Supervisor(config, "hcom-grok:test")
            with patch(
                "scripts.hcom_grok_seat.supervisor.session_directory",
                return_value=existing,
            ):
                session_id, resume = supervisor.load_session()
            self.assertTrue(resume)
            self.assertEqual(session_id, "session-1")

    def test_resume_mode_rejects_missing_saved_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = make_config(Path(td), session_mode="resume")
            supervisor = Supervisor(config, "hcom-grok:test")
            with self.assertRaisesRegex(RuntimeError, "Run hcom-grok to start fresh"):
                supervisor.load_session()

    def test_heartbeat_refreshes_native_hcom_status_time(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = make_config(Path(td))
            config.project.mkdir()
            con = create_hcom_db(config.hcom_db)
            con.execute(
                "INSERT INTO instances(name,status,status_time,last_seen) VALUES(?,?,?,?)",
                ("gsea", "active", 1, 1),
            )
            con.commit()
            con.close()

            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.session_id = "session-1"
            supervisor.tui = type("FakeTui", (), {"pid": 123})()  # type: ignore[assignment]
            with patch("scripts.hcom_grok_seat.supervisor.time.time", return_value=1234.75):
                supervisor.heartbeat("listening", "ready")

            con = sqlite3.connect(config.hcom_db)
            row = con.execute(
                "SELECT status,status_detail,status_time,last_seen FROM instances "
                "WHERE name='gsea'"
            ).fetchone()
            con.close()
            self.assertEqual(row, ("listening", "ready", 1234, 1234))

    def test_child_environment_rebinds_hcom_identity(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = make_config(Path(td))
            with patch.dict(
                os.environ,
                {
                    "HCOM_PROCESS_ID": "parent-process",
                    "HCOM_INSTANCE_NAME": "kimo",
                    "HCOM_SESSION_ID": "parent-session",
                    "CODEX_SESSION_ID": "parent-codex",
                    "CURSOR_CONVERSATION_ID": "parent-cursor",
                    "CLAUDECODE": "1",
                    "OPENCODE": "1",
                    "KILO": "1",
                    "KIMI_CODE_CLI": "1",
                },
                clear=False,
            ):
                env = clean_child_env(config, "seat-run-token")
            self.assertEqual(env["HCOM_PROCESS_ID"], "seat-run-token")
            self.assertEqual(env["HCOM_INSTANCE_NAME"], "gsea")
            self.assertNotIn("HCOM_SESSION_ID", env)
            self.assertNotIn("CODEX_SESSION_ID", env)
            self.assertNotIn("CURSOR_CONVERSATION_ID", env)
            self.assertNotIn("CLAUDECODE", env)
            self.assertNotIn("OPENCODE", env)
            self.assertNotIn("KILO", env)
            self.assertNotIn("KIMI_CODE_CLI", env)
            self.assertEqual(env["GROK_CLAUDE_HOOKS_ENABLED"], "false")
            self.assertEqual(env["GROK_CURSOR_HOOKS_ENABLED"], "false")

    def test_live_process_executable_uses_os_visible_command(self) -> None:
        completed = subprocess.CompletedProcess(
            ["ps"], 0, "/actual/python -m scripts.hcom_grok_seat.supervisor run\n", ""
        )
        with patch("scripts.hcom_grok_seat.supervisor.subprocess.run", return_value=completed):
            self.assertEqual(live_process_executable(42), "/actual/python")

    def test_atomic_json_is_private(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "private" / "state.json"
            atomic_json(path, {"ready": True})
            self.assertEqual(json.loads(path.read_text()), {"ready": True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.parent.stat().st_mode & 0o777, 0o700)

    async def test_turn_collector_returns_only_correlated_assistant_text(self) -> None:
        collector = TurnCollector("session-1")
        await collector.begin("prompt-1")
        await collector(
            {
                "method": "session/update",
                "params": {
                    "sessionId": "session-1",
                    "_meta": {"promptId": "prompt-1"},
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": "hello "},
                    },
                },
            }
        )
        await collector(
            {
                "method": "_x.ai/session_notification",
                "params": {
                    "sessionId": "session-1",
                    "update": {
                        "sessionUpdate": "turn_completed",
                        "prompt_id": "prompt-1",
                        "stop_reason": "end_turn",
                    },
                },
            }
        )
        self.assertEqual(await collector.finish("prompt-1"), ("hello", "end_turn"))

    async def test_ack_advances_cursor_without_model_turn(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = make_config(Path(td))
            config.project.mkdir()
            con = create_hcom_db(config.hcom_db)
            event_id = insert_message(con, intent="ack", text="received")
            con.close()
            supervisor = Supervisor(config, "hcom-grok:test")
            db_stat = config.hcom_db.stat()
            supervisor.cursor = {
                "schema": 1,
                "db_path": str(config.hcom_db),
                "db_device": db_stat.st_dev,
                "db_inode": db_stat.st_ino,
                "last_event_id": 0,
                "last_event_sha256": None,
                "pending_reply": None,
            }

            async def forbidden(*_args: object) -> None:
                raise AssertionError("ack crossed the model boundary")

            supervisor.deliver = forbidden  # type: ignore[method-assign]
            self.assertTrue(await supervisor.process_mail(TurnCollector("session")))
            self.assertEqual(supervisor.cursor["last_event_id"], event_id)

    async def test_request_completion_is_returned_through_hcom(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            config = make_config(Path(td))
            config.project.mkdir()
            con = create_hcom_db(config.hcom_db)
            event_id = insert_message(con, intent="request", text="give a short answer")
            row = con.execute(
                "SELECT id,timestamp,type,instance,data FROM events WHERE id=?", (event_id,)
            ).fetchone()
            con.close()
            event = EventRow(int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
            classified = classify_event(event, "gsea")
            assert classified.envelope is not None
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.session_id = "session-1"
            db_stat = config.hcom_db.stat()
            supervisor.cursor = {
                "schema": 1,
                "db_path": str(config.hcom_db),
                "db_device": db_stat.st_dev,
                "db_inode": db_stat.st_ino,
                "last_event_id": 0,
                "last_event_sha256": None,
                "pending_reply": None,
            }
            collector = TurnCollector("session-1")
            sent: list[list[str]] = []

            class FakeClient:
                async def call(self, _method: str, params: dict, timeout: float) -> dict:
                    prompt_id = params["_meta"]["promptId"]
                    await collector(
                        {
                            "method": "session/update",
                            "params": {
                                "sessionId": "session-1",
                                "_meta": {"promptId": prompt_id},
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {"type": "text", "text": "working reply"},
                                },
                            },
                        }
                    )
                    return {"stopReason": "end_turn"}

                async def flush_notifications(self) -> None:
                    return None

            async def fake_hcom(
                args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                sent.append(args)
                return subprocess.CompletedProcess(args, 0, "ok", "")

            supervisor.client = FakeClient()  # type: ignore[assignment]
            supervisor.hcom_command = fake_hcom  # type: ignore[method-assign]
            await supervisor.deliver(classified.envelope, collector)
            self.assertEqual(supervisor.cursor["last_event_id"], event_id)
            self.assertIsNone(supervisor.cursor["pending_reply"])
            self.assertEqual(len(sent), 1)
            self.assertIn("@kimo", sent[0])
            self.assertIn("working reply", sent[0])
            self.assertIn("feature-test", sent[0])

    def test_resume_argv_preserves_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor = Supervisor(make_config(Path(td)), "hcom-grok:test")
            supervisor.session_id = "session-123"
            argv = supervisor.tui_argv(True)
            self.assertEqual(argv[argv.index("--resume") + 1], "session-123")
            self.assertNotIn("--session-id", argv)

    def test_visible_tui_does_not_receive_ready_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor = Supervisor(
                make_config(Path(td), background=False), "hcom-grok:test"
            )
            supervisor.session_id = "session-visible"
            with patch("builtins.print") as output:
                supervisor.announce_ready(False)
            output.assert_not_called()

    def test_background_tui_keeps_ready_output_in_launch_log(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor = Supervisor(
                make_config(Path(td), background=True), "hcom-grok:test"
            )
            supervisor.session_id = "session-background"
            with patch("builtins.print") as output:
                supervisor.announce_ready(True)
            output.assert_called_once()
            self.assertIn("mode=RESUMED", output.call_args.args[0])
            self.assertIn("session=session-background", output.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
