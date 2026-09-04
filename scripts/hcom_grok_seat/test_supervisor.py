from __future__ import annotations

import asyncio
import json
import os
import shlex
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import tomllib
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from .acp_client import RpcHandle
from .acp_session import AcpHandshake, PermissionBroker, ResumeReplayFence
from .envelope import EventRow, classify_event
from .pager_status import (
    PagerStatusSetup,
    _encode_authenticated_record,
    prepare_pager_status,
    read_pager_status,
    record_authenticated_pager_payload,
    stage_pager_status,
)
from .supervisor import (
    Config,
    PagerPeerProof,
    SidecarBinding,
    Supervisor,
    TuiChild,
    TurnCollector,
    atomic_json,
    clean_child_env,
    filesystem_socket_identity,
    install_shutdown_handlers,
    live_process_executable,
    process_start_identity,
    remove_owned_socket,
    session_directory,
    sidecar_process_matches,
)
from .visible_session import VisibleSessionObservation


SESSION_1 = "11111111-1111-4111-8111-111111111111"
SESSION_2 = "22222222-2222-4222-8222-222222222222"
SESSION_3 = "33333333-3333-4333-8333-333333333333"


def observation(
    kind: str,
    session_id: str | None,
    reason: str = "test",
    *,
    state_ns: int | None = None,
) -> VisibleSessionObservation:
    return VisibleSessionObservation(  # type: ignore[arg-type]
        kind=kind,
        reason=reason,
        session_id=session_id,
        pid=4322,
        focus_state_monotonic_ns=state_ns,
    )


class RecordingClient:
    def __init__(self, session_id: str, collector: TurnCollector | None = None) -> None:
        self.session_id = session_id
        self.collector = collector
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.proc = None
        self.prompt_started: asyncio.Event | None = None
        self.prompt_release: asyncio.Event | None = None
        self._next_id = 0

    async def _complete(self, method: str, params: dict) -> dict:
        if method != "session/prompt":
            return {}
        if self.prompt_release is not None:
            await self.prompt_release.wait()
        prompt_id = str(params["_meta"]["promptId"])  # type: ignore[index]
        if self.collector is not None:
            await self.collector(
                {
                    "method": "session/update",
                    "params": {
                        "sessionId": self.session_id,
                        "_meta": {"promptId": prompt_id},
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "recorded reply"},
                        },
                    },
                }
            )
        return {"stopReason": "end_turn"}

    async def begin_request(self, method: str, params: dict) -> RpcHandle:
        self.calls.append((method, params))
        if self.prompt_started is not None:
            self.prompt_started.set()
        self._next_id += 1
        response = asyncio.create_task(self._complete(method, params))
        return RpcHandle(self._next_id, method, response)

    async def await_response(self, handle: RpcHandle, *, timeout: float) -> dict:
        return await asyncio.wait_for(asyncio.shield(handle.response), timeout)

    async def call(self, method: str, params: dict, timeout: float) -> dict:
        handle = await self.begin_request(method, params)
        return await self.await_response(handle, timeout=timeout)

    async def flush_notifications(self) -> None:
        return None

    async def close_transport(self) -> None:
        self.closed = True


def make_binding(
    session_id: str,
    client: object,
    collector: TurnCollector | None = None,
) -> SidecarBinding:
    return SidecarBinding(
        session_id,
        client,  # type: ignore[arg-type]
        collector or TurnCollector(session_id),
        PermissionBroker(session_id),
        AcpHandshake(1, "1.0.13", "cached_token", {"loadSession": True}, None),
        ResumeReplayFence(session_id, "load"),
        0,
        None,
    )


def make_config(
    root: Path,
    *,
    background: bool = True,
    session_mode: str = "resume",
    sidecar_handoff: str = "close-first",
) -> Config:
    hcom_dir = root / "hcom"
    hcom_dir.mkdir(exist_ok=True)
    launch_home = root / "seat-home"
    launch_home.mkdir(exist_ok=True)
    return Config(
        state_root=launch_home / "state",
        log_root=root / "logs",
        project=root / "project",
        grok_home=root / "seat-home" / ".grok",
        hcom_dir=hcom_dir,
        hcom_db=hcom_dir / "hcom.db",
        grok_bin="grok",
        hcom_bin="hcom",
        seat="gsea",
        socket_path=str(root / "leader.sock"),
        background_tui=background,
        session_mode=session_mode,
        sidecar_handoff=sidecar_handoff,
        launch_home=launch_home,
        isolated_home=True,
    )


def prepared_supervisor(
    root: Path, *, sidecar_handoff: str = "concurrent"
) -> tuple[Supervisor, sqlite3.Connection, SidecarBinding]:
    config = make_config(root, sidecar_handoff=sidecar_handoff)
    config.project.mkdir()
    config.state_root.mkdir()
    config.log_root.mkdir()
    os.chmod(config.state_root, 0o700)
    os.chmod(config.log_root, 0o700)
    connection = create_hcom_db(config.hcom_db)
    connection.execute(
        "INSERT INTO instances(name,pid,status,session_id,directory) VALUES(?,?,?,?,?)",
        (config.seat, 4322, "listening", SESSION_1, str(config.project)),
    )
    connection.commit()
    supervisor = Supervisor(config, "hcom-grok:test")
    supervisor.session_id = SESSION_1
    supervisor.launch_session_id = SESSION_1
    supervisor.bridge_state = "BOUND"
    supervisor.visible_session_id = SESSION_1
    supervisor.tui = TuiChild(4322)
    old_collector = TurnCollector(SESSION_1)
    old_client = RecordingClient(SESSION_1, old_collector)
    old_binding = make_binding(SESSION_1, old_client, old_collector)
    supervisor.binding = old_binding
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
    atomic_json(config.cursor_path, supervisor.cursor)
    atomic_json(
        config.session_path,
        {
            "schema": 2,
            "session_id": SESSION_1,
            "launch_session_id": SESSION_1,
            "binding_generation": 0,
            "bound_at_ns": 1,
            "previous_session_id": None,
            "transition_reason": None,
            "project": str(config.project),
            "created_ns": 1,
        },
    )
    return supervisor, connection, old_binding


def create_hcom_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute("PRAGMA user_version=18")
    con.execute(
        "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT NOT NULL,"
        "type TEXT NOT NULL,instance TEXT NOT NULL,data TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE instances(name TEXT PRIMARY KEY,session_id TEXT UNIQUE,"
        "parent_session_id TEXT,pid INTEGER,status TEXT,"
        "status_context TEXT,status_detail TEXT,status_time INTEGER DEFAULT 0,"
        "last_seen INTEGER DEFAULT 0,directory TEXT,created_at REAL NOT NULL DEFAULT 0,"
        "FOREIGN KEY(parent_session_id) REFERENCES instances(session_id) ON DELETE SET NULL)"
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
    async def test_unsafe_configured_state_root_is_refused_without_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.state_root.mkdir(parents=True)
            os.chmod(config.state_root, 0o755)
            sentinel = config.state_root / "unrelated.txt"
            sentinel.write_text("keep me\n", encoding="utf-8")
            os.chmod(sentinel, 0o644)
            before_dir_mode = config.state_root.stat().st_mode & 0o777
            before_file_mode = sentinel.stat().st_mode & 0o777

            with self.assertRaisesRegex(RuntimeError, "mode 0700"):
                Supervisor(config, "hcom-grok:test").acquire_lock()

            self.assertEqual(config.state_root.stat().st_mode & 0o777, before_dir_mode)
            self.assertEqual(sentinel.stat().st_mode & 0o777, before_file_mode)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep me\n")

    async def test_pager_bootstrap_is_staged_before_leader_and_tui(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.project.mkdir()
            setup = PagerStatusSetup(
                enabled=True,
                reason="ready",
                status_path=config.state_root / "pager-status.json",
                token="1" * 32,
                config_path=config.grok_home / "config.toml",
                config_transaction_path=(
                    config.state_root / "pager-config-transaction.json"
                ),
                owner_claim_path=config.grok_home / ".hcom-grok-pager-owner.json",
                owner_id="owner",
                expected_block=b"config",
                expected_claim=b"claim",
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            binding = make_binding(SESSION_1, RecordingClient(SESSION_1))
            order: list[str] = []

            supervisor.acquire_lock = Mock()  # type: ignore[method-assign]
            supervisor.start_pager_broker = AsyncMock(  # type: ignore[method-assign]
                side_effect=lambda: order.append("broker")
            )
            supervisor.recover_stale_runtime = AsyncMock(  # type: ignore[method-assign]
                side_effect=lambda: order.append("recover-runtime")
            )
            supervisor.load_cursor = Mock(return_value={"last_event_id": 0})  # type: ignore[method-assign]
            supervisor.load_session = Mock(return_value=(SESSION_1, False))  # type: ignore[method-assign]
            supervisor.write_run_state = Mock()  # type: ignore[method-assign]
            supervisor.spawn_leader = AsyncMock(  # type: ignore[method-assign]
                side_effect=lambda: order.append("leader")
            )
            supervisor.spawn_tui = AsyncMock(  # type: ignore[method-assign]
                side_effect=lambda _resume: order.append("tui")
            )
            supervisor.wait_session = AsyncMock()  # type: ignore[method-assign]
            supervisor.create_sidecar = AsyncMock(return_value=binding)  # type: ignore[method-assign]
            supervisor.connect_sidecar = AsyncMock(return_value=binding)  # type: ignore[method-assign]

            async def fresh(_minimum: int) -> VisibleSessionObservation:
                order.append("fresh-sample")
                return observation("aligned", SESSION_1)

            supervisor.observe_fresh_binding = fresh  # type: ignore[method-assign]
            supervisor.observe_registry_diagnostic = Mock()  # type: ignore[method-assign]
            supervisor.register_hcom = AsyncMock()  # type: ignore[method-assign]
            supervisor.retry_pending_reply = AsyncMock()  # type: ignore[method-assign]
            supervisor.announce_ready = Mock()  # type: ignore[method-assign]
            supervisor.log = Mock()  # type: ignore[method-assign]
            supervisor.cleanup = AsyncMock()  # type: ignore[method-assign]
            supervisor.stop_event.set()

            module = Supervisor.__module__
            with patch(f"{module}.prepare_pager_status", return_value=setup), patch(
                f"{module}.recover_pager_config",
                side_effect=lambda _setup: order.append("recover-config") or None,
            ), patch(
                f"{module}.stage_pager_status",
                side_effect=lambda _setup: order.append("stage") or None,
            ):
                self.assertEqual(await supervisor.run(), 0)

            self.assertEqual(
                order,
                [
                    "recover-runtime",
                    "recover-config",
                    "broker",
                    "stage",
                    "leader",
                    "tui",
                    "fresh-sample",
                ],
            )

    async def test_run_failure_after_stage_restores_existing_seat_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            original = b'[models]\ndefault = "grok-4.6"\n'
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            config.log_root.mkdir(parents=True)
            for path in (
                config.launch_home,
                config.grok_home,
                config.state_root,
                config.log_root,
            ):
                os.chmod(path, 0o700)
            config.grok_home.joinpath("config.toml").write_bytes(original)
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.acquire_lock = Mock()  # type: ignore[method-assign]
            supervisor.recover_stale_runtime = AsyncMock()  # type: ignore[method-assign]
            supervisor.start_pager_broker = AsyncMock()  # type: ignore[method-assign]
            supervisor.stop_pager_broker = AsyncMock()  # type: ignore[method-assign]
            supervisor.load_cursor = Mock(return_value={"last_event_id": 0})  # type: ignore[method-assign]
            supervisor.load_session = Mock(return_value=(SESSION_1, False))  # type: ignore[method-assign]
            supervisor.write_run_state = Mock()  # type: ignore[method-assign]
            supervisor.spawn_leader = AsyncMock()  # type: ignore[method-assign]
            supervisor.spawn_tui = AsyncMock(  # type: ignore[method-assign]
                side_effect=RuntimeError("failure after pager stage")
            )
            supervisor.create_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=make_binding(SESSION_1, RecordingClient(SESSION_1))
            )
            supervisor.log = Mock()  # type: ignore[method-assign]

            self.assertEqual(await supervisor.run(), 1)
            self.assertEqual(
                config.grok_home.joinpath("config.toml").read_bytes(),
                original,
            )
            self.assertFalse(
                config.grok_home.joinpath(".hcom-grok-pager-owner.json").exists()
            )
            self.assertFalse(config.state_root.joinpath("pager-status-command.py").exists())

    async def test_foreign_config_drift_blocks_delivery_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            os.chmod(config.launch_home, 0o700)
            os.chmod(config.grok_home, 0o700)
            os.chmod(config.state_root, 0o700)
            connection = create_hcom_db(config.hcom_db)
            event_id = insert_message(connection, intent="inform", text="still deliver")
            connection.close()
            db_stat = config.hcom_db.stat()
            cursor = {
                "schema": 1,
                "db_path": str(config.hcom_db),
                "db_device": db_stat.st_dev,
                "db_inode": db_stat.st_ino,
                "last_event_id": 0,
                "last_event_sha256": None,
                "pending_reply": None,
            }
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
            )
            self.assertTrue(setup.enabled, setup.reason)
            supervisor = Supervisor(config, "hcom-grok:test")
            client = RecordingClient(SESSION_1)
            binding = make_binding(SESSION_1, client)

            supervisor.acquire_lock = Mock()  # type: ignore[method-assign]
            supervisor.start_pager_broker = AsyncMock()  # type: ignore[method-assign]
            supervisor.recover_stale_runtime = AsyncMock()  # type: ignore[method-assign]
            supervisor.load_cursor = Mock(return_value=cursor)  # type: ignore[method-assign]
            supervisor.load_session = Mock(return_value=(SESSION_1, False))  # type: ignore[method-assign]
            supervisor.write_run_state = Mock()  # type: ignore[method-assign]
            supervisor.spawn_leader = AsyncMock()  # type: ignore[method-assign]

            async def overwrite_bootstrap(_resume: bool) -> None:
                setup.config_path.write_text("foreign replacement\n", encoding="utf-8")
                os.chmod(setup.config_path, 0o600)

            supervisor.spawn_tui = overwrite_bootstrap  # type: ignore[method-assign]
            supervisor.wait_session = AsyncMock()  # type: ignore[method-assign]
            supervisor.create_sidecar = AsyncMock(return_value=binding)  # type: ignore[method-assign]
            supervisor.connect_sidecar = AsyncMock(return_value=binding)  # type: ignore[method-assign]
            supervisor.observe_fresh_binding = AsyncMock(  # type: ignore[method-assign]
                return_value=observation(
                    "unsafe",
                    SESSION_1,
                    "pager status ownership claim drifted",
                )
            )
            supervisor.observe_registry_diagnostic = Mock()  # type: ignore[method-assign]
            supervisor.register_hcom = AsyncMock()  # type: ignore[method-assign]
            supervisor.retry_pending_reply = AsyncMock()  # type: ignore[method-assign]
            supervisor.announce_ready = Mock()  # type: ignore[method-assign]
            supervisor.log = Mock()  # type: ignore[method-assign]
            supervisor.cleanup = AsyncMock()  # type: ignore[method-assign]
            supervisor.stop_event.set()

            module = Supervisor.__module__
            with patch(f"{module}.prepare_pager_status", return_value=setup):
                self.assertEqual(await supervisor.run(), 0)

            self.assertEqual(supervisor.bridge_state, "DEGRADED")
            self.assertEqual(
                setup.config_path.read_text(encoding="utf-8"),
                "foreign replacement\n",
            )
            self.assertIsNone(supervisor.pager_bootstrap_retained)

            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_1)
            )
            supervisor.hcom_command = AsyncMock(  # type: ignore[method-assign]
                return_value=subprocess.CompletedProcess([], 0, "ok", "")
            )
            self.assertEqual(await supervisor.process_mail(), "held")
            self.assertLess(supervisor.cursor["last_event_id"], event_id)
            self.assertFalse(
                any(method == "session/prompt" for method, _params in client.calls)
            )

    async def test_tampered_pager_focus_never_reaches_session_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            supervisor, connection, binding = prepared_supervisor(root)
            insert_message(connection, intent="inform", text="must stay held")
            connection.close()
            config = supervisor.config
            config.grok_home.mkdir(parents=True)
            os.chmod(config.launch_home, 0o700)
            os.chmod(config.grok_home, 0o700)
            os.chmod(config.state_root, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
            )
            self.assertTrue(setup.enabled, setup.reason)
            self.assertIsNone(stage_pager_status(setup))
            now = time.monotonic_ns()
            record = {
                "schema": 1,
                "session_id": SESSION_1,
                "cwd": str(config.project),
                "status_schema_version": 1,
                "grok_version": "1.0.13",
                "trigger": "state",
                "tui_pid": 4322,
                "captured_monotonic_ns": now,
                "captured_wall_ns": time.time_ns(),
                "state_session_id": SESSION_1,
                "state_cwd": str(config.project),
                "state_observed_monotonic_ns": now,
            }
            setup.status_path.write_bytes(_encode_authenticated_record(record, setup.token))
            os.chmod(setup.status_path, 0o600)
            forged = json.loads(setup.status_path.read_text(encoding="utf-8"))
            forged["session_id"] = SESSION_2
            forged["state_session_id"] = SESSION_2
            forged["captured_monotonic_ns"] = time.monotonic_ns()
            forged["state_observed_monotonic_ns"] = forged["captured_monotonic_ns"]
            setup.status_path.write_text(json.dumps(forged), encoding="utf-8")
            os.chmod(setup.status_path, 0o600)
            supervisor.pager_status = setup
            assert supervisor.tui is not None
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]

            self.assertEqual(await supervisor.process_mail(), "held")
            self.assertEqual(supervisor.cursor["last_event_id"], 0)
            self.assertEqual(supervisor.bridge_state, "DEGRADED")
            client = binding.client
            self.assertFalse(
                any(method == "session/prompt" for method, _params in client.calls)  # type: ignore[attr-defined]
            )

    async def test_fresh_observer_waits_for_post_gate_status_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _binding = prepared_supervisor(Path(td))
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("transient-missing", None, "predates delivery gate"),
                    observation("aligned", SESSION_1),
                ]
            )
            assert supervisor.tui is not None
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]
            with patch("asyncio.sleep", new=AsyncMock()):
                result = await supervisor.observe_fresh_binding(123456)
            self.assertEqual(result.kind, "aligned")
            self.assertEqual(
                [call.args for call in supervisor.observe_binding.call_args_list],  # type: ignore[attr-defined]
                [(123456,), (123456,)],
            )
            connection.close()

    def test_config_captures_plugin_manager_isolation_marker(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            home = root / "seat-home"
            with patch.dict(
                os.environ,
                {
                    "HOME": str(home),
                    "GROK_HOME": str(home / ".grok"),
                    "HCOM_GROK_STATE_ROOT": str(home / "state"),
                    "HCOM_GROK_ISOLATED_HOME": "1",
                },
                clear=True,
            ):
                config = Config.from_env(background_tui=True)
            self.assertEqual(config.launch_home, home.resolve())
            self.assertTrue(config.isolated_home)
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            for path in (home, config.grok_home, config.state_root):
                os.chmod(path, 0o700)
            pager = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home or home,
                grok_home=config.grok_home,
                isolated_home=config.isolated_home,
                environment={},
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            self.assertTrue(pager.enabled, pager.reason)

    async def test_runtime_hardening_preserves_and_executes_owned_pager_shim(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            config.log_root.mkdir(parents=True)
            for path in (
                config.launch_home,
                config.grok_home,
                config.state_root,
                config.log_root,
            ):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            self.assertTrue(setup.enabled, setup.reason)
            self.assertIsNone(stage_pager_status(setup))
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            supervisor.tui = TuiChild(os.getpid())
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]
            supervisor._tui_start_identity = process_start_identity(os.getpid())
            await supervisor.start_pager_broker()

            supervisor.harden_runtime_files()

            assert setup.shim_path is not None
            self.assertEqual(setup.shim_path.stat().st_mode & 0o777, 0o700)
            parsed = tomllib.loads(setup.expected_block.decode("utf-8"))
            command = shlex.split(parsed["ui"]["status_line"]["command"])
            payload = {
                "schema_version": 1,
                "session_id": SESSION_1,
                "cwd": str(config.project),
                "version": "1.0.13",
                "trigger": "state",
            }
            result = await asyncio.to_thread(
                subprocess.run,
                command,
                input=json.dumps(payload).encode(),
                cwd=config.project,
                env={"HOME": str(config.launch_home), "PATH": os.defpath},
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            status = read_pager_status(
                setup,
                tui_pid=os.getpid(),
                agent_version="1.0.13",
            )
            self.assertEqual(status.kind, "valid", status.reason)
            await supervisor.stop_pager_broker()

    async def test_untrusted_pager_client_cannot_invalidate_good_focus(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            for path in (config.launch_home, config.grok_home, config.state_root):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            self.assertIsNone(stage_pager_status(setup))
            record_authenticated_pager_payload(
                setup,
                json.dumps(
                    {
                        "schema_version": 1,
                        "session_id": SESSION_1,
                        "cwd": str(config.project),
                        "version": "1.0.13",
                        "trigger": "state",
                    }
                ).encode(),
                tui_pid=os.getpid(),
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            supervisor.tui = TuiChild(os.getpid())
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]
            supervisor._tui_start_identity = process_start_identity(os.getpid())
            supervisor._pager_peer_is_owned = Mock(  # type: ignore[method-assign]
                return_value=(None, "wrong argv")
            )
            await supervisor.start_pager_broker()
            assert setup.ingest_socket_path is not None

            reader, writer = await asyncio.open_unix_connection(
                str(setup.ingest_socket_path)
            )
            del reader
            writer.write(b"not-json")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            await asyncio.sleep(0)

            preserved = read_pager_status(
                setup, tui_pid=os.getpid(), agent_version="1.0.13"
            )
            self.assertEqual(preserved.kind, "valid", preserved.reason)
            await supervisor.stop_pager_broker()

    async def test_trusted_malformed_pager_client_invalidates_focus(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            for path in (config.launch_home, config.grok_home, config.state_root):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            self.assertIsNone(stage_pager_status(setup))
            record_authenticated_pager_payload(
                setup,
                json.dumps(
                    {
                        "schema_version": 1,
                        "session_id": SESSION_1,
                        "cwd": str(config.project),
                        "version": "1.0.13",
                        "trigger": "state",
                    }
                ).encode(),
                tui_pid=os.getpid(),
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            supervisor.tui = TuiChild(os.getpid())
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]
            supervisor._tui_start_identity = process_start_identity(os.getpid())
            assert supervisor._tui_start_identity is not None
            proof = PagerPeerProof(
                os.getpid(),
                supervisor._tui_start_identity,
                os.getpid(),
                supervisor._tui_start_identity,
            )
            supervisor._pager_peer_is_owned = Mock(  # type: ignore[method-assign]
                return_value=(proof, "owned")
            )
            await supervisor.start_pager_broker()
            assert setup.ingest_socket_path is not None

            reader, writer = await asyncio.open_unix_connection(
                str(setup.ingest_socket_path)
            )
            writer.write(b"not-json")
            writer.write_eof()
            await writer.drain()
            self.assertEqual(await reader.read(1), b"1")
            writer.close()
            await writer.wait_closed()

            invalidated = read_pager_status(
                setup, tui_pid=os.getpid(), agent_version="1.0.13"
            )
            self.assertEqual(invalidated.kind, "transient")
            self.assertIn("not valid JSON", invalidated.reason)
            await supervisor.stop_pager_broker()

    async def test_pager_peer_identity_change_during_read_cannot_publish(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            for path in (config.launch_home, config.grok_home, config.state_root):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            self.assertIsNone(stage_pager_status(setup))
            record_authenticated_pager_payload(
                setup,
                json.dumps(
                    {
                        "schema_version": 1,
                        "session_id": SESSION_1,
                        "cwd": str(config.project),
                        "version": "1.0.13",
                        "trigger": "state",
                    }
                ).encode(),
                tui_pid=os.getpid(),
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            supervisor.tui = TuiChild(os.getpid())
            supervisor._tui_start_identity = process_start_identity(os.getpid())
            assert supervisor._tui_start_identity is not None
            proof = PagerPeerProof(
                456,
                "peer-start",
                os.getpid(),
                supervisor._tui_start_identity,
            )
            supervisor._pager_peer_is_owned = Mock(  # type: ignore[method-assign]
                return_value=(proof, "owned")
            )
            supervisor._pager_peer_proof_is_current = Mock(  # type: ignore[method-assign]
                return_value=(False, "pager peer identity changed during payload read")
            )
            await supervisor.start_pager_broker()
            assert setup.ingest_socket_path is not None

            reader, writer = await asyncio.open_unix_connection(
                str(setup.ingest_socket_path)
            )
            writer.write(
                json.dumps(
                    {
                        "schema_version": 1,
                        "session_id": SESSION_2,
                        "cwd": str(config.project),
                        "version": "1.0.13",
                        "trigger": "state",
                    }
                ).encode()
            )
            writer.write_eof()
            await writer.drain()
            self.assertEqual(await reader.read(1), b"")
            writer.close()
            await writer.wait_closed()

            preserved = read_pager_status(
                setup, tui_pid=os.getpid(), agent_version="1.0.13"
            )
            self.assertEqual(preserved.kind, "valid", preserved.reason)
            assert preserved.sample is not None
            self.assertEqual(preserved.sample.session_id, SESSION_1)
            await supervisor.stop_pager_broker()

    def test_pager_peer_requires_exact_binary_argv_and_owned_ancestry(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            for path in (config.launch_home, config.grok_home, config.state_root):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            supervisor.tui = TuiChild(123)
            supervisor.tui.exited = Mock(return_value=False)  # type: ignore[method-assign]
            supervisor._tui_start_identity = "tui-start"
            expected_argv = [
                "/python",
                str(setup.shim_path),
            ]
            module = Supervisor.__module__
            start_ids = {123: "tui-start", 456: "peer-start"}
            with patch(f"{module}.unix_peer_identity", return_value=(456, os.getuid())), patch(
                f"{module}.process_start_identity",
                side_effect=lambda pid: start_ids.get(pid),
            ), patch(f"{module}.live_process_executable", return_value="/python"), patch(
                f"{module}.process_argv", return_value=["/python", "wrong.py"]
            ), patch(f"{module}.process_parent_pid", return_value=123):
                proof, reason = supervisor._pager_peer_is_owned(object())
                self.assertIsNone(proof)
                self.assertIn("argv", reason)

            with patch(f"{module}.unix_peer_identity", return_value=(456, os.getuid())), patch(
                f"{module}.process_start_identity",
                side_effect=lambda pid: start_ids.get(pid),
            ), patch(f"{module}.live_process_executable", return_value="/python"), patch(
                f"{module}.process_argv", return_value=expected_argv
            ), patch(f"{module}.process_parent_pid", return_value=999):
                proof, reason = supervisor._pager_peer_is_owned(object())
                self.assertIsNone(proof)
                self.assertIn("direct child", reason)

            with patch(f"{module}.unix_peer_identity", return_value=(456, os.getuid())), patch(
                f"{module}.process_start_identity",
                side_effect=lambda pid: start_ids.get(pid),
            ), patch(f"{module}.live_process_executable", return_value="/python"), patch(
                f"{module}.process_argv", return_value=expected_argv
            ), patch(f"{module}.process_parent_pid", return_value=123):
                proof, reason = supervisor._pager_peer_is_owned(object())
                self.assertIsNotNone(proof, reason)
                assert proof is not None
                self.assertEqual(proof.pid, 456)
                current, current_reason = supervisor._pager_peer_proof_is_current(
                    proof
                )
                self.assertTrue(current, current_reason)

            changed_ids = {123: "tui-start", 456: "reused-peer"}
            with patch(
                f"{module}.process_start_identity",
                side_effect=lambda pid: changed_ids.get(pid),
            ):
                current, reason = supervisor._pager_peer_proof_is_current(proof)
            self.assertFalse(current)
            self.assertIn("pager peer identity changed", reason)

    async def test_socket_collisions_are_refused_and_foreign_paths_survive(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.grok_home.mkdir(parents=True)
            config.state_root.mkdir(parents=True)
            config.log_root.mkdir(parents=True)
            for path in (
                config.launch_home,
                config.grok_home,
                config.state_root,
                config.log_root,
            ):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            assert setup.ingest_socket_path is not None
            setup.ingest_socket_path.write_text("foreign", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "non-socket"):
                await supervisor.start_pager_broker()
            self.assertEqual(setup.ingest_socket_path.read_text(), "foreign")

            leader_path = Path(config.socket_path)
            leader_path.write_text("leader sentinel", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "non-socket"):
                await supervisor.spawn_leader()
            self.assertEqual(leader_path.read_text(), "leader sentinel")

            leader_path.unlink()
            outside = root / "outside"
            outside.write_text("outside", encoding="utf-8")
            leader_path.symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "non-socket"):
                await supervisor.spawn_leader()
            self.assertTrue(leader_path.is_symlink())
            self.assertEqual(outside.read_text(), "outside")

    def test_socket_cleanup_removes_only_exact_created_inode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "owned.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(path))
            expected = filesystem_socket_identity(path)
            self.assertIsNotNone(expected)
            listener.close()
            self.assertTrue(remove_owned_socket(path, expected))
            self.assertFalse(path.exists())

            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(path))
            old_identity = filesystem_socket_identity(path)
            listener.close()
            path.unlink()
            path.write_text("replacement", encoding="utf-8")
            self.assertFalse(remove_owned_socket(path, old_identity))
            self.assertEqual(path.read_text(), "replacement")

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
            supervisor.load_session = Mock(return_value=(SESSION_1, False))  # type: ignore[method-assign]
            supervisor.spawn_leader = AsyncMock()  # type: ignore[method-assign]
            supervisor.spawn_tui = AsyncMock()  # type: ignore[method-assign]
            supervisor.wait_session = AsyncMock()  # type: ignore[method-assign]
            fake_client = AsyncMock()
            fake_client.proc = None
            supervisor.create_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=make_binding(SESSION_1, fake_client)
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=make_binding(SESSION_1, fake_client)
            )
            supervisor.register_hcom = AsyncMock()  # type: ignore[method-assign]
            supervisor._hcom_registered = True
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

    def test_new_mode_defers_persistence_until_acp_returns_a_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.project.mkdir()
            config.state_root.mkdir()
            os.chmod(config.state_root, 0o700)
            config.session_path.write_text(
                json.dumps({"session_id": "old-session", "project": str(config.project)})
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            session_id, resume = supervisor.load_session()
            self.assertFalse(resume)
            self.assertEqual(session_id, "")
            self.assertEqual(
                json.loads(config.session_path.read_text())["session_id"], "old-session"
            )
            supervisor.adopt_created_session(SESSION_1)
            saved = json.loads(config.session_path.read_text())
            self.assertEqual(saved["session_id"], SESSION_1)
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
                f"{Supervisor.__module__}.session_directory",
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
            with patch(f"{Supervisor.__module__}.time.time", return_value=1234.75):
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
        with patch(f"{Supervisor.__module__}.subprocess.run", return_value=completed):
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
            self.assertEqual(await supervisor.process_mail(), "progressed")
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
                next_id = 0

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

                async def begin_request(self, method: str, params: dict) -> RpcHandle:
                    self.next_id += 1
                    response = asyncio.create_task(self.call(method, params, 900))
                    await asyncio.sleep(0)
                    return RpcHandle(self.next_id, method, response)

                async def await_response(
                    self, handle: RpcHandle, *, timeout: float
                ) -> dict:
                    return await asyncio.wait_for(
                        asyncio.shield(handle.response), timeout
                    )

                async def flush_notifications(self) -> None:
                    return None

            async def fake_hcom(
                args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                sent.append(args)
                return subprocess.CompletedProcess(args, 0, "ok", "")

            client = FakeClient()
            binding = make_binding("session-1", client, collector)
            supervisor.binding = binding
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", "session-1")
            )
            supervisor.hcom_command = fake_hcom  # type: ignore[method-assign]
            await supervisor.deliver(classified.envelope, binding)
            self.assertEqual(supervisor.cursor["last_event_id"], event_id)
            self.assertIsNone(supervisor.cursor["pending_reply"])
            self.assertEqual(len(sent), 1)
            self.assertIn("@kimo", sent[0])
            self.assertIn("working reply", sent[0])
            self.assertIn("feature-test", sent[0])

    async def test_visible_change_commits_new_binding_and_repairs_hcom(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            connection.execute(
                "INSERT INTO instances(name,session_id,parent_session_id) VALUES(?,?,?)",
                ("child-seat", "child-session", SESSION_1),
            )
            connection.commit()
            candidate_collector = TurnCollector(SESSION_2)
            candidate_client = RecordingClient(SESSION_2, candidate_collector)
            candidate = make_binding(
                SESSION_2, candidate_client, candidate_collector
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                ]
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=candidate
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]

            changed = await supervisor.rebind_visible(
                observation("visible-change", SESSION_2)
            )

            self.assertTrue(changed)
            self.assertIs(supervisor.binding, candidate)
            self.assertEqual(supervisor.session_id, SESSION_2)
            self.assertEqual(supervisor.binding_generation, 1)
            supervisor.close_binding.assert_awaited_once_with(old)  # type: ignore[attr-defined]
            saved = json.loads(supervisor.config.session_path.read_text())
            self.assertEqual(saved["session_id"], SESSION_2)
            self.assertEqual(saved["previous_session_id"], SESSION_1)
            self.assertEqual(saved["launch_session_id"], SESSION_1)
            rows = connection.execute(
                "SELECT session_id FROM session_bindings WHERE instance_name=?",
                (supervisor.config.seat,),
            ).fetchall()
            self.assertEqual(rows, [(SESSION_2,)])
            parent = connection.execute(
                "SELECT parent_session_id FROM instances WHERE name='child-seat'"
            ).fetchone()
            self.assertEqual(parent, (SESSION_2,))
            connection.close()

    async def test_delivery_gate_rereads_visible_session_before_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            insert_message(connection, intent="inform", text="new-session delivery")
            candidate_collector = TurnCollector(SESSION_2)
            candidate_client = RecordingClient(SESSION_2, candidate_collector)
            candidate = make_binding(
                SESSION_2, candidate_client, candidate_collector
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("aligned", SESSION_2),
                    observation("aligned", SESSION_2),
                ]
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=candidate
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]

            self.assertEqual(await supervisor.process_mail(), "progressed")

            old_client = old.client
            self.assertFalse(
                any(method == "session/prompt" for method, _ in old_client.calls)  # type: ignore[attr-defined]
            )
            prompts = [
                params
                for method, params in candidate_client.calls
                if method == "session/prompt"
            ]
            self.assertEqual(len(prompts), 1)
            self.assertEqual(prompts[0]["sessionId"], SESSION_2)
            prompt_body = prompts[0]["prompt"][0]["text"]  # type: ignore[index]
            self.assertIn("[HCOM BRIDGE CONTEXT]", prompt_body)
            connection.close()

    async def test_held_request_blocks_later_ack_cursor_advance(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _old = prepared_supervisor(Path(td))
            request_id = insert_message(connection, intent="request", text="hold me")
            ack_id = insert_message(connection, intent="ack", text="do not skip")
            self.assertGreater(ack_id, request_id)
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("unsafe", None, "ambiguous owned pid")
            )

            self.assertEqual(await supervisor.process_mail(), "held")
            self.assertEqual(supervisor.cursor["last_event_id"], 0)
            self.assertEqual(supervisor.bridge_state, "DEGRADED")
            connection.close()

    async def test_unsent_pending_reply_blocks_later_cursor_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _old = prepared_supervisor(Path(td))
            later_ack = insert_message(connection, intent="ack", text="later")
            supervisor.cursor["pending_reply"] = {
                "sender": "kimo",
                "reply_ref": "remote:1",
                "thread": "feature-test",
                "body": "reply",
                "source_event_id": 1,
            }
            atomic_json(supervisor.config.cursor_path, supervisor.cursor)

            async def failed_hcom(
                _args: list[str], timeout: float = 20
            ) -> subprocess.CompletedProcess[str]:
                return subprocess.CompletedProcess([], 1, "", "offline")

            supervisor.hcom_command = failed_hcom  # type: ignore[method-assign]
            self.assertEqual(await supervisor.process_mail(), "held")
            self.assertEqual(supervisor.cursor["last_event_id"], 0)
            self.assertIsInstance(supervisor.cursor["pending_reply"], dict)
            self.assertGreater(later_ack, supervisor.cursor["last_event_id"])
            connection.close()

    async def test_rapid_visible_switch_discards_first_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            collector_2 = TurnCollector(SESSION_2)
            client_2 = RecordingClient(SESSION_2, collector_2)
            candidate_2 = make_binding(SESSION_2, client_2, collector_2)
            collector_3 = TurnCollector(SESSION_3)
            client_3 = RecordingClient(SESSION_3, collector_3)
            candidate_3 = make_binding(SESSION_3, client_3, collector_3)
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_3),
                    observation("visible-change", SESSION_3),
                    observation("visible-change", SESSION_3),
                    observation("visible-change", SESSION_3),
                ]
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                side_effect=[candidate_2, candidate_3]
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]

            self.assertTrue(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertIs(supervisor.binding, candidate_3)
            self.assertEqual(supervisor.session_id, SESSION_3)
            self.assertEqual(
                [call.args[0] for call in supervisor.connect_sidecar.await_args_list],  # type: ignore[attr-defined]
                [SESSION_2, SESSION_3],
            )
            self.assertEqual(
                [call.args[0] for call in supervisor.close_binding.await_args_list],  # type: ignore[attr-defined]
                [candidate_2, old],
            )
            connection.close()

    async def test_newer_focus_at_commit_boundary_never_prompts_old_candidate(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            insert_message(connection, intent="inform", text="follow latest pane")
            collector_2 = TurnCollector(SESSION_2)
            client_2 = RecordingClient(SESSION_2, collector_2)
            candidate_2 = make_binding(SESSION_2, client_2, collector_2)
            collector_3 = TurnCollector(SESSION_3)
            client_3 = RecordingClient(SESSION_3, collector_3)
            candidate_3 = make_binding(SESSION_3, client_3, collector_3)
            supervisor._pager_focus_floor_ns = 10
            supervisor.observe_settled_binding = AsyncMock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2, state_ns=10),
                    observation("visible-change", SESSION_2, state_ns=10),
                    observation("visible-change", SESSION_2, state_ns=10),
                    observation("visible-change", SESSION_3, state_ns=20),
                    observation("visible-change", SESSION_3, state_ns=20),
                    observation("aligned", SESSION_3, state_ns=20),
                ]
            )
            commit_checks = 0

            def observe_now(_minimum: int | None = None) -> VisibleSessionObservation:
                nonlocal commit_checks
                commit_checks += 1
                if commit_checks == 1:
                    supervisor._pager_focus_floor_ns = 20
                    return observation("visible-change", SESSION_3, state_ns=20)
                return observation("aligned", SESSION_3, state_ns=20)

            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=observe_now
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                side_effect=[candidate_2, candidate_3]
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]

            self.assertEqual(await supervisor.process_mail(), "progressed")

            self.assertIs(supervisor.binding, candidate_3)
            self.assertEqual(supervisor.session_id, SESSION_3)
            self.assertFalse(
                any(method == "session/prompt" for method, _ in client_2.calls)
            )
            self.assertEqual(
                sum(method == "session/prompt" for method, _ in client_3.calls),
                1,
            )
            self.assertEqual(
                [call.args[0] for call in supervisor.close_binding.await_args_list],  # type: ignore[attr-defined]
                [candidate_2, old],
            )
            connection.close()

    async def test_close_first_switch_back_recreates_original_binding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(
                Path(td), sidecar_handoff="close-first"
            )
            collector_2 = TurnCollector(SESSION_2)
            candidate_2 = make_binding(
                SESSION_2, RecordingClient(SESSION_2, collector_2), collector_2
            )
            collector_1 = TurnCollector(SESSION_1)
            replacement_1 = make_binding(
                SESSION_1, RecordingClient(SESSION_1, collector_1), collector_1
            )
            supervisor.observe_settled_binding = AsyncMock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("aligned", SESSION_1),
                    observation("aligned", SESSION_1),
                    observation("aligned", SESSION_1),
                ]
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_1)
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                side_effect=[candidate_2, replacement_1]
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]

            self.assertTrue(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertIs(supervisor.binding, replacement_1)
            self.assertEqual(supervisor.session_id, SESSION_1)
            self.assertEqual(supervisor.bridge_state, "BOUND")
            self.assertEqual(
                [call.args[0] for call in supervisor.close_binding.await_args_list],  # type: ignore[attr-defined]
                [old, candidate_2],
            )
            self.assertEqual(
                [call.args[0] for call in supervisor.connect_sidecar.await_args_list],  # type: ignore[attr-defined]
                [SESSION_2, SESSION_1],
            )
            connection.close()

    async def test_mid_turn_change_finishes_on_captured_old_binding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            insert_message(connection, intent="inform", text="finish on old")
            old_client = old.client
            old_client.prompt_started = asyncio.Event()  # type: ignore[attr-defined]
            old_client.prompt_release = asyncio.Event()  # type: ignore[attr-defined]
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_1)
            )

            delivery = asyncio.create_task(supervisor.process_mail())
            await asyncio.wait_for(old_client.prompt_started.wait(), timeout=1)  # type: ignore[attr-defined]
            supervisor._record_observation(
                observation("visible-change", SESSION_2)
            )
            old_client.prompt_release.set()  # type: ignore[attr-defined]
            self.assertEqual(await asyncio.wait_for(delivery, timeout=1), "progressed")
            prompts = [
                params
                for method, params in old_client.calls  # type: ignore[attr-defined]
                if method == "session/prompt"
            ]
            self.assertEqual(len(prompts), 1)
            self.assertEqual(prompts[0]["sessionId"], SESSION_1)
            self.assertEqual(
                supervisor.cursor["last_event_id"],
                connection.execute("SELECT MAX(id) FROM events").fetchone()[0],
            )
            self.assertEqual(supervisor.bridge_state, "REBIND_PENDING")
            connection.close()

    async def test_mid_turn_focus_change_stops_before_later_ack(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(Path(td))
            request_id = insert_message(
                connection, intent="inform", text="finish current turn"
            )
            ack_id = insert_message(connection, intent="ack", text="later row")
            old_client = old.client
            old_client.prompt_started = asyncio.Event()  # type: ignore[attr-defined]
            old_client.prompt_release = asyncio.Event()  # type: ignore[attr-defined]
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_1)
            )

            delivery = asyncio.create_task(supervisor.process_mail())
            await asyncio.wait_for(old_client.prompt_started.wait(), timeout=1)  # type: ignore[attr-defined]
            supervisor._record_observation(
                observation("visible-change", SESSION_2)
            )
            old_client.prompt_release.set()  # type: ignore[attr-defined]

            self.assertEqual(await asyncio.wait_for(delivery, timeout=1), "progressed")
            self.assertEqual(supervisor.cursor["last_event_id"], request_id)
            self.assertLess(supervisor.cursor["last_event_id"], ack_id)
            self.assertEqual(supervisor.bridge_state, "REBIND_PENDING")
            connection.close()

    async def test_close_first_handoff_closes_old_before_candidate_attach(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(
                Path(td), sidecar_handoff="close-first"
            )
            collector = TurnCollector(SESSION_2)
            candidate = make_binding(
                SESSION_2, RecordingClient(SESSION_2, collector), collector
            )
            order: list[str] = []

            async def close(binding: SidecarBinding) -> None:
                order.append("close-old" if binding is old else "close-candidate")

            async def connect(
                target: str, *, generation: int | None = None
            ) -> SidecarBinding:
                self.assertEqual(target, SESSION_2)
                self.assertEqual(generation, 1)
                order.append("connect-candidate")
                return candidate

            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                ]
            )
            supervisor.close_binding = close  # type: ignore[method-assign]
            supervisor.connect_sidecar = connect  # type: ignore[method-assign]

            self.assertTrue(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertEqual(order, ["close-old", "connect-candidate"])
            connection.close()

    async def test_close_first_candidate_failure_holds_durable_session_and_mail(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(
                Path(td), sidecar_handoff="close-first"
            )
            cursor_before = (
                supervisor.config.cursor_path.read_bytes()
                if supervisor.config.cursor_path.exists()
                else None
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("visible-change", SESSION_2)
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                side_effect=RuntimeError("load rejected")
            )

            self.assertFalse(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertIsNone(supervisor.binding)
            self.assertEqual(supervisor.session_id, SESSION_1)
            self.assertEqual(
                json.loads(supervisor.config.session_path.read_text())["session_id"],
                SESSION_1,
            )
            self.assertEqual(supervisor.bridge_state, "DEGRADED")
            supervisor.close_binding.assert_awaited_once_with(old)  # type: ignore[attr-defined]
            if cursor_before is not None:
                self.assertEqual(supervisor.config.cursor_path.read_bytes(), cursor_before)
            connection.close()

    async def test_precommit_session_write_failure_keeps_old_authority(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, old = prepared_supervisor(
                Path(td), sidecar_handoff="concurrent"
            )
            collector = TurnCollector(SESSION_2)
            candidate = make_binding(
                SESSION_2, RecordingClient(SESSION_2, collector), collector
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                ]
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=candidate
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]
            supervisor.persist_binding_session = Mock(  # type: ignore[method-assign]
                side_effect=OSError("disk full")
            )

            self.assertFalse(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertIs(supervisor.binding, old)
            self.assertEqual(supervisor.session_id, SESSION_1)
            self.assertEqual(
                json.loads(supervisor.config.session_path.read_text())["session_id"],
                SESSION_1,
            )
            supervisor.close_binding.assert_awaited_once_with(candidate)  # type: ignore[attr-defined]
            connection.close()

    async def test_postcommit_hcom_failure_recovers_forward_to_new_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _old = prepared_supervisor(
                Path(td), sidecar_handoff="concurrent"
            )
            collector = TurnCollector(SESSION_2)
            candidate = make_binding(
                SESSION_2, RecordingClient(SESSION_2, collector), collector
            )
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                side_effect=[
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                    observation("visible-change", SESSION_2),
                ]
            )
            supervisor.connect_sidecar = AsyncMock(  # type: ignore[method-assign]
                return_value=candidate
            )
            supervisor.close_binding = AsyncMock()  # type: ignore[method-assign]
            original_mirror = supervisor._mirror_hcom_binding
            should_fail = True
            mirror_calls = 0

            def flaky_mirror(status: str, detail: str = "") -> None:
                nonlocal mirror_calls
                mirror_calls += 1
                if should_fail and mirror_calls == 2:
                    raise sqlite3.OperationalError("busy")
                original_mirror(status, detail)

            supervisor._mirror_hcom_binding = flaky_mirror  # type: ignore[method-assign]
            self.assertFalse(
                await supervisor.rebind_visible(
                    observation("visible-change", SESSION_2)
                )
            )
            self.assertEqual(supervisor.session_id, SESSION_2)
            self.assertIs(supervisor.binding, candidate)
            self.assertEqual(
                json.loads(supervisor.config.session_path.read_text())["session_id"],
                SESSION_2,
            )

            should_fail = False
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_2)
            )
            self.assertTrue(await supervisor.maintain_binding())
            self.assertEqual(supervisor.bridge_state, "BOUND")
            rows = connection.execute(
                "SELECT session_id FROM session_bindings WHERE instance_name=?",
                (supervisor.config.seat,),
            ).fetchall()
            self.assertEqual(rows, [(SESSION_2,)])
            connection.close()

    def test_session_json_wins_over_inverse_hcom_mirror(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _old = prepared_supervisor(Path(td))
            connection.execute(
                "UPDATE instances SET session_id=? WHERE name=?",
                (SESSION_2, supervisor.config.seat),
            )
            connection.execute(
                "INSERT OR REPLACE INTO session_bindings(session_id,instance_name,created_at) "
                "VALUES(?,?,?)",
                (SESSION_2, supervisor.config.seat, 1.0),
            )
            connection.commit()

            supervisor.heartbeat("listening", "repair")

            instance = connection.execute(
                "SELECT session_id FROM instances WHERE name=?",
                (supervisor.config.seat,),
            ).fetchone()
            bindings = connection.execute(
                "SELECT session_id FROM session_bindings WHERE instance_name=?",
                (supervisor.config.seat,),
            ).fetchall()
            self.assertEqual(instance, (SESSION_1,))
            self.assertEqual(bindings, [(SESSION_1,)])
            connection.close()

    async def test_runtime_monitor_heartbeats_while_delivery_is_active(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            supervisor, connection, _old = prepared_supervisor(Path(td))
            supervisor._delivery_active = True
            supervisor._active_event_id = 77
            supervisor.observe_binding = Mock(  # type: ignore[method-assign]
                return_value=observation("aligned", SESSION_1)
            )
            published: list[tuple[str, str]] = []

            async def publish(status: str, detail: str, **_extra: object) -> None:
                published.append((status, detail))
                if len(published) >= 3:
                    supervisor.stop_event.set()

            supervisor.publish_runtime = publish  # type: ignore[method-assign]
            with patch(
                f"{Supervisor.__module__}.OBSERVATION_SECONDS", 0.001
            ):
                await asyncio.wait_for(supervisor.runtime_monitor(), timeout=1)
            self.assertGreaterEqual(len(published), 3)
            self.assertTrue(all(status == "active" for status, _ in published))
            connection.close()

    def test_distinct_state_roots_get_distinct_default_sockets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            common = {
                "HOME": str(root / "home"),
                "HCOM_GROK_PROJECT": str(root / "project"),
                "HCOM_GROK_SEAT": "same-seat",
            }
            with patch.dict(
                os.environ,
                common | {"HCOM_GROK_STATE_ROOT": str(root / "state-a")},
                clear=True,
            ):
                first = Config.from_env(background_tui=True)
            with patch.dict(
                os.environ,
                common | {"HCOM_GROK_STATE_ROOT": str(root / "state-b")},
                clear=True,
            ):
                second = Config.from_env(background_tui=True)
            self.assertNotEqual(first.socket_path, second.socket_path)
            self.assertLess(len(first.socket_path.encode()), 100)

    def test_corrupt_session_json_is_never_overwritten_by_new_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="new")
            config.state_root.mkdir()
            config.session_path.write_text("{truncated")
            before = config.session_path.read_bytes()
            with self.assertRaisesRegex(RuntimeError, "unreadable or invalid"):
                Supervisor(config, "hcom-grok:test").load_session()
            self.assertEqual(config.session_path.read_bytes(), before)

    def test_v2_session_state_rejects_silent_generation_reset(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="resume")
            config.project.mkdir()
            config.state_root.mkdir()
            saved_directory = session_directory(
                config.grok_home, config.project, SESSION_1
            )
            saved_directory.mkdir(parents=True)
            config.session_path.write_text(
                json.dumps(
                    {
                        "schema": 2,
                        "session_id": SESSION_1,
                        "launch_session_id": SESSION_1,
                        "binding_generation": "bad",
                        "bound_at_ns": 1,
                        "previous_session_id": None,
                        "transition_reason": None,
                        "project": str(config.project),
                    }
                )
            )
            with self.assertRaisesRegex(RuntimeError, "binding_generation"):
                Supervisor(config, "hcom-grok:test").load_session()

    def test_v2_session_state_restores_current_rollback_compatible_id(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="resume")
            config.project.mkdir()
            config.state_root.mkdir()
            os.chmod(config.state_root, 0o700)
            session_directory(config.grok_home, config.project, SESSION_2).mkdir(
                parents=True
            )
            config.session_path.write_text(
                json.dumps(
                    {
                        "schema": 2,
                        "session_id": SESSION_2,
                        "launch_session_id": SESSION_1,
                        "binding_generation": 4,
                        "bound_at_ns": 8,
                        "previous_session_id": SESSION_3,
                        "transition_reason": "visible_tui_selection",
                        "project": str(config.project),
                        "created_ns": 7,
                    }
                )
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            session_id, resume = supervisor.load_session()
            self.assertTrue(resume)
            self.assertEqual(session_id, SESSION_2)
            self.assertEqual(supervisor.launch_session_id, SESSION_1)
            self.assertEqual(supervisor.binding_generation, 4)
            self.assertEqual(supervisor.previous_session_id, SESSION_3)
            supervisor.session_id = SESSION_2
            supervisor.persist_binding_session(SESSION_3)
            saved = json.loads(config.session_path.read_text())
            self.assertEqual(saved["session_id"], SESSION_3)
            self.assertEqual(saved["project"], str(config.project))
            self.assertEqual(saved["created_ns"], 7)

    def test_boolean_session_schema_is_not_treated_as_legacy_v1(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, session_mode="resume")
            config.project.mkdir()
            config.state_root.mkdir()
            session_directory(config.grok_home, config.project, SESSION_1).mkdir(
                parents=True
            )
            config.session_path.write_text(
                json.dumps(
                    {
                        "schema": True,
                        "session_id": SESSION_1,
                        "project": str(config.project),
                    }
                )
            )
            with self.assertRaisesRegex(RuntimeError, "Unsupported saved session schema"):
                Supervisor(config, "hcom-grok:test").load_session()

    def test_sidecar_identity_requires_uid_start_role_and_socket(self) -> None:
        argv = [
            "/usr/local/bin/grok",
            "agent",
            "--leader",
            "--leader-socket",
            "/tmp/seat.sock",
            "stdio",
        ]
        with patch(
            f"{Supervisor.__module__}.process_uid", return_value=501
        ), patch(
            f"{Supervisor.__module__}.process_start_identity",
            return_value="strong-start",
        ), patch(
            f"{Supervisor.__module__}.process_argv", return_value=argv
        ):
            self.assertTrue(
                sidecar_process_matches(
                    123,
                    expected_uid=501,
                    expected_start="strong-start",
                    leader_socket="/tmp/seat.sock",
                )
            )
            self.assertFalse(
                sidecar_process_matches(
                    123,
                    expected_uid=501,
                    expected_start="strong-start",
                    leader_socket="/tmp/other.sock",
                )
            )

    async def test_startup_reaps_only_a_positively_identified_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.state_root.mkdir()
            config.log_root.mkdir()
            os.chmod(config.state_root, 0o700)
            os.chmod(config.log_root, 0o700)
            atomic_json(
                config.run_path,
                {
                    "supervisor_pid": 999,
                    "binding_transition": {
                        "candidate_sidecar_pid": 123,
                        "candidate_uid": os.getuid(),
                        "candidate_process_start": "strong-start",
                        "leader_socket": config.socket_path,
                        "candidate_role": "grok-agent-stdio",
                    },
                },
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            candidate_checks = 0

            def alive(pid: int) -> bool:
                nonlocal candidate_checks
                if pid == 999:
                    return False
                candidate_checks += 1
                return candidate_checks == 1

            with patch(
                f"{Supervisor.__module__}.pid_alive", side_effect=alive
            ), patch(
                f"{Supervisor.__module__}.sidecar_process_matches",
                return_value=True,
            ), patch(
                f"{Supervisor.__module__}.process_group_alive",
                return_value=False,
            ), patch(f"{Supervisor.__module__}.os.killpg") as killpg:
                await supervisor.recover_stale_sidecars()
            killpg.assert_called_once_with(123, signal.SIGTERM)
            self.assertIsNone(supervisor._orphan_report)

    async def test_reused_supervisor_pid_does_not_block_verified_sidecar_recovery(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.state_root.mkdir()
            config.log_root.mkdir()
            os.chmod(config.state_root, 0o700)
            os.chmod(config.log_root, 0o700)
            atomic_json(
                config.run_path,
                {
                    "supervisor_pid": 999,
                    "supervisor_process_start": "old-supervisor",
                    "binding_transition": {
                        "candidate_sidecar_pid": 123,
                        "candidate_uid": os.getuid(),
                        "candidate_process_start": "strong-start",
                        "leader_socket": config.socket_path,
                        "candidate_role": "grok-agent-stdio",
                    },
                },
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            candidate_checks = 0

            def alive(pid: int) -> bool:
                nonlocal candidate_checks
                if pid == 999:
                    return True
                candidate_checks += 1
                return candidate_checks == 1

            module = Supervisor.__module__
            with patch(f"{module}.pid_alive", side_effect=alive), patch(
                f"{module}.process_start_identity", return_value="new-supervisor"
            ), patch(
                f"{module}.sidecar_process_matches", return_value=True
            ), patch(
                f"{module}.process_group_alive", return_value=False
            ), patch(f"{module}.os.killpg") as killpg:
                await supervisor.recover_stale_sidecars()
            killpg.assert_called_once_with(123, signal.SIGTERM)
            self.assertIsNone(supervisor._orphan_report)

    async def test_startup_reports_unproven_or_live_owner_without_signaling(self) -> None:
        for prior_live, identity_matches in ((True, True), (False, False)):
            with self.subTest(
                prior_live=prior_live, identity_matches=identity_matches
            ), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                config = make_config(root)
                config.state_root.mkdir()
                config.log_root.mkdir()
                os.chmod(config.state_root, 0o700)
                os.chmod(config.log_root, 0o700)
                atomic_json(
                    config.run_path,
                    {
                        "supervisor_pid": 999,
                        "binding_transition": {
                            "candidate_sidecar_pid": 123,
                            "candidate_uid": os.getuid(),
                            "candidate_process_start": "strong-start",
                            "leader_socket": config.socket_path,
                            "candidate_role": "grok-agent-stdio",
                        },
                    },
                )
                supervisor = Supervisor(config, "hcom-grok:test")

                def alive(pid: int) -> bool:
                    return prior_live if pid == 999 else True

                with patch(
                    f"{Supervisor.__module__}.pid_alive", side_effect=alive
                ), patch(
                    f"{Supervisor.__module__}.sidecar_process_matches",
                    return_value=identity_matches,
                ), patch(f"{Supervisor.__module__}.os.killpg") as killpg:
                    await supervisor.recover_stale_sidecars()
                killpg.assert_not_called()
                self.assertIsNotNone(supervisor._orphan_report)

    async def test_hard_crash_recovery_reaps_verified_tree_and_exact_sockets(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.state_root.mkdir()
            config.log_root.mkdir()
            config.grok_home.mkdir(parents=True)
            for path in (
                config.launch_home,
                config.state_root,
                config.log_root,
                config.grok_home,
            ):
                os.chmod(path, 0o700)
            setup = prepare_pager_status(
                state_root=config.state_root,
                launch_home=config.launch_home,
                grok_home=config.grok_home,
                isolated_home=True,
                host_home=root / "host-home",
                socket_root=root / "pager-sockets",
            )
            assert setup.ingest_socket_path is not None
            leader_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            leader_listener.bind(config.socket_path)
            pager_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            pager_listener.bind(str(setup.ingest_socket_path))
            leader_identity = filesystem_socket_identity(Path(config.socket_path))
            pager_identity = filesystem_socket_identity(setup.ingest_socket_path)
            assert leader_identity is not None and pager_identity is not None
            leader_listener.close()
            pager_listener.close()
            atomic_json(
                config.run_path,
                {
                    "supervisor_pid": 999,
                    "supervisor_process_start": "old-supervisor",
                    "tui_pid": 101,
                    "tui_process_start": "tui-start",
                    "tui_process_group": 101,
                    "sidecar_pid": 102,
                    "sidecar_process_start": "sidecar-start",
                    "leader_pid": 103,
                    "leader_process_start": "leader-start",
                    "session_id": SESSION_1,
                    "socket_path": config.socket_path,
                    "leader_socket_device": leader_identity[0],
                    "leader_socket_inode": leader_identity[1],
                    "pager_socket_path": str(setup.ingest_socket_path),
                    "pager_socket_device": pager_identity[0],
                    "pager_socket_inode": pager_identity[1],
                },
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.pager_status = setup
            live_pids = {101, 102, 103}
            live_groups = {101, 102, 103}
            signals: list[tuple[str, int, int]] = []

            def alive(pid: int) -> bool:
                return pid in live_pids

            def group_alive(pid: int) -> bool:
                return pid in live_groups

            def kill(pid: int, signum: int) -> None:
                signals.append(("pid", pid, signum))
                live_pids.discard(pid)

            def killpg(pid: int, signum: int) -> None:
                signals.append(("pgid", pid, signum))
                live_pids.discard(pid)
                live_groups.discard(pid)

            module = Supervisor.__module__
            with patch(f"{module}.pid_alive", side_effect=alive), patch(
                f"{module}.process_group_alive", side_effect=group_alive
            ), patch(f"{module}.tui_process_matches", return_value=True), patch(
                f"{module}.sidecar_process_matches", return_value=True
            ), patch(f"{module}.leader_process_matches", return_value=True), patch(
                f"{module}.os.kill", side_effect=kill
            ), patch(f"{module}.os.killpg", side_effect=killpg):
                await supervisor.recover_stale_runtime()

            self.assertEqual(
                signals,
                [
                    ("pgid", 101, signal.SIGTERM),
                    ("pgid", 102, signal.SIGTERM),
                    ("pgid", 103, signal.SIGTERM),
                ],
            )
            self.assertFalse(Path(config.socket_path).exists())
            self.assertFalse(setup.ingest_socket_path.exists())

    async def test_hard_crash_recovery_never_signals_unproven_tui(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.state_root.mkdir()
            config.log_root.mkdir()
            os.chmod(config.state_root, 0o700)
            os.chmod(config.log_root, 0o700)
            atomic_json(
                config.run_path,
                {
                    "supervisor_pid": 999,
                    "supervisor_process_start": "old-supervisor",
                    "tui_pid": 101,
                    "tui_process_start": "tui-start",
                    "tui_process_group": 101,
                    "session_id": SESSION_1,
                    "socket_path": config.socket_path,
                },
            )
            supervisor = Supervisor(config, "hcom-grok:test")
            module = Supervisor.__module__

            with patch(
                f"{module}.pid_alive", side_effect=lambda pid: pid == 101
            ), patch(f"{module}.tui_process_matches", return_value=False), patch(
                f"{module}.os.kill"
            ) as kill, patch(f"{module}.os.killpg") as killpg:
                with self.assertRaisesRegex(RuntimeError, "unproven stale tui"):
                    await supervisor.recover_stale_runtime()
            kill.assert_not_called()
            killpg.assert_not_called()

    async def test_rebound_resume_records_exact_tui_argv_session(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root, background=False, session_mode="resume")
            config.project.mkdir()
            config.state_root.mkdir()
            os.chmod(config.state_root, 0o700)
            supervisor = Supervisor(config, "hcom-grok:test")
            supervisor.launch_session_id = SESSION_1
            supervisor.session_id = SESSION_2
            fake_process = Mock(pid=321, returncode=None)
            module = Supervisor.__module__
            with patch(
                f"{module}.asyncio.create_subprocess_exec",
                AsyncMock(return_value=fake_process),
            ) as spawn, patch(
                f"{module}.process_start_identity", return_value="tui-start"
            ), patch(f"{module}.os.getpgid", return_value=77):
                await supervisor.spawn_tui(resume=True)

            argv = list(spawn.call_args.args)
            self.assertIn("--resume", argv)
            self.assertEqual(argv[argv.index("--resume") + 1], SESSION_2)
            self.assertEqual(supervisor._tui_argv_session_id, SESSION_2)
            supervisor.write_run_state(False, starting=True)
            state = json.loads(config.run_path.read_text(encoding="utf-8"))
            self.assertEqual(state["tui_argv_session_id"], SESSION_2)
            self.assertNotEqual(state["tui_argv_session_id"], SESSION_1)

    async def test_connect_sidecar_builds_target_scoped_validated_binding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.log_root.mkdir()
            os.chmod(config.log_root, 0o700)
            supervisor = Supervisor(config, "hcom-grok:test")
            spawned: list[object] = []

            class SpawnedClient:
                def __init__(self, sink: object, pid: int) -> None:
                    self.notification_sink = sink
                    self.proc = Mock(pid=pid)
                    self.closed = False

                async def close_transport(self) -> None:
                    self.closed = True

            async def spawn(_argv: list[str], **kwargs: object) -> SpawnedClient:
                client = SpawnedClient(kwargs["notification_sink"], 700 + len(spawned))
                spawned.append(client)
                return client

            handshake = AcpHandshake(
                1, "1.0.13", "cached_token", {"loadSession": True}, None
            )

            async def load(
                client: object,
                observed_handshake: AcpHandshake,
                session_id: str,
                project: Path,
                *,
                replay_fence: ResumeReplayFence,
            ) -> dict:
                self.assertIs(observed_handshake, handshake)
                self.assertEqual(session_id, SESSION_2)
                self.assertEqual(project, config.project)
                self.assertIs(client.notification_sink, replay_fence)  # type: ignore[attr-defined]
                await replay_fence.seal()
                return {"_meta": {"sessionId": SESSION_2}}

            with patch(
                f"{Supervisor.__module__}.AsyncAcpClient.spawn",
                side_effect=spawn,
            ) as spawn_call, patch(
                f"{Supervisor.__module__}.initialize_authenticated",
                AsyncMock(return_value=handshake),
            ), patch(
                f"{Supervisor.__module__}.load_acp_session",
                side_effect=load,
            ):
                binding = await supervisor.connect_sidecar(SESSION_2, generation=7)

            self.assertEqual(binding.session_id, SESSION_2)
            self.assertEqual(binding.permission.session_id, SESSION_2)
            self.assertEqual(binding.collector.session_id, SESSION_2)
            self.assertIs(binding.handshake, handshake)
            self.assertIs(binding.fence, binding.client.notification_sink)
            self.assertEqual(binding.generation, 7)
            argv = list(spawn_call.call_args.args[0])
            self.assertNotIn("--no-auto-update", argv)
            self.assertEqual(argv[-1], "stdio")
            self.assertIs(spawn_call.call_args.kwargs["start_new_session"], True)
            self.assertIn("sidecar-g7-", str(spawn_call.call_args.kwargs["log_path"]))

    async def test_connect_sidecar_closes_failed_candidate_without_mutating_binding(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            config = make_config(root)
            config.project.mkdir()
            config.log_root.mkdir()
            supervisor = Supervisor(config, "hcom-grok:test")
            old_collector = TurnCollector(SESSION_1)
            old = make_binding(
                SESSION_1, RecordingClient(SESSION_1, old_collector), old_collector
            )
            supervisor.binding = old

            class FailedClient:
                def __init__(self, sink: object) -> None:
                    self.notification_sink = sink
                    self.proc = Mock(pid=700)
                    self.closed = False

                async def close_transport(self) -> None:
                    self.closed = True

            failed: FailedClient | None = None

            async def spawn(_argv: list[str], **kwargs: object) -> FailedClient:
                nonlocal failed
                failed = FailedClient(kwargs["notification_sink"])
                return failed

            with patch(
                f"{Supervisor.__module__}.AsyncAcpClient.spawn",
                side_effect=spawn,
            ), patch(
                f"{Supervisor.__module__}.initialize_authenticated",
                AsyncMock(side_effect=RuntimeError("bad handshake")),
            ):
                with self.assertRaisesRegex(RuntimeError, "bad handshake"):
                    await supervisor.connect_sidecar(SESSION_2, generation=1)
            assert failed is not None
            self.assertTrue(failed.closed)
            self.assertIs(supervisor.binding, old)

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
