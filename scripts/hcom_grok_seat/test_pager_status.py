from __future__ import annotations

import json
import os
import errno
import shlex
import subprocess
import sys
import time
import tomllib
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from .pager_status import (
    CONFIG_NAME,
    MAX_STATUS_AGE_NS,
    OWNER_CLAIM_NAME,
    _encode_authenticated_record,
    cleanup_pager_status,
    pager_ownership_intact,
    prepare_pager_status,
    read_pager_status,
    record_authenticated_pager_payload,
    stage_pager_status,
)


class PagerStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.home = self.root / "seat-home"
        self.grok_home = self.home / ".grok"
        self.state_root = self.home / ".local/state/hcom-grok/current"
        self.host_home = self.root / "host-home"
        for path in (self.grok_home, self.state_root, self.host_home):
            path.mkdir(parents=True, exist_ok=True)
            os.chmod(path, 0o700)
        os.chmod(self.home, 0o700)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def prepare(self, **overrides):
        values = {
            "state_root": self.state_root,
            "launch_home": self.home,
            "grok_home": self.grok_home,
            "isolated_home": True,
            "environment": {},
            "host_home": self.host_home,
            "system_grok_root": self.root / "etc-grok",
            "python_executable": Path(sys.executable),
            "socket_root": self.root / "socket-root",
        }
        values.update(overrides)
        return prepare_pager_status(**values)

    @staticmethod
    def payload(session_id: str = "11111111-1111-4111-8111-111111111111") -> dict:
        return {
            "schema_version": 1,
            "session_id": session_id,
            "cwd": "/tmp/project",
            "version": "1.0.13",
            "trigger": "state",
        }

    def invoke_recorder(self, setup, payload: object) -> subprocess.CompletedProcess[bytes]:
        if setup.shim_path is not None and not setup.shim_path.exists():
            self.assertIsNone(stage_pager_status(setup))
        raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        record_authenticated_pager_payload(
            setup,
            raw,
            tui_pid=os.getpid(),
        )
        return subprocess.CompletedProcess([], 0, b"", b"")

    def recorder_command(self, setup) -> list[str]:
        if setup.shim_path is not None and not setup.shim_path.exists():
            self.assertIsNone(stage_pager_status(setup))
        parsed = tomllib.loads(setup.expected_block.decode("utf-8"))
        return shlex.split(parsed["ui"]["status_line"]["command"])

    def test_prepare_claims_private_home_without_touching_user_config(self) -> None:
        user_config = self.grok_home / "config.toml"
        user_config.write_text("[models]\ndefault = \"grok-4.6\"\n", encoding="utf-8")
        before = user_config.read_bytes()

        setup = self.prepare()

        self.assertTrue(setup.enabled, setup.reason)
        self.assertEqual(user_config.read_bytes(), before)
        self.assertEqual(setup.config_path.read_bytes(), before)
        self.assertTrue(setup.owner_claim_path.is_file())
        self.assertEqual(stage_pager_status(setup), None)
        parsed = tomllib.loads(setup.config_path.read_text(encoding="utf-8"))
        self.assertEqual(parsed["ui"]["status_line"]["type"], "command")
        self.assertEqual(parsed["ui"]["status_line"]["refresh_interval"], 1)
        command = parsed["ui"]["status_line"]["command"]
        self.assertEqual(shlex.split(command), [str(setup.shim_path)])
        self.assertNotIn(str(setup.ingest_socket_path), command)
        self.assertNotIn(str(setup.status_path), command)
        self.assertNotIn(setup.token, command)
        self.assertNotIn("PYTHONPATH", command)
        assert setup.shim_path is not None
        shim = setup.shim_path.read_text(encoding="utf-8")
        self.assertTrue(shim.startswith(f"#!{setup.python_executable}\n"))
        self.assertIn("sys.path.insert", shim)
        self.assertIn("scripts.hcom_grok_seat.pager_status import main", shim)
        self.assertIn(str(setup.ingest_socket_path), shim)
        self.assertEqual(setup.shim_path.stat().st_mode & 0o777, 0o700)
        self.assertEqual(
            setup.config_path.read_bytes(),
            before + setup.expected_block,
        )

    def test_owned_config_must_remain_present_while_recorder_is_authoritative(self) -> None:
        setup = self.prepare()
        self.assertTrue(setup.enabled, setup.reason)
        self.assertIsNone(stage_pager_status(setup))
        setup.config_path.unlink()
        result = self.invoke_recorder(setup, self.payload())
        self.assertEqual(result.returncode, 0)
        read = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
        )
        self.assertEqual(read.kind, "unsafe", read.reason)
        self.assertIn("ownership", read.reason)

    def test_one_shot_ownership_open_failure_is_transient_then_recovers(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        module = prepare_pager_status.__module__
        real_open = os.open
        failed = False

        def one_shot_emfile(path, flags, *args, **kwargs):
            nonlocal failed
            if not failed and Path(path) == setup.owner_claim_path:
                failed = True
                raise OSError(errno.EMFILE, "simulated descriptor exhaustion")
            return real_open(path, flags, *args, **kwargs)

        with patch(f"{module}.os.open", side_effect=one_shot_emfile):
            unavailable = read_pager_status(
                setup,
                tui_pid=os.getpid(),
                agent_version="1.0.13",
            )
            recovered = read_pager_status(
                setup,
                tui_pid=os.getpid(),
                agent_version="1.0.13",
            )

        self.assertEqual(unavailable.kind, "transient", unavailable.reason)
        self.assertIn("could not be opened", unavailable.reason)
        self.assertEqual(recovered.kind, "valid", recovered.reason)

    def test_exact_ownership_drift_remains_unsafe(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        setup.owner_claim_path.write_bytes(b"foreign claim bytes\n")
        os.chmod(setup.owner_claim_path, 0o600)

        result = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
        )

        self.assertEqual(result.kind, "unsafe", result.reason)
        self.assertIn("claim bytes drifted", result.reason)

    def test_supervisor_records_private_payload_for_owned_tui(self) -> None:
        setup = self.prepare()
        self.assertTrue(setup.enabled, setup.reason)
        result = self.invoke_recorder(setup, self.payload())
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"")
        self.assertEqual(setup.status_path.stat().st_mode & 0o777, 0o600)
        read = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
        )
        self.assertEqual(read.kind, "valid", read.reason)
        assert read.sample is not None
        self.assertEqual(read.sample.session_id, self.payload()["session_id"])
        self.assertEqual(read.sample.tui_pid, os.getpid())
        self.assertNotIn(setup.token, setup.status_path.read_text(encoding="utf-8"))
        wrong_parent = read_pager_status(
            setup,
            tui_pid=os.getpid() + 1,
            agent_version="1.0.13",
        )
        self.assertEqual(wrong_parent.kind, "unsafe")

    def test_unmediated_file_edit_cannot_forge_a_visible_session(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        value = json.loads(setup.status_path.read_text(encoding="utf-8"))
        value["session_id"] = "22222222-2222-4222-8222-222222222222"
        value["state_session_id"] = value["session_id"]
        value["captured_monotonic_ns"] = time.monotonic_ns()
        value["state_observed_monotonic_ns"] = value["captured_monotonic_ns"]
        setup.status_path.write_text(json.dumps(value), encoding="utf-8")
        os.chmod(setup.status_path, 0o600)
        read = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
        )
        self.assertEqual(read.kind, "unsafe")
        self.assertIn("authentication", read.reason)

    def test_refresh_cannot_introduce_a_new_focus_identity(self) -> None:
        setup = self.prepare()
        state_payload = self.payload()
        self.invoke_recorder(setup, state_payload)
        first = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert first.sample is not None
        refresh = self.payload("22222222-2222-4222-8222-222222222222")
        refresh["trigger"] = "refresh_interval"
        self.invoke_recorder(setup, refresh)
        after = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        self.assertEqual(after.kind, "transient")
        self.assertIn("disagrees", after.reason)

        self.invoke_recorder(setup, state_payload)
        restored = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert restored.sample is not None
        same_refresh = dict(state_payload)
        same_refresh["trigger"] = "refresh_interval"
        self.invoke_recorder(setup, same_refresh)
        valid_refresh = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert valid_refresh.sample is not None
        self.assertEqual(valid_refresh.sample.trigger, "refresh_interval")
        self.assertEqual(
            valid_refresh.sample.state_observed_monotonic_ns,
            restored.sample.state_observed_monotonic_ns,
        )

    def test_state_refresh_write_orders_never_restore_an_old_focus(self) -> None:
        setup = self.prepare()
        first = self.payload()
        second = self.payload("22222222-2222-4222-8222-222222222222")
        stale_refresh = dict(first)
        stale_refresh["trigger"] = "refresh_interval"

        self.invoke_recorder(setup, first)
        self.invoke_recorder(setup, second)
        self.invoke_recorder(setup, stale_refresh)
        state_then_stale = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        self.assertEqual(state_then_stale.kind, "transient")
        self.assertIn("disagrees", state_then_stale.reason)

        matching_second_refresh = dict(second)
        matching_second_refresh["trigger"] = "refresh_interval"
        self.invoke_recorder(setup, matching_second_refresh)
        recovered_liveness = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        self.assertEqual(recovered_liveness.kind, "valid", recovered_liveness.reason)
        assert recovered_liveness.sample is not None
        self.assertEqual(recovered_liveness.sample.session_id, second["session_id"])

        self.invoke_recorder(setup, first)
        self.invoke_recorder(setup, stale_refresh)
        self.invoke_recorder(setup, second)
        stale_then_state = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        self.assertEqual(stale_then_state.kind, "valid", stale_then_state.reason)
        assert stale_then_state.sample is not None
        self.assertEqual(stale_then_state.sample.session_id, second["session_id"])

    def test_cached_authenticated_record_below_focus_floor_is_rejected(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        cached_first = setup.status_path.read_bytes()
        first = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert first.sample is not None

        self.invoke_recorder(
            setup,
            self.payload("22222222-2222-4222-8222-222222222222"),
        )
        second = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert second.sample is not None
        self.assertGreater(
            second.sample.state_observed_monotonic_ns,
            first.sample.state_observed_monotonic_ns,
        )

        setup.status_path.write_bytes(cached_first)
        os.chmod(setup.status_path, 0o600)
        replayed = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
            minimum_state_monotonic_ns=(
                second.sample.state_observed_monotonic_ns
            ),
        )
        self.assertEqual(replayed.kind, "unsafe")
        self.assertIn("replayed", replayed.reason)

    def test_invalidation_publication_floor_rejects_same_state_cached_replay(
        self,
    ) -> None:
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        second = self.payload("22222222-2222-4222-8222-222222222222")
        state = record_authenticated_pager_payload(
            setup,
            json.dumps(second).encode(),
            tui_pid=os.getpid(),
        )
        self.assertIsNotNone(state.captured_monotonic_ns)
        cached = setup.status_path.read_bytes()

        stale_refresh = self.payload()
        stale_refresh["trigger"] = "refresh_interval"
        invalidated = record_authenticated_pager_payload(
            setup,
            json.dumps(stale_refresh).encode(),
            tui_pid=os.getpid(),
        )
        self.assertEqual(invalidated.kind, "invalidated")
        self.assertIsNotNone(invalidated.captured_monotonic_ns)
        assert state.captured_monotonic_ns is not None
        assert invalidated.captured_monotonic_ns is not None
        self.assertGreater(
            invalidated.captured_monotonic_ns,
            state.captured_monotonic_ns,
        )

        setup.status_path.write_bytes(cached)
        os.chmod(setup.status_path, 0o600)
        replayed = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
            minimum_monotonic_ns=invalidated.captured_monotonic_ns,
        )
        self.assertEqual(replayed.kind, "transient")
        self.assertIn("predates delivery gate", replayed.reason)

        matching_refresh = dict(second)
        matching_refresh["trigger"] = "refresh_interval"
        recovered = record_authenticated_pager_payload(
            setup,
            json.dumps(matching_refresh).encode(),
            tui_pid=os.getpid(),
        )
        self.assertEqual(recovered.kind, "recorded")
        assert recovered.captured_monotonic_ns is not None
        self.assertGreater(
            recovered.captured_monotonic_ns,
            invalidated.captured_monotonic_ns,
        )

    def test_record_timestamp_uses_supervisor_monotonic_clock(self) -> None:
        setup = self.prepare()
        before = time.monotonic_ns()
        self.invoke_recorder(setup, self.payload())
        after = time.monotonic_ns()
        read = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert read.sample is not None
        self.assertLessEqual(before, read.sample.captured_monotonic_ns)
        self.assertLessEqual(read.sample.captured_monotonic_ns, after)

    def test_post_admission_freshness_gate(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        current = read_pager_status(
            setup, tui_pid=os.getpid(), agent_version="1.0.13"
        )
        assert current.sample is not None
        future_gate = current.sample.captured_monotonic_ns + 1
        held = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
            minimum_monotonic_ns=future_gate,
        )
        self.assertEqual(held.kind, "transient")
        self.assertIn("predates delivery gate", held.reason)

    def test_status_sample_must_remain_private(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        os.chmod(setup.status_path, 0o644)
        result = read_pager_status(
            setup,
            tui_pid=os.getpid(),
            agent_version="1.0.13",
        )
        self.assertEqual(result.kind, "unsafe")
        self.assertIn("world-accessible", result.reason)

    def test_stale_future_token_version_and_schema_are_rejected(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        value = json.loads(setup.status_path.read_text(encoding="utf-8"))
        cases = (
            ("auth_tag", "0" * 64, "unsafe"),
            ("grok_version", "9.9.9", "unsafe"),
            ("status_schema_version", True, "unsafe"),
            ("captured_monotonic_ns", time.monotonic_ns() + 2_000_000_000, "unsafe"),
            (
                "captured_monotonic_ns",
                time.monotonic_ns() - MAX_STATUS_AGE_NS - 1,
                "transient",
            ),
        )
        for key, replacement, expected in cases:
            with self.subTest(key=key, replacement=replacement):
                changed = dict(value)
                changed[key] = replacement
                if key == "captured_monotonic_ns":
                    changed["state_observed_monotonic_ns"] = replacement
                if key == "auth_tag":
                    encoded = (json.dumps(changed) + "\n").encode()
                else:
                    changed.pop("auth_tag", None)
                    encoded = _encode_authenticated_record(changed, setup.token)
                setup.status_path.write_bytes(encoded)
                result = read_pager_status(
                    setup,
                    tui_pid=os.getpid(),
                    agent_version="1.0.13",
                )
                self.assertEqual(result.kind, expected, result.reason)

    def test_recorder_drops_malformed_or_oversized_input(self) -> None:
        setup = self.prepare()
        self.invoke_recorder(setup, self.payload())
        for body in (b"not-json", b"{" + b"x" * 70_000 + b"}"):
            with self.subTest(length=len(body)):
                outcome = record_authenticated_pager_payload(
                    setup,
                    body,
                    tui_pid=os.getpid(),
                )
                self.assertEqual(outcome.kind, "invalidated")
                invalid = read_pager_status(
                    setup,
                    tui_pid=os.getpid(),
                    agent_version="1.0.13",
                )
                self.assertEqual(invalid.kind, "transient")

    def test_status_client_is_silent_when_broker_is_absent(self) -> None:
        setup = self.prepare()
        command = self.recorder_command(setup)
        result = subprocess.run(
            command,
            input=json.dumps(self.payload()).encode(),
            cwd=self.root,
            env={"HOME": str(self.home), "PATH": os.defpath},
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"")
        self.assertFalse(setup.status_path.exists())

    def test_recorder_without_owned_command_arguments_is_inert(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.hcom_grok_seat.pager_status",
                "--submit-pager-status",
                "--socket-path",
                str(self.state_root / "unexpected.sock"),
            ],
            input=json.dumps(self.payload()).encode(),
            env={},
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"")
        self.assertFalse((self.state_root / "unexpected.json").exists())

    def test_refuses_shared_or_unproven_home_shapes(self) -> None:
        outside = self.root / "outside-state"
        outside.mkdir()
        cases = (
            ({"isolated_home": False}, "explicitly isolated"),
            ({"launch_home": self.host_home, "grok_home": self.host_home / ".grok"}, "shared host"),
            ({"grok_home": self.root / "different-grok"}, "HOME/.grok"),
            ({"state_root": outside}, "inside the isolated HOME"),
            ({"environment": {"GROK_CONFIG": "{}"}}, "config overlay"),
            ({"environment": {"GROK_CONFIG_PATH": "/tmp/x"}}, "config overlay"),
        )
        (self.host_home / ".grok").mkdir(exist_ok=True)
        (self.root / "different-grok").mkdir(exist_ok=True)
        for kwargs, reason in cases:
            with self.subTest(kwargs=kwargs):
                setup = self.prepare(**kwargs)
                self.assertFalse(setup.enabled)
                self.assertIn(reason, setup.reason)

    def test_refuses_user_status_line_and_managed_sync_override(self) -> None:
        config = self.grok_home / "config.toml"
        for text, reason in (
            ('[ui.status_line]\ntype = "disabled"\n', "foreign"),
            ("[features]\nmanaged_config = true\n", "synchronization"),
            ("not = [valid", "cannot validate"),
        ):
            with self.subTest(text=text):
                config.write_text(text, encoding="utf-8")
                setup = self.prepare()
                self.assertFalse(setup.enabled)
                self.assertIn(reason, setup.reason)

    def test_allows_unrelated_system_policy_but_refuses_seat_policy(self) -> None:
        system_root = self.root / "etc-grok"
        system_root.mkdir()
        policy = system_root / "managed_config.toml"
        policy.write_text("[ui]\n", encoding="utf-8")
        setup = self.prepare(system_grok_root=system_root)
        self.assertTrue(setup.enabled, setup.reason)
        cleanup_pager_status(setup)

        signature = self.grok_home / "managed_config.sig.json"
        signature.write_text("{}\n", encoding="utf-8")
        setup = self.prepare(system_grok_root=system_root)
        self.assertFalse(setup.enabled)
        self.assertIn("signed seat", setup.reason)
        signature.unlink()

        config = self.grok_home / CONFIG_NAME
        config.write_text("[ui.status_line]\ntype='command'\n", encoding="utf-8")
        setup = self.prepare()
        self.assertFalse(setup.enabled)
        self.assertIn("foreign", setup.reason)

    def test_second_state_root_cannot_claim_same_home(self) -> None:
        first = self.prepare()
        self.assertTrue(first.enabled, first.reason)
        second_state = self.home / ".local/state/hcom-grok/other"
        second_state.mkdir(parents=True)
        os.chmod(second_state, 0o700)
        second = self.prepare(state_root=second_state)
        self.assertFalse(second.enabled)
        self.assertIn("claimed by another", second.reason)

    def test_same_owner_reclaims_exact_stale_config_block(self) -> None:
        first = self.prepare()
        self.assertIsNone(stage_pager_status(first))
        second = self.prepare()
        self.assertTrue(second.enabled, second.reason)
        self.assertTrue(second.config_path.exists())
        self.assertIsNone(stage_pager_status(second))

    def test_refuses_group_or_world_accessible_private_roots(self) -> None:
        for path in (self.home, self.grok_home, self.state_root):
            with self.subTest(path=path):
                os.chmod(path, 0o755)
                setup = self.prepare()
                self.assertFalse(setup.enabled)
                self.assertIn("private directories", setup.reason)
                os.chmod(path, 0o700)

    def test_stale_config_marker_without_claim_is_not_deleted(self) -> None:
        first = self.prepare()
        self.assertIsNone(stage_pager_status(first))
        first.owner_claim_path.unlink()
        before = first.config_path.read_bytes()
        second = self.prepare()
        self.assertFalse(second.enabled)
        self.assertIn("no matching ownership claim", second.reason)
        self.assertEqual(first.config_path.read_bytes(), before)

    def test_stage_never_replaces_a_racing_file(self) -> None:
        setup = self.prepare()
        setup.config_path.write_text("foreign\n", encoding="utf-8")
        reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        self.assertEqual(setup.config_path.read_text(encoding="utf-8"), "foreign\n")

    def test_stage_transaction_never_clobbers_a_racing_valid_config(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        replacement = b'[models]\ndefault = "replacement"\n'
        self.grok_home.joinpath(CONFIG_NAME).write_bytes(original)
        setup = self.prepare()
        real_rename = os.rename

        def racing_rename(source, destination, *args, **kwargs):
            result = real_rename(source, destination, *args, **kwargs)
            if Path(source) == setup.config_path:
                setup.config_path.write_bytes(replacement)
            return result

        module = prepare_pager_status.__module__
        with patch(f"{module}.os.rename", side_effect=racing_rename):
            reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        self.assertEqual(setup.config_path.read_bytes(), replacement)
        self.assertTrue(setup.config_transaction_path.is_file())

    def test_open_fd_writer_during_publication_rolls_back_to_newer_bytes(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        replacement = b'[models]\ndefault = "written-through-old-fd"\n'
        config = self.grok_home / CONFIG_NAME
        config.write_bytes(original)
        setup = self.prepare()
        writer_fd = os.open(config, os.O_WRONLY)
        real_link = os.link
        raced = False

        def racing_link(source, destination, *args, **kwargs):
            nonlocal raced
            if not raced and Path(destination) == setup.config_path:
                raced = True
                os.ftruncate(writer_fd, 0)
                os.lseek(writer_fd, 0, os.SEEK_SET)
                os.write(writer_fd, replacement)
                os.fsync(writer_fd)
            return real_link(source, destination, *args, **kwargs)

        module = prepare_pager_status.__module__
        try:
            with patch(f"{module}.os.link", side_effect=racing_link):
                reason = stage_pager_status(setup)
        finally:
            os.close(writer_fd)

        self.assertIsNotNone(reason)
        self.assertIn("changed during pager publication", reason or "")
        self.assertEqual(config.read_bytes(), replacement)
        self.assertNotIn(setup.expected_block, config.read_bytes())
        self.assertFalse(setup.config_transaction_path.exists())
        self.assertEqual(list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*")), [])

    def test_preexisting_hardlinked_config_is_refused_without_mutation(self) -> None:
        original = b'[models]\ndefault = "hardlinked"\n'
        config = self.grok_home / CONFIG_NAME
        alias = self.root / "config-alias.toml"
        config.write_bytes(original)
        os.link(config, alias)

        setup = self.prepare()

        self.assertFalse(setup.enabled)
        self.assertIn("safely read config.toml", setup.reason)
        self.assertEqual(config.read_bytes(), original)
        self.assertEqual(alias.read_bytes(), original)
        self.assertEqual(config.stat().st_ino, alias.stat().st_ino)
        self.assertFalse(setup.owner_claim_path.exists())

    def test_foreign_owned_config_is_rejected_at_admission_and_staging(self) -> None:
        original = b'[models]\ndefault = "owner-check"\n'
        config = self.grok_home / CONFIG_NAME
        config.write_bytes(original)
        inode = config.stat().st_ino
        module = prepare_pager_status.__module__
        real_fstat = os.fstat

        def foreign_fstat(fd):
            info = real_fstat(fd)
            if info.st_ino != inode:
                return info
            values = list(info)
            values[4] = os.getuid() + 1
            return os.stat_result(values)

        with patch(f"{module}.os.fstat", side_effect=foreign_fstat):
            refused = self.prepare()
        self.assertFalse(refused.enabled)
        self.assertEqual(config.read_bytes(), original)
        self.assertFalse(refused.owner_claim_path.exists())

        setup = self.prepare()
        with patch(f"{module}.os.fstat", side_effect=foreign_fstat):
            reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        self.assertIn("unsafe", reason or "")
        self.assertEqual(config.read_bytes(), original)

    def test_metadata_change_before_detach_is_preserved(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        config = self.grok_home / CONFIG_NAME
        config.write_bytes(original)
        os.chmod(config, 0o640)
        setup = self.prepare()
        real_rename = os.rename

        def racing_rename(source, destination, *args, **kwargs):
            if Path(source) == setup.config_path:
                os.chmod(setup.config_path, 0o600)
            return real_rename(source, destination, *args, **kwargs)

        module = prepare_pager_status.__module__
        with patch(f"{module}.os.rename", side_effect=racing_rename):
            self.assertIsNone(stage_pager_status(setup))
        self.assertEqual(config.stat().st_mode & 0o777, 0o600)

    def test_journal_creation_error_leaves_no_full_config_temporary(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        self.grok_home.joinpath(CONFIG_NAME).write_bytes(original)
        setup = self.prepare()
        module = prepare_pager_status.__module__
        real_create = __import__(module, fromlist=["_atomic_create"])._atomic_create

        def fail_journal(path, payload, *, mode=0o600):
            if Path(path) == setup.config_transaction_path:
                raise OSError(errno.EIO, "simulated journal failure")
            return real_create(path, payload, mode=mode)

        with patch(f"{module}._atomic_create", side_effect=fail_journal):
            reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        self.assertEqual(setup.config_path.read_bytes(), original)
        self.assertFalse(setup.config_transaction_path.exists())
        self.assertEqual(
            list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*.tmp")),
            [],
        )

    def test_recovery_discards_partial_temp_written_before_detach(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        config = self.grok_home / CONFIG_NAME
        config.write_bytes(original)
        setup = self.prepare()
        module = prepare_pager_status.__module__

        def interrupted_write(path, _payload):
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(fd, b"partial only")
                os.fsync(fd)
            finally:
                os.close(fd)
            raise SystemExit("simulated hard stop")

        with patch(
            f"{module}._write_transform_temporary",
            side_effect=interrupted_write,
        ):
            with self.assertRaisesRegex(SystemExit, "simulated hard stop"):
                stage_pager_status(setup)

        self.assertEqual(config.read_bytes(), original)
        self.assertTrue(setup.config_transaction_path.exists())
        self.assertEqual(
            len(list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*.tmp"))),
            1,
        )
        pager_module = __import__(module, fromlist=["recover_pager_config"])
        self.assertIsNone(pager_module.recover_pager_config(setup))
        self.assertEqual(config.read_bytes(), original)
        self.assertFalse(setup.config_transaction_path.exists())
        self.assertEqual(list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*")), [])
        self.assertIsNone(stage_pager_status(setup))

    def test_interrupted_injection_rolls_back_without_residue(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        self.grok_home.joinpath(CONFIG_NAME).write_bytes(original)
        setup = self.prepare()
        real_link = os.link
        failed = False

        def interrupted_link(source, destination, *args, **kwargs):
            nonlocal failed
            if not failed and Path(destination) == setup.config_path:
                failed = True
                raise OSError(errno.EIO, "simulated interruption")
            return real_link(source, destination, *args, **kwargs)

        module = prepare_pager_status.__module__
        with patch(f"{module}.os.link", side_effect=interrupted_link):
            reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        self.assertEqual(setup.config_path.read_bytes(), original)
        self.assertFalse(setup.config_transaction_path.exists())

        self.assertIsNone(stage_pager_status(setup))
        self.assertEqual(
            setup.config_path.read_bytes(),
            original + setup.expected_block,
        )
        self.assertFalse(setup.config_transaction_path.exists())

    def test_recovery_with_missing_quarantine_preserves_newer_target(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        newer = b'[models]\ndefault = "newer"\n'
        config = self.grok_home / CONFIG_NAME
        config.write_bytes(original)
        setup = self.prepare()
        module = __import__(prepare_pager_status.__module__, fromlist=["*"])
        intended = original + setup.expected_block
        quarantine = setup.config_path.with_name(
            f".{CONFIG_NAME}.hcom-{setup.owner_id}-crash.bak"
        )
        temporary = setup.config_path.with_name(
            f".{CONFIG_NAME}.hcom-{setup.owner_id}-crash.tmp"
        )
        journal = module._transaction_record(
            setup,
            operation="inject",
            quarantine=quarantine,
            temporary=temporary,
            original=original,
            intended=intended,
        )
        self.assertTrue(module._atomic_create(setup.config_transaction_path, journal))
        module._write_transform_temporary(temporary, intended)
        config.write_bytes(newer)

        self.assertIsNone(module.recover_pager_config(setup))
        self.assertEqual(config.read_bytes(), newer)
        self.assertFalse(temporary.exists())
        self.assertFalse(setup.config_transaction_path.exists())

    def test_recovery_completes_partial_hardlink_restore(self) -> None:
        original = b'[models]\ndefault = "first"\n'
        self.grok_home.joinpath(CONFIG_NAME).write_bytes(original)
        setup = self.prepare()
        module = prepare_pager_status.__module__
        real_link = os.link
        failed = False

        def publication_failure(source, destination, *args, **kwargs):
            nonlocal failed
            if not failed and Path(destination) == setup.config_path:
                failed = True
                raise OSError(errno.EIO, "simulated publish failure")
            return real_link(source, destination, *args, **kwargs)

        def partial_restore(detached, destination):
            real_link(detached, destination, follow_symlinks=False)
            return False

        with patch(f"{module}.os.link", side_effect=publication_failure), patch(
            f"{module}._restore_detached_path",
            side_effect=partial_restore,
        ):
            reason = stage_pager_status(setup)
        self.assertIsNotNone(reason)
        quarantines = list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*.bak"))
        self.assertEqual(len(quarantines), 1)
        self.assertEqual(setup.config_path.stat().st_ino, quarantines[0].stat().st_ino)
        self.assertTrue(setup.config_transaction_path.exists())

        self.assertIsNone(stage_pager_status(setup))
        self.assertEqual(
            setup.config_path.read_bytes(),
            original + setup.expected_block,
        )
        self.assertEqual(
            list(self.grok_home.glob(f".{CONFIG_NAME}.hcom-*")),
            [],
        )
        self.assertFalse(setup.config_transaction_path.exists())

    def test_semantic_owned_status_survives_marker_stripping_fail_closed_cleanup(
        self,
    ) -> None:
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        marker = f"{os.linesep}# managed-by: hcom-grok pager-status-v1 owner={setup.owner_id}{os.linesep}"
        raw = setup.config_path.read_text(encoding="utf-8")
        setup.config_path.write_text(raw.replace(marker, "", 1), encoding="utf-8")
        os.chmod(setup.config_path, 0o600)

        resumed = self.prepare()
        self.assertTrue(resumed.enabled, resumed.reason)
        self.assertIsNone(stage_pager_status(resumed))
        self.assertTrue(pager_ownership_intact(resumed))
        retained = cleanup_pager_status(resumed)
        self.assertIn(str(resumed.config_path), retained)
        self.assertIn(str(resumed.owner_claim_path), retained)
        self.assertIn("[ui.status_line]", resumed.config_path.read_text(encoding="utf-8"))

    def test_existing_empty_or_whitespace_config_is_refused_without_mutation(self) -> None:
        config = self.grok_home / CONFIG_NAME
        for raw in (b"", b" \n\t"):
            with self.subTest(raw=raw):
                config.write_bytes(raw)
                setup = self.prepare()
                self.assertFalse(setup.enabled)
                self.assertIn("empty existing", setup.reason)
                self.assertEqual(config.read_bytes(), raw)

    @unittest.skipUnless(sys.platform == "darwin", "macOS metadata contract")
    def test_existing_config_mode_and_xattr_survive_injection_and_cleanup(self) -> None:
        config = self.grok_home / CONFIG_NAME
        original = b'[models]\ndefault = "grok-4.6"\n'
        config.write_bytes(original)
        os.chmod(config, 0o640)
        subprocess.run(
            ["/usr/bin/xattr", "-w", "hcom.test-metadata", "preserve", str(config)],
            check=True,
            capture_output=True,
        )
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        self.assertEqual(config.stat().st_mode & 0o777, 0o640)
        staged_xattr = subprocess.run(
            ["/usr/bin/xattr", "-p", "hcom.test-metadata", str(config)],
            check=True,
            capture_output=True,
        )
        self.assertEqual(staged_xattr.stdout.rstrip(b"\n"), b"preserve")

        self.assertEqual(cleanup_pager_status(setup), [])
        self.assertEqual(config.read_bytes(), original)
        self.assertEqual(config.stat().st_mode & 0o777, 0o640)
        cleaned_xattr = subprocess.run(
            ["/usr/bin/xattr", "-p", "hcom.test-metadata", str(config)],
            check=True,
            capture_output=True,
        )
        self.assertEqual(cleaned_xattr.stdout.rstrip(b"\n"), b"preserve")

    def test_managed_and_requirements_files_are_never_used_or_changed(self) -> None:
        managed = self.grok_home / "managed_config.toml"
        requirements = self.grok_home / "requirements.toml"
        managed.write_bytes(b'[models]\ndefault = "policy"\n')
        requirements.write_bytes(b'[requirements]\nsource = "owner"\n')
        before = (managed.read_bytes(), requirements.read_bytes())

        setup = self.prepare()
        self.assertTrue(setup.enabled, setup.reason)
        self.assertIsNone(stage_pager_status(setup))
        self.assertEqual(cleanup_pager_status(setup), [])
        self.assertEqual(
            (managed.read_bytes(), requirements.read_bytes()),
            before,
        )

    def test_cleanup_removes_only_exact_owned_artifacts(self) -> None:
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        self.invoke_recorder(setup, self.payload())
        retained = cleanup_pager_status(setup)
        self.assertEqual(retained, [])
        self.assertFalse(setup.config_path.exists())
        self.assertFalse(setup.status_path.exists())
        self.assertFalse(setup.owner_claim_path.exists())

        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        setup.config_path.write_text("drifted\n", encoding="utf-8")
        retained = cleanup_pager_status(setup)
        self.assertIn(str(setup.config_path), retained)
        self.assertEqual(setup.config_path.read_text(), "drifted\n")

    def test_cleanup_preserves_existing_and_grok_appended_config(self) -> None:
        original = b'[models]\ndefault = "grok-4.6"\n'
        self.grok_home.joinpath(CONFIG_NAME).write_bytes(original)
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        appended = (
            b'\n[marketplace]\n'
            b'official_marketplace_auto_installed = true\n'
        )
        with setup.config_path.open("ab") as handle:
            handle.write(appended)
        self.assertEqual(cleanup_pager_status(setup), [])
        self.assertEqual(
            setup.config_path.read_bytes(),
            original + appended,
        )

    def test_cleanup_never_deletes_replacement_arriving_during_detach(self) -> None:
        setup = self.prepare()
        self.assertIsNone(stage_pager_status(setup))
        real_rename = os.rename

        def racing_rename(source, destination, *args, **kwargs):
            result = real_rename(source, destination, *args, **kwargs)
            if Path(source) == setup.config_path:
                setup.config_path.write_text(
                    "foreign replacement\n", encoding="utf-8"
                )
                os.chmod(setup.config_path, 0o600)
            return result

        module = prepare_pager_status.__module__
        with patch(f"{module}.os.rename", side_effect=racing_rename):
            retained = cleanup_pager_status(setup)
        self.assertIn(str(setup.config_path), retained)
        self.assertEqual(
            setup.config_path.read_text(encoding="utf-8"),
            "foreign replacement\n",
        )

    def test_symlinked_config_file_is_never_followed_or_replaced(self) -> None:
        outside = self.root / "outside.toml"
        outside.write_text("untouched\n", encoding="utf-8")
        (self.grok_home / CONFIG_NAME).symlink_to(outside)
        setup = self.prepare()
        self.assertFalse(setup.enabled)
        self.assertEqual(outside.read_text(), "untouched\n")

    def test_wrong_claim_is_never_replaced(self) -> None:
        claim = self.grok_home / OWNER_CLAIM_NAME
        claim.write_text('{"owner_id":"foreign"}\n', encoding="utf-8")
        before = claim.read_bytes()
        setup = self.prepare()
        self.assertFalse(setup.enabled)
        self.assertEqual(claim.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
