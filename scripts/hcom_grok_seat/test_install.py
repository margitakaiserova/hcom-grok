"""Focused tests for versioned local installation and rollback."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from . import install


def _source(root: Path, body: str) -> Path:
    package = root / "source"
    package.mkdir(exist_ok=True)
    (package / "__init__.py").write_text("\"\"\"test package\"\"\"\n")
    (package / "operator.py").write_text(body)
    (package / "supervisor.py").write_text("def main(): return 0\n")
    return package


class InstallTests(unittest.TestCase):
    def test_real_installed_launcher_imports_package_from_any_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            package = Path(__file__).resolve().parent
            installed = install.install_release(
                package, root / "releases", root / "bin", "integration-v1"
            )
            release = Path(installed["release"])
            manifest = json.loads((release / "manifest.json").read_text())
            self.assertIn("visible_session.py", manifest["files"])
            self.assertIn("pager_status.py", manifest["files"])
            imported = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from scripts.hcom_grok_seat import pager_status, supervisor, visible_session; "
                    "assert supervisor.observe_visible_session is "
                    "visible_session.observe_visible_session; "
                    "assert supervisor.observe_pager_session is "
                    "visible_session.observe_pager_session; "
                    "assert pager_status.PAGER_RECORD_SCHEMA == 1",
                ],
                cwd=root,
                env={**os.environ, "PYTHONPATH": str(release)},
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(imported.returncode, 0, imported.stderr)
            env = os.environ.copy()
            env.update(
                {
                    "HCOM_GROK_STATE_ROOT": str(root / "state"),
                    "HCOM_GROK_LOG_ROOT": str(root / "logs"),
                    "HCOM_GROK_RELEASE_ROOT": str(root / "releases"),
                    "HCOM_GROK_BIN_ROOT": str(root / "bin"),
                    "HCOM_GROK_HCOM_DB": str(root / "hcom.db"),
                }
            )
            result = subprocess.run(
                [str(root / "bin/hcom-grok"), "--json", "status"],
                cwd=root,
                env=env,
                capture_output=True,
                text=True,
                timeout=5,
            )
            self.assertEqual(result.returncode, 3, result.stderr)
            self.assertFalse(json.loads(result.stdout)["running"])
            self.assertNotIn(str(Path.cwd()), result.stderr)

    def test_install_copies_exact_python_files_and_switches_current(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            result = install.install_release(source, root / "releases", root / "bin", "v1")
            release = root / "releases" / "v1"
            self.assertEqual((release / "scripts/hcom_grok_seat/operator.py").read_bytes(), (source / "operator.py").read_bytes())
            self.assertEqual(sorted(json.loads((release / "manifest.json").read_text())["files"]), ["__init__.py", "operator.py", "supervisor.py"])
            self.assertEqual(os.readlink(root / "releases/current"), "v1")
            self.assertEqual(Path(result["release"]), release.resolve())
            launcher = root / "bin/hcom-grok"
            self.assertIn(install.LAUNCHER_MARKER, launcher.read_text())
            self.assertEqual(launcher.stat().st_mode & 0o777, 0o700)
            self.assertEqual(release.stat().st_mode & 0o077, 0)

    def test_second_install_preserves_previous_and_rollback_swaps(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            install.install_release(source, root / "releases", root / "bin", "v1")
            (source / "operator.py").write_text("TOKEN = 'two'\n")
            install.install_release(source, root / "releases", root / "bin", "v2")
            self.assertEqual(os.readlink(root / "releases/current"), "v2")
            self.assertEqual(os.readlink(root / "releases/previous"), "v1")
            result = install.rollback_release(root / "releases")
            self.assertEqual(os.readlink(root / "releases/current"), "v1")
            self.assertEqual(os.readlink(root / "releases/previous"), "v2")
            self.assertEqual(Path(result["current"]), (root / "releases/v1").resolve())

    def test_reusing_version_with_changed_content_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            install.install_release(source, root / "releases", root / "bin", "v1")
            (source / "operator.py").write_text("TOKEN = 'different'\n")
            with self.assertRaisesRegex(RuntimeError, "different content"):
                install.install_release(source, root / "releases", root / "bin", "v1")
            self.assertEqual((root / "releases/v1/scripts/hcom_grok_seat/operator.py").read_text(), "TOKEN = 'one'\n")

    def test_reserved_release_link_names_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            for name in ("current", "previous"):
                with self.subTest(name=name), self.assertRaisesRegex(ValueError, "reserved"):
                    install.install_release(source, root / "releases", root / "bin", name)

    def test_unmanaged_existing_launcher_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            (root / "bin").mkdir()
            launcher = root / "bin/hcom-grok"
            launcher.write_text("#!/bin/sh\necho unrelated\n")
            with self.assertRaisesRegex(RuntimeError, "unmanaged command"):
                install.install_release(source, root / "releases", root / "bin", "v1")
            self.assertEqual(launcher.read_text(), "#!/bin/sh\necho unrelated\n")
            self.assertFalse((root / "releases/current").exists())

    def test_existing_bin_directory_permissions_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            bin_root = root / "bin"
            bin_root.mkdir(mode=0o755)
            os.chmod(bin_root, 0o755)
            install.install_release(source, root / "releases", bin_root, "v1")
            self.assertEqual(bin_root.stat().st_mode & 0o777, 0o755)

    def test_rollback_without_previous_release_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = _source(root, "TOKEN = 'one'\n")
            install.install_release(source, root / "releases", root / "bin", "v1")
            with self.assertRaisesRegex(RuntimeError, "no previous"):
                install.rollback_release(root / "releases")


if __name__ == "__main__":
    unittest.main()
