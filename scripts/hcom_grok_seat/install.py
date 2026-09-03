#!/usr/bin/env python3
"""Versioned, reversible local installer for ``hcom-grok``."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Sequence


LAUNCHER_MARKER = "# managed-by: hcom-grok installer-v1"
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
RESERVED_RELEASES = {"current", "previous"}


def _private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def _package_files(package_dir: Path) -> list[Path]:
    files = sorted(path for path in package_dir.glob("*.py") if path.is_file())
    if not files or not (package_dir / "operator.py").is_file():
        raise RuntimeError(f"not an HCOM Grok package directory: {package_dir}")
    return files


def _tree_digest(package_dir: Path, files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _installed_digest(release: Path, expected_files: list[str]) -> str:
    package = release / "scripts" / "hcom_grok_seat"
    actual_files = sorted(path.name for path in package.glob("*.py") if path.is_file())
    if actual_files != sorted(expected_files):
        raise RuntimeError(f"installed release file list does not match manifest: {release}")
    return _tree_digest(package, [package / name for name in sorted(expected_files)])


def _write_json(path: Path, value: dict[str, Any]) -> None:
    old_umask = os.umask(0o077)
    try:
        path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.chmod(path, 0o600)
    finally:
        os.umask(old_umask)


def _atomic_symlink(root: Path, name: str, target: str) -> None:
    temporary = root / f".{name}.{os.getpid()}.{time.time_ns()}"
    try:
        temporary.symlink_to(target)
        os.replace(temporary, root / name)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _link_target(root: Path, name: str) -> str | None:
    path = root / name
    if not path.is_symlink():
        if path.exists():
            raise RuntimeError(f"refusing to replace non-symlink path: {path}")
        return None
    target = os.readlink(path)
    resolved = (root / target).resolve() if not Path(target).is_absolute() else Path(target).resolve()
    try:
        resolved.relative_to(root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"release link escapes release root: {path} -> {target}") from exc
    if not resolved.is_dir():
        raise RuntimeError(f"release link target does not exist: {path} -> {target}")
    return target


def _launcher_text(release_root: Path) -> str:
    quoted_root = shlex.quote(str(release_root))
    quoted_python = shlex.quote(str(Path(sys.executable).resolve()))
    return (
        "#!/bin/sh\n"
        f"{LAUNCHER_MARKER}\n"
        f"release_root={quoted_root}\n"
        'PYTHONPATH="$release_root/current${PYTHONPATH:+:$PYTHONPATH}" '
        f"exec {quoted_python} -m scripts.hcom_grok_seat.operator \"$@\"\n"
    )


def _prepare_bin_root(bin_root: Path) -> None:
    if bin_root.exists():
        if not bin_root.is_dir():
            raise RuntimeError(f"command directory is not a directory: {bin_root}")
        return
    bin_root.mkdir(mode=0o700, parents=True)


def _preflight_launcher(bin_root: Path) -> Path:
    _prepare_bin_root(bin_root)
    launcher = bin_root / "hcom-grok"
    if launcher.exists() or launcher.is_symlink():
        try:
            existing = launcher.read_text()
        except OSError as exc:
            raise RuntimeError(f"cannot inspect existing launcher: {launcher}") from exc
        if LAUNCHER_MARKER not in existing:
            raise RuntimeError(f"refusing to replace unmanaged command: {launcher}")
    return launcher


def _install_launcher(bin_root: Path, release_root: Path) -> Path:
    launcher = _preflight_launcher(bin_root)
    temporary = bin_root / f".hcom-grok.{os.getpid()}.tmp"
    old_umask = os.umask(0o077)
    try:
        temporary.write_text(_launcher_text(release_root))
        os.chmod(temporary, 0o700)
        os.replace(temporary, launcher)
    finally:
        os.umask(old_umask)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return launcher


def install_release(
    package_dir: Path,
    release_root: Path,
    bin_root: Path,
    version: str | None = None,
) -> dict[str, Any]:
    package_dir = package_dir.expanduser().resolve()
    release_root = release_root.expanduser().resolve()
    bin_root = bin_root.expanduser().resolve()
    files = _package_files(package_dir)
    digest = _tree_digest(package_dir, files)
    release_name = version or f"release-{digest[:16]}"
    if not VERSION_RE.fullmatch(release_name):
        raise ValueError("version must contain only letters, numbers, dot, underscore, or hyphen")
    if release_name in RESERVED_RELEASES:
        raise ValueError(f"version name is reserved: {release_name}")
    _preflight_launcher(bin_root)
    _private_dir(release_root)
    destination = release_root / release_name
    if destination.exists():
        if destination.is_symlink() or not destination.is_dir():
            raise RuntimeError(f"release destination is not a private directory: {destination}")
        manifest = json.loads((destination / "manifest.json").read_text())
        if manifest.get("sha256") != digest:
            raise RuntimeError(f"release already exists with different content: {destination}")
        if _installed_digest(destination, list(manifest.get("files") or [])) != digest:
            raise RuntimeError(f"installed release content does not match its manifest: {destination}")
    else:
        temporary = Path(tempfile.mkdtemp(prefix=f".{release_name}.", dir=release_root))
        try:
            scripts_dir = temporary / "scripts"
            installed_package = scripts_dir / "hcom_grok_seat"
            _private_dir(scripts_dir)
            _private_dir(installed_package)
            (scripts_dir / "__init__.py").write_text("\"\"\"Installed hcom-grok scripts package.\"\"\"\n")
            os.chmod(scripts_dir / "__init__.py", 0o600)
            for source in files:
                target = installed_package / source.name
                shutil.copyfile(source, target)
                os.chmod(target, 0o600)
            manifest = {
                "format": 1,
                "release": release_name,
                "sha256": digest,
                "files": [path.name for path in files],
                "installed_ns": time.time_ns(),
            }
            _write_json(temporary / "manifest.json", manifest)
            if _installed_digest(temporary, manifest["files"]) != digest:
                raise RuntimeError("source package changed while the release was copied")
            os.replace(temporary, destination)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)

    old_current = _link_target(release_root, "current")
    if old_current and old_current != release_name:
        _atomic_symlink(release_root, "previous", old_current)
    _atomic_symlink(release_root, "current", release_name)
    launcher = _install_launcher(bin_root, release_root)
    return {
        "ok": True,
        "release": str(destination),
        "current": str(release_root / "current"),
        "previous": old_current,
        "launcher": str(launcher),
        "sha256": digest,
    }


def rollback_release(release_root: Path) -> dict[str, Any]:
    release_root = release_root.expanduser().resolve()
    _private_dir(release_root)
    current = _link_target(release_root, "current")
    previous = _link_target(release_root, "previous")
    if current is None:
        raise RuntimeError("no current HCOM Grok release")
    if previous is None:
        raise RuntimeError("no previous HCOM Grok release to roll back to")
    _atomic_symlink(release_root, "current", previous)
    _atomic_symlink(release_root, "previous", current)
    return {
        "ok": True,
        "current": str((release_root / previous).resolve()),
        "previous": str((release_root / current).resolve()),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Install the local HCOM Grok operator")
    parser.add_argument("--package-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--bin-root", type=Path, required=True)
    parser.add_argument("--version")
    parser.add_argument("--rollback", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.rollback:
            result = rollback_release(args.release_root)
        else:
            result = install_release(args.package_dir, args.release_root, args.bin_root, args.version)
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"hcom-grok install: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
