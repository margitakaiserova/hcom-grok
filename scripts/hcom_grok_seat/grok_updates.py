"""Reconcile a prompt against Grok's disk journal and buffered live updates."""
from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Any


DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024
DEFAULT_MAX_LINE_BYTES = 2 * 1024 * 1024
DEFAULT_MAX_LIVE_EVENTS = 4096
DEFAULT_MAX_LIVE_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_ASSISTANT_BYTES = 16 * 1024 * 1024
_UPDATE_METHODS = {
    "session/update",
    "_x.ai/session/update",
    "_x.ai/session_notification",
}


class GrokUpdateError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromptEvidence:
    prompt_id: str
    admitted: bool
    completed: bool
    assistant_text: str
    stop_reason: str | None
    event_ids: tuple[str, ...]
    partial_tail: bool
    admission_count: int
    completion_count: int
    disk_bytes: int
    live_event_count: int

    @property
    def not_found(self) -> bool:
        """True only for a complete, reconciled disk and live observation."""

        return (
            not self.admitted
            and not self.completed
            and not self.event_ids
            and not self.partial_tail
        )


@dataclass(frozen=True)
class _InputRecord:
    obj: dict[str, Any]
    source: str
    is_live: bool


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _iter_disk_records(
    path: Path,
    *,
    max_file_bytes: int,
    max_line_bytes: int,
) -> tuple[Iterator[_InputRecord], os.stat_result]:
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise GrokUpdateError(f"Grok updates journal is missing: {path}") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise GrokUpdateError("Grok updates journal is not a regular file")
    if before.st_size > max_file_bytes:
        raise GrokUpdateError(
            f"Grok updates journal exceeds {max_file_bytes} bytes"
        )

    def records() -> Iterator[_InputRecord]:
        total = 0
        try:
            handle = path.open("rb")
        except OSError as exc:
            raise GrokUpdateError(f"cannot open Grok updates journal: {exc}") from exc
        with handle:
            current = os.fstat(handle.fileno())
            if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
                raise GrokUpdateError("Grok updates journal changed identity during open")
            line_number = 0
            while total < before.st_size:
                remaining = before.st_size - total
                raw = handle.readline(min(max_line_bytes + 1, remaining))
                if not raw:
                    raise GrokUpdateError("Grok updates journal was truncated during scan")
                line_number += 1
                total += len(raw)
                if len(raw) > max_line_bytes:
                    raise GrokUpdateError(
                        f"Grok update line {line_number} exceeds {max_line_bytes} bytes"
                    )
                if not raw.endswith(b"\n"):
                    if total < before.st_size:
                        raise GrokUpdateError(
                            f"Grok update line {line_number} exceeds {max_line_bytes} bytes"
                        )
                    yield _InputRecord({}, f"disk line {line_number} partial", False)
                    break
                if not raw.strip():
                    continue
                try:
                    obj = json.loads(
                        raw.decode("utf-8", "strict"),
                        parse_constant=_reject_json_constant,
                    )
                except (UnicodeDecodeError, ValueError) as exc:
                    raise GrokUpdateError(
                        f"malformed complete Grok update line {line_number}: {exc}"
                    ) from exc
                if not isinstance(obj, dict):
                    raise GrokUpdateError(
                        f"Grok update line {line_number} is not an object"
                    )
                yield _InputRecord(obj, f"disk line {line_number}", False)

    return records(), before


def _replay_marker(obj: dict[str, Any], source: str) -> bool:
    markers: list[bool] = []
    containers: list[Any] = [obj.get("_meta")]
    params = obj.get("params")
    if isinstance(params, dict):
        containers.append(params.get("_meta"))
        update = params.get("update")
        if isinstance(update, dict):
            containers.append(update.get("_meta"))
    for container in containers:
        if not isinstance(container, dict) or "isReplay" not in container:
            continue
        value = container["isReplay"]
        if type(value) is not bool:
            raise GrokUpdateError(f"{source} has a non-boolean replay marker")
        markers.append(value)
    if markers and any(value != markers[0] for value in markers[1:]):
        raise GrokUpdateError(f"{source} has conflicting replay markers")
    return markers[0] if markers else False


def _correlated_prompt_id(
    params: dict[str, Any], update: dict[str, Any], source: str
) -> str | None:
    candidates: list[tuple[str, str]] = []
    containers = (
        (params.get("_meta"), "params._meta", ("promptId",)),
        (update, "params.update", ("prompt_id", "promptId")),
        (update.get("_meta"), "params.update._meta", ("promptId",)),
    )
    for container, label, keys in containers:
        if not isinstance(container, dict):
            continue
        for key in keys:
            if key not in container:
                continue
            value = container[key]
            if not isinstance(value, str) or not value:
                raise GrokUpdateError(
                    f"{source} has an invalid prompt ID at {label}.{key}"
                )
            candidates.append((f"{label}.{key}", value))
    values = {value for _, value in candidates}
    if len(values) > 1:
        raise GrokUpdateError(
            f"{source} has conflicting prompt IDs: {sorted(values)!r}"
        )
    return next(iter(values)) if values else None


def _semantic_fingerprint(
    session_id: str, prompt_id: str | None, update: dict[str, Any]
) -> str:
    value = {"sessionId": session_id, "promptId": prompt_id, "update": update}
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8", "strict")
        return hashlib.sha256(encoded).hexdigest()
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise GrokUpdateError("Grok update contains a non-JSON value") from exc


def scan_prompt_updates(
    path: Path,
    session_id: str,
    prompt_id: str,
    *,
    buffered_live: Iterable[dict[str, Any]],
    max_file_bytes: int = DEFAULT_MAX_FILE_BYTES,
    max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
    max_live_events: int = DEFAULT_MAX_LIVE_EVENTS,
    max_live_bytes: int = DEFAULT_MAX_LIVE_BYTES,
    max_assistant_bytes: int = DEFAULT_MAX_ASSISTANT_BYTES,
) -> PromptEvidence:
    """Reconcile exact prompt evidence from disk and an ordered live buffer.

    ``buffered_live`` is mandatory. This prevents recovery callers from making
    a negative admission decision from the disk snapshot alone.
    """

    if not isinstance(session_id, str) or not session_id:
        raise ValueError("session_id must be a non-empty string")
    if not isinstance(prompt_id, str) or not prompt_id:
        raise ValueError("prompt_id must be a non-empty string")
    for label, value in (
        ("max_file_bytes", max_file_bytes),
        ("max_line_bytes", max_line_bytes),
        ("max_live_events", max_live_events),
        ("max_live_bytes", max_live_bytes),
        ("max_assistant_bytes", max_assistant_bytes),
    ):
        if type(value) is not int or value < 1:
            raise ValueError(f"{label} must be a positive integer")

    disk_records, before = _iter_disk_records(
        path,
        max_file_bytes=max_file_bytes,
        max_line_bytes=max_line_bytes,
    )
    live_records: list[_InputRecord] = []
    live_bytes = 0
    for index, obj in enumerate(buffered_live, 1):
        if index > max_live_events:
            raise GrokUpdateError(
                f"buffered live updates exceed {max_live_events} events"
            )
        if not isinstance(obj, dict):
            raise GrokUpdateError(f"live update {index} is not an object")
        try:
            live_bytes += len(
                json.dumps(
                    obj,
                    ensure_ascii=False,
                    sort_keys=True,
                    allow_nan=False,
                    separators=(",", ":"),
                ).encode("utf-8", "strict")
            )
        except (TypeError, ValueError, UnicodeEncodeError) as exc:
            raise GrokUpdateError(
                f"live update {index} is not exact finite JSON"
            ) from exc
        if live_bytes > max_live_bytes:
            raise GrokUpdateError(
                f"buffered live updates exceed {max_live_bytes} bytes"
            )
        live_records.append(_InputRecord(obj, f"live update {index}", True))

    admitted = False
    completed = False
    admission_count = 0
    completion_count = 0
    stop_reason: str | None = None
    chunks: list[str] = []
    assistant_bytes = 0
    seen_events: dict[str, str] = {}
    matched_events: list[str] = []
    partial_tail = False
    live_event_count = 0

    for record in chain(disk_records, live_records):
        obj = record.obj
        if not obj and record.source.endswith(" partial"):
            partial_tail = True
            continue
        if record.is_live:
            if obj.get("jsonrpc") != "2.0" or "id" in obj:
                raise GrokUpdateError(f"{record.source} is not an ACP notification")
        else:
            if set(obj) != {"method", "params", "timestamp"}:
                raise GrokUpdateError(
                    f"{record.source} is not an exact persisted Grok update"
                )
            timestamp = obj.get("timestamp")
            if (
                not isinstance(timestamp, (int, float))
                or isinstance(timestamp, bool)
                or not math.isfinite(timestamp)
                or timestamp < 0
            ):
                raise GrokUpdateError(f"{record.source} has an invalid timestamp")
        method = obj.get("method")
        if method not in _UPDATE_METHODS:
            raise GrokUpdateError(
                f"{record.source} has unsupported update method {method!r}"
            )
        params = obj.get("params")
        if not isinstance(params, dict):
            raise GrokUpdateError(f"{record.source} omitted params")
        observed_session = params.get("sessionId")
        if not isinstance(observed_session, str) or not observed_session:
            raise GrokUpdateError(f"{record.source} omitted exact session identity")
        if observed_session != session_id:
            raise GrokUpdateError(
                f"{record.source} belongs to session {observed_session!r}"
            )
        if _replay_marker(obj, record.source):
            raise GrokUpdateError(
                f"{record.source} is replay and crossed the live replay fence"
            )
        meta = params.get("_meta")
        if not isinstance(meta, dict):
            raise GrokUpdateError(f"{record.source} omitted event metadata")
        event_id = meta.get("eventId")
        if not isinstance(event_id, str) or not event_id:
            raise GrokUpdateError(f"{record.source} omitted an exact event ID")
        update = params.get("update")
        if not isinstance(update, dict):
            raise GrokUpdateError(f"{record.source} omitted update payload")
        observed_prompt = _correlated_prompt_id(params, update, record.source)
        fingerprint = _semantic_fingerprint(observed_session, observed_prompt, update)
        prior = seen_events.get(event_id)
        if prior is not None:
            if prior != fingerprint:
                raise GrokUpdateError(
                    f"event ID {event_id!r} has contradictory payloads"
                )
            continue
        seen_events[event_id] = fingerprint
        if record.is_live:
            live_event_count += 1
        if observed_prompt != prompt_id:
            continue

        matched_events.append(event_id)
        update_kind = update.get("sessionUpdate")
        if not isinstance(update_kind, str) or not update_kind:
            raise GrokUpdateError(f"{record.source} omitted sessionUpdate kind")
        if update_kind == "hook_execution" and update.get("event_name") == "user_prompt_submit":
            if update.get("prompt_id") != prompt_id:
                raise GrokUpdateError(
                    f"{record.source} admission lacks its exact prompt_id"
                )
            admission_count += 1
            if admission_count != 1 or admitted or completed:
                raise GrokUpdateError("prompt has duplicate or out-of-order admission")
            admitted = True
        elif update_kind == "agent_message_chunk":
            if not admitted or completed:
                raise GrokUpdateError("assistant chunk is outside the admitted prompt interval")
            content = update.get("content")
            if (
                not isinstance(content, dict)
                or content.get("type") != "text"
                or not isinstance(content.get("text"), str)
            ):
                raise GrokUpdateError("assistant message chunk is not exact text content")
            try:
                assistant_bytes += len(content["text"].encode("utf-8", "strict"))
            except UnicodeEncodeError as exc:
                raise GrokUpdateError(
                    "assistant message chunk contains invalid Unicode"
                ) from exc
            if assistant_bytes > max_assistant_bytes:
                raise GrokUpdateError(
                    f"assistant text exceeds {max_assistant_bytes} bytes"
                )
            chunks.append(content["text"])
        elif update_kind == "turn_completed":
            if update.get("prompt_id") != prompt_id:
                raise GrokUpdateError(
                    f"{record.source} completion lacks its exact prompt_id"
                )
            completion_count += 1
            if not admitted or completed or completion_count != 1:
                raise GrokUpdateError("prompt has duplicate or out-of-order completion")
            reason = update.get("stop_reason")
            if not isinstance(reason, str) or not reason:
                raise GrokUpdateError("prompt completion omitted stop_reason")
            completed = True
            stop_reason = reason
        elif not admitted or completed:
            raise GrokUpdateError(
                f"prompt-correlated {update_kind!r} is outside the admitted interval"
            )

    disk_bytes = before.st_size
    try:
        after = path.lstat()
    except FileNotFoundError as exc:
        raise GrokUpdateError("Grok updates journal disappeared during scan") from exc
    if (
        stat.S_ISLNK(after.st_mode)
        or not stat.S_ISREG(after.st_mode)
        or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
    ):
        raise GrokUpdateError("Grok updates journal changed identity during scan")
    if after.st_size < before.st_size:
        raise GrokUpdateError("Grok updates journal was truncated during scan")
    if after.st_size > max_file_bytes:
        raise GrokUpdateError(
            f"Grok updates journal exceeds {max_file_bytes} bytes"
        )
    return PromptEvidence(
        prompt_id=prompt_id,
        admitted=admitted,
        completed=completed,
        assistant_text="".join(chunks),
        stop_reason=stop_reason,
        event_ids=tuple(matched_events),
        partial_tail=partial_tail,
        admission_count=admission_count,
        completion_count=completion_count,
        disk_bytes=disk_bytes,
        live_event_count=live_event_count,
    )
