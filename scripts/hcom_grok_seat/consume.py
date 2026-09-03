"""Sqlite unread -> ACP session/prompt. Never hcom listen."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable


CLIENT = "hcom-grok-bridge"


def commit_cursor(path: Path, last_id: int) -> None:
    """Atomic durable write of the consume cursor. Call before any drain/sleep."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w") as fh:
        fh.write(str(int(last_id)) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    try:
        dfd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def load_cursor(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text().strip() or "0")
    except ValueError:
        return None


@dataclass(frozen=True)
class Inbound:
    event_id: int
    sender: str
    text: str
    raw: str
    intent: str
    reply_to: str | None
    thread: str | None


def prompt_id(name: str, event_id: int, body: str) -> str:
    sha = hashlib.sha256(body.encode()).hexdigest()[:12]
    return f"hcom:gseat:{name}:{event_id}:{sha}"


def unread_messages(db_path: Path, last_id: int) -> list[tuple[int, str, str]]:
    if not db_path.exists():
        return []
    con = sqlite3.connect(str(db_path))
    try:
        cols = [r[1] for r in con.execute("PRAGMA table_info(events)").fetchall()]
        body_col = "data" if "data" in cols else ("payload" if "payload" in cols else None)
        if body_col is None:
            return []
        q = f"SELECT id, type, {body_col} FROM events WHERE id > ? ORDER BY id"
        rows = con.execute(q, (last_id,)).fetchall()
    except sqlite3.Error:
        return []
    finally:
        con.close()
    out: list[tuple[int, str, str]] = []
    for eid, typ, payload in rows:
        text = payload if isinstance(payload, str) else ""
        out.append((int(eid), str(typ or ""), text))
    return out


def parse_message(payload: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return obj if isinstance(obj, dict) else None


def inbound_for_instance(
    rows: Iterable[tuple[int, str, str]],
    name: str,
    seen: set[int],
) -> list[Inbound]:
    found: list[Inbound] = []
    for eid, typ, payload in rows:
        if eid in seen:
            continue
        if typ != "message":
            continue
        obj = parse_message(payload)
        if obj is None:
            continue
        sender = str(obj.get("from") or "")
        if sender == name:
            continue
        intent = str(obj.get("intent") or "inform")
        if intent == "ack":
            continue
        mentions = obj.get("mentions") or []
        delivered = obj.get("delivered_to") or []
        if name not in mentions and name not in delivered:
            continue
        text = obj.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        reply_to = obj.get("reply_to")
        if reply_to is not None:
            reply_to = str(reply_to)
        thread = obj.get("thread")
        if thread is not None:
            thread = str(thread)
        found.append(
            Inbound(
                event_id=eid,
                sender=sender,
                text=text,
                raw=payload,
                intent=intent,
                reply_to=reply_to,
                thread=thread,
            )
        )
    return found


def format_letter(item: Inbound, name: str) -> str:
    extra = ""
    if item.intent != "inform":
        extra += f" --reply-to {item.event_id}"
    if item.thread:
        extra += f" --thread {item.thread}"
    if item.intent == "inform":
        reply_rule = (
            "This is inform. Reply only if useful. If you reply, use --intent inform."
        )
        send_intent = "inform"
    else:
        reply_rule = "This is request. You must reply once with --intent ack."
        send_intent = "ack"
    return (
        "[HCOM]\n"
        f"From: {item.sender}\n"
        f"To: {name}\n"
        f"Event: {item.event_id}\n"
        f"Intent: {item.intent}\n"
        f"Reply-to: {item.reply_to or ''}\n"
        f"Thread: {item.thread or ''}\n"
        f"Body:\n{item.text}\n"
        "\n"
        "This is an HCOM letter. Do not run hcom listen. Do not wait.\n"
        f"{reply_rule}\n"
        "Reply with one shell command:\n"
        f"  hcom send --name {name} --intent {send_intent}{extra} @{item.sender} -- <short answer>\n"
        "If the body asks for an exact reply token, that token is the entire send body.\n"
        "Incoming ack letters are not shown; never invent a reply to an ack.\n"
        "Then stop.\n"
    )


def session_prompt_params(session_id: str, item: Inbound, name: str) -> dict[str, Any]:
    body = format_letter(item, name)
    return {
        "sessionId": session_id,
        "prompt": [{"type": "text", "text": body}],
        "_meta": {
            "promptId": prompt_id(name, item.event_id, body),
            "sendNow": False,
            "clientIdentifier": CLIENT,
        },
    }


RpcFn = Callable[..., Any]


def _prompt_ok(res: Any) -> bool:
    return isinstance(res, dict) and "error" not in res


def consume_once(
    db_path: Path,
    last_id: int,
    name: str,
    session_id: str,
    acp_rpc: RpcFn,
    seen: set[int],
) -> tuple[int, list[dict[str, Any]]]:
    """Push unread inbound mail as session/prompt. Returns (new_last_id, results).

    Cursor only advances past an inbound letter after a successful prompt.
    A failed prompt leaves that event id for retry. Non-inbound rows still
    advance so old news is flushed.
    """
    rows = unread_messages(db_path, last_id)
    new_last = last_id
    results: list[dict[str, Any]] = []
    for eid, typ, payload in rows:
        items = inbound_for_instance([(eid, typ, payload)], name, seen)
        if not items:
            new_last = max(new_last, eid)
            continue
        item = items[0]
        params = session_prompt_params(session_id, item, name)
        res = acp_rpc("session/prompt", params, wait=60)
        results.append({"event_id": item.event_id, "params": params, "result": res})
        if not _prompt_ok(res):
            break
        seen.add(item.event_id)
        new_last = max(new_last, eid)
    return new_last, results
