# HCOM Grok Adapter

This adapter makes each visible Grok Build conversation behave like a normal HCOM seat. HCOM remains the mailbox, identity system and reply router. A small Python supervisor owns each Grok leader, visible TUI and ACP sidecar used to deliver HCOM messages into that Grok session.

## Everyday commands

```text
hcom-grok                 Start a fresh, automatically named Grok seat
hcom-grok -c              Continue the latest seat in this HCOM room
hcom-grok -c mugi         Continue the named seat
hcom-grok resume [seat]   Clear equivalent of -c with an optional name
hcom-grok list            List Grok seats in this HCOM room
hcom-grok status [seat]   Show mode, session, project, seat and activity
hcom-grok restart [seat]  Restart one process and retain its conversation
hcom-grok stop [seat]     Recovery stop for the latest or named seat
hcom-grok doctor [seat]   Check binaries, state, HCOM and the installed release
hcom-grok logs [seat]     Show bridge logs
```

The old `hcom-grok start` spelling remains as a deprecated continue alias for compatibility. It prints a warning and does not appear in normal help.

Every bare launch asks official HCOM for a collision-safe name. Running it in ten terminals creates ten individually addressable seats. It never replaces an existing seat.

## Conversation behavior

- Fresh launch creates a new HCOM seat, a new Grok session ID and uses the current directory.
- Continue and restart retain the selected seat, its saved Grok session ID and its original directory.
- Starting fresh does not erase old Grok session folders or reset another seat's HCOM mail cursor.
- An HCOM thread groups related mail. It does not create or erase Grok conversation memory.
- Each seat processes one Grok turn at a time. Separate Grok seats can work independently.
- Seat registries are isolated by `HCOM_DIR`, so the same short name cannot cross between Date, DNA or another room.
- `/new`, `/clear`, and a same-pane `/resume` move future HCOM delivery to the
  conversation selected by that seat's exact visible TUI process. A turn already
  in progress completes in the conversation where it began.
- Grok 1.0.13 exposes visibility identity, not command provenance. A same-PID,
  same-project fork selected in that pane therefore follows the same visible-session
  rule. A foreign PID, project/worktree, `GROK_HOME`, ambiguous record, or failed
  sidecar attach is blocked instead of guessed.
- Changing Grok conversations does not clear HCOM threads, pending replies, or the
  seat's mailbox cursor.

Status reports `NEW` or `RESUMED`. It reports `busy` while Grok is handling an
HCOM turn and `running` while the seat is ready. Status and inspect output also
expose `bound_session_id`, `visible_session_id`, `session_alignment`, binding
generation, `focus_source`, pager sample/trigger health, the diagnostic registry
session, and a bounded degraded reason. Doctor fails a known running divergence
or a running seat without a validated pager-focus sample.

## Message flow

```text
sender uses hcom send
        |
        v
HCOM records and routes the message
        |
        v
adapter injects one ACP session prompt
        |
        v
Grok completes the turn
        |
        v
adapter returns the final reply through HCOM
        |
        v
the sender's normal HCOM hook wakes it
```

The sender does not need to poll Grok. It sends the request, records the thread or reply reference, and ends its active turn. Normal HCOM delivery wakes the sender when Grok replies.

Request replies are automatic. Grok should answer normally and must not send the same reply manually. When a request explicitly asks Grok to initiate a separate message, Grok may use `hcom send` for that additional message and still return a normal concise completion response.

Inform messages are absorbed without an automatic reply. Acknowledgement messages do not become Grok model turns.

## Safety and recovery

- Runtime directories are private and process ownership is verified before signals are sent.
- A fresh launch creates its Grok conversation through ACP first, then starts the
  sole visible TUI with `--resume` for that returned ID. This is the Grok 1.0.13
  path that arms the supported pager-status command; it avoids a direct
  `--session-id` launch whose first pager never schedules that command.
- A fresh launch allocates a separate official HCOM identity instead of replacing an active seat.
- Continue fails clearly when the saved project or Grok session folder is missing.
- The HCOM cursor is preserved across Grok conversation and process restarts.
- Visible-session identity comes from Grok's structured pager status payload,
  matched to a kernel-identified direct child of the exact owned TUI,
  canonical project, contained session directory, and allowlisted identity
  fields in `summary.json`. The adapter never scrapes terminal text or reads
  title, recap, message, or summary content.
- The status helper receives only a private Unix-socket path. The supervisor
  verifies the helper's kernel PID/UID, stable process identity, Python binary,
  exact mode-0700 executable-shim argument, and direct TUI parent before accepting bytes. Only
  the supervisor holds the key used to authenticate the mode-0600 focus record.
- `active_sessions.json` is diagnostic only. Grok 1.0.13 leaves it stale after
  some same-pane `/resume` selections, so it cannot safely choose delivery.
- Before each model-bound letter the adapter waits at most three seconds for a
  pager sample captured after that delivery gate. A refresh proves liveness only
  for the last matching state-event identity; it cannot select a conversation.
  The identity then remains stable for 350 ms and is checked again while focus
  publication is serialized with the ACP request-frame admission. Missing,
  stale, replayed, foreign, or rapidly changing evidence holds the letter and
  leaves its cursor unadvanced.
- A bridge-only failure holds undelivered model mail while leaving the visible TUI
  and leader available for local use. It does not advance past the held event.
- Installation is versioned and supports rollback to the previous release.
- The adapter validates the HCOM database schema before reading it.
- Exiting the Grok TUI, pressing Control+C, or closing its terminal window shuts down the owned bridge and publishes the normal official HCOM stopped lifecycle event.

The explicit `stop` command is retained for recovery and background operation. Normal interactive use does not require it.

After a hard supervisor crash, restart reaps only a process tree and sockets that
match the recorded UID, non-reusable process-start identities, roles, arguments,
and inode identities. Any unproven survivor is left untouched and relaunch is
blocked. This does not attempt to reconstruct partially completed model tool
work. The adapter also does not run parallel turns inside one Grok seat; parallel
work uses separate seats.

## Adapter boundary

Grok is not a built-in HCOM tool. The adapter therefore calls official `hcom start` and `hcom stop`, reads the schema-guarded local HCOM event database, updates local seat heartbeat records and carries messages through Grok ACP.

This is a compatibility adapter, not a second message bus. Its names, room boundaries and stop lifecycle now come directly from HCOM. An official HCOM integration could reuse the proven Grok launcher, fresh and resume rules, ACP delivery, reply correlation and tests, then replace the direct database boundary with HCOM's native tool adapter interface.

When Plugins Manager launches this adapter, the manager owns the private seat home
and `GROK_HOME`, the project `HCOM_DIR`, and the Ghostty pane. Conversation IDs,
visible-session validation, sidecar replacement, and cursor continuity remain
adapter-private. The manager supplies an isolated-home admission marker; the
adapter still verifies that `HOME` is not the login home, `GROK_HOME` is exactly
`HOME/.grok`, the state root is contained there, and one supervisor owns it.
Plugins Manager creates/tightens the two Grok seat directories to mode 0700 at
materialization/launch so existing manager seats meet that admission boundary.

After the leader socket is ready and immediately before the TUI starts, the adapter
reversibly appends its exact marker-owned `[ui.status_line]` block to the isolated
seat's `$GROK_HOME/config.toml`. It creates that file only when absent. For an
existing eligible file, a journaled compare/detach/publish transaction preserves
all unrelated bytes, mode, ACLs, and extended attributes; an interrupted
transaction is recovered before the next stage or cleanup. The adapter refuses a
foreign status line (including `type = "disabled"`), an unowned marker, an external
config overlay, or a signed/managed-sync seat policy. It never uses or changes
`managed_config.toml`, requirements, authentication, or history.

The configured command is exactly one absolute path: an adapter-owned, mode-0700
executable shim. Its shebang selects the supervisor interpreter and its private
contents carry the socket path; the configured command, child environment, and
logs expose no authentication key or writable record path. After the sidecar, TUI,
and leader have stopped, cleanup recovers any journal and removes only the exact
marker block, shim, status record, and ownership claim. It detaches artifacts to
random names, validates bytes/inode/mode, and retains racing replacements rather
than overwriting or deleting them.

Shared or otherwise unproven homes keep the local Grok TUI and leader usable but
hold model-bound HCOM mail. This is deliberate: without the pager probe, a
same-pane `/resume` is invisible to the adapter and could route a letter into the
hidden prior conversation. Bare launch still means fresh; `-c`/`resume` still
means resume.

## Room launchers

Local room wrappers set the project and `HCOM_DIR`, then pass every argument through to this command:

```text
hcom-grok-date    Date Calculations room
hcom-grok-dna     Atlas DNA Maths room
```

These wrappers do not share seat registries. Existing wrappers that use the login
`HOME/.grok` do not qualify for automatic visible-session tracking; use a
seat-private home contract (as Plugins Manager does) before enabling model-bound
delivery.

## Local verification

The local suite does not launch or contact Grok:

```bash
python3 -m unittest discover -s scripts/hcom_grok_seat -t . -p 'test_*.py'
```

A live benchmark is a separate operator-approved step. Its normal path is one ping, one small file task, one revision and one resume check. It is not an exhaustive crash or soak test.
