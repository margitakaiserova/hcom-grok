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

Status reports `NEW` or `RESUMED`. It reports `busy` while Grok is handling an HCOM turn and `running` while the seat is ready.

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
- A fresh launch allocates a separate official HCOM identity instead of replacing an active seat.
- Continue fails clearly when the saved project or Grok session folder is missing.
- The HCOM cursor is preserved across Grok conversation and process restarts.
- Installation is versioned and supports rollback to the previous release.
- The adapter validates the HCOM database schema before reading it.
- Exiting the Grok TUI, pressing Control+C, or closing its terminal window shuts down the owned bridge and publishes the normal official HCOM stopped lifecycle event.

The explicit `stop` command is retained for recovery and background operation. Normal interactive use does not require it.

This adapter intentionally does not attempt exact recovery from a hard crash during partially completed tool work. It also does not run parallel turns inside one Grok seat. Parallel work uses separate seats.

## Adapter boundary

Grok is not a built-in HCOM tool. The adapter therefore calls official `hcom start` and `hcom stop`, reads the schema-guarded local HCOM event database, updates local seat heartbeat records and carries messages through Grok ACP.

This is a compatibility adapter, not a second message bus. Its names, room boundaries and stop lifecycle now come directly from HCOM. An official HCOM integration could reuse the proven Grok launcher, fresh and resume rules, ACP delivery, reply correlation and tests, then replace the direct database boundary with HCOM's native tool adapter interface.

## Room launchers

Local room wrappers set the project and `HCOM_DIR`, then pass every argument through to this command:

```text
hcom-grok-date    Date Calculations room
hcom-grok-dna     Atlas DNA Maths room
```

These wrappers do not share seat registries. Existing room launchers are left unchanged.

## Local verification

The local suite does not launch or contact Grok:

```bash
python3 -m unittest discover -s scripts/hcom_grok_seat -t . -p 'test_*.py'
```

A live benchmark is a separate operator-approved step. Its normal path is one ping, one small file task, one revision and one resume check. It is not an exhaustive crash or soak test.
