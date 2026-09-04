# Grok Visible-Session Rebinding Implementation Plan

**Status:** Implemented in the working trees on 2026-09-04; offline suites pass. The final disposable live release gate remains required before automatic rebinding ships.

**Goal:** Keep one HCOM Grok seat bound to the conversation actually shown in its owned Ghostty TUI whenever the human changes that pane's conversation, without losing or duplicating HCOM mail, restarting the visible TUI, weakening project/room isolation, or making Plugins Manager own Grok conversation IDs.

**Source specification:** Maneesh's 2026-09-04 request to determine whether the visibility defect belongs to the standalone HCOM Grok adapter or to Plugins Manager, preserve operation across isolated projects/HCOM rooms, and produce an implementation-ready plan audited by the participating AIs. The concrete reproduction is the live `grok-main` seat launched by Plugins Manager: the adapter/HCOM side remained bound to `21f9a09e-55cf-4cc8-a6cc-a0f51cb7982a`, while the same visible TUI process moved to `01a068d4-acb8-7c91-860a-dba807a3816e` after `/clear`.

**Delivery shape:** One adapter release plus one narrow Plugins Manager launch-environment marker. Detection without safe rebinding is not the intended finished product; fail-closed behavior is the fallback when a transition cannot be proven safe or the disposable live proof fails. Plugins Manager does not receive Grok session-management logic.

## Outcome in plain language

Plugins Manager opened one Ghostty pane and launched one `hcom-grok` process. `hcom-grok` then created a leader, a visible Grok TUI, and a hidden ACP sidecar. Initially, the TUI and sidecar shared one Grok conversation, so HCOM turns could paint in the pane. When `/clear` created a new Grok conversation, only the TUI followed it. The sidecar kept sending HCOM work to the old conversation, which remained alive but invisible.

The fix belongs in `hcom-grok` because it owns both Grok clients and the HCOM-to-Grok mapping. The adapter will observe which conversation its exact TUI process is showing, pause before taking another HCOM letter when that conversation changes, validate that the conversation belongs to the same seat home and project, reconnect only the hidden sidecar to it, and then update the saved and HCOM-visible session identity. If any fact is ambiguous, it will hold the mail and report a blocked state instead of silently talking off-screen.

Grok 1.0.13 does not publish reliable command provenance: the adapter cannot prove whether a same-PID switch came from `/clear`, `/new`, `/resume`, or a same-process `/fork --no-worktree`. The implementable contract is therefore visibility ownership, not slash-command classification. A pager-scoped status command supplies the selected `session_id` and cwd to a private supervisor Unix socket. The supervisor validates the helper's kernel PID/UID, stable process identity, exact Python/shim arguments, direct parent equal to its TUI, Grok version, project, seat home, contained session directory, and bounded `summary.json` identity before future HCOM delivery follows it.

### Implementation-discovery amendment (authoritative)

The original audit assumed `active_sessions.json` was the focus authority. A disposable Grok 1.0.13 gate disproved that assumption: `/clear` updated the registry, but a same-pane `/resume` changed the visible pager and the next local ACP prompt while the registry remained on the abandoned session indefinitely. `SessionStart` hooks also did not fire for that already-live resume. The supported `[ui.status_line] type="command"` payload did follow startup, `/clear`, and `/resume`, and every measured recorder process was a direct child of the exact TUI PID. Therefore:

- pager status is the sole focus authority;
- `active_sessions.json` is retained only as a diagnostic/corroborating registry and disagreement does not override pager focus;
- before either the leader or TUI starts, the recorder is configured by an exact marker-owned `[ui.status_line]` block reversibly injected into the isolated seat's `$GROK_HOME/config.toml`; the configured command contains exactly one absolute mode-0700 executable-shim path;
- an existing eligible `config.toml` is transformed through a journaled compare/detach/publish transaction that preserves unrelated bytes, mode, ACLs, and xattrs; recovery runs before another stage or cleanup, and cleanup after Grok's children stop removes only the exact owned block and artifacts;
- `managed_config.toml`, requirements, signed policy, auth, and history are never used or changed. Foreign status lines, an unowned marker, an external overlay, a signed seat policy, or managed-config synchronization fail closed;
- the private ingest-socket path appears only inside the exact owned shim; the supervisor-only record-authentication key and writable record path appear in neither configuration command, process arguments, nor child environment;
- the ingest socket lives in a real user-owned mode-0700 per-UID directory under `/tmp`, uses a short state-root-derived name, is chmod 0600, refuses every pre-existing socket/symlink/file collision, and is removed only when its recorded device/inode identity still matches;
- automatic focus tracking is enabled only for a proven seat-private Plugins Manager-style home: the marker is necessary but not sufficient, and the adapter independently checks the passwd home boundary, `GROK_HOME == HOME/.grok`, state-root containment, user ownership, and the exclusive supervisor claim;
- Plugins Manager creates or validates its Grok seat `HOME` and `HOME/.grok` as real current-user directories and tightens them to mode 0700 during materialization/launch; the adapter still performs its own admission checks;
- before each model-bound letter and each post-attach rebind boundary, the adapter waits at most three seconds for a sample captured after that delivery/transition gate. A one-second refresh is a liveness re-run of Grok's last state payload, not a new focus query; correctness relies on the live-proven state event emitted by focus changes. A supervisor focus lock then orders the final synchronous focus check with ACP request-frame admission without blocking status updates during the model turn.

## Proven repository and runtime findings

### The split is real and `/clear` caused it

- `scripts/hcom_grok_seat/supervisor.py:370-404` chooses one `self.session_id` at startup and never changes it.
- `scripts/hcom_grok_seat/supervisor.py:497-520` launches the visible TUI with that ID.
- `scripts/hcom_grok_seat/supervisor.py:558-598` launches a second, hidden ACP client and loads the same ID once.
- `scripts/hcom_grok_seat/supervisor.py:762-780` sends every HCOM prompt to that startup ID.
- `scripts/hcom_grok_seat/supervisor.py:621-652` republishes that same ID to HCOM, so `hcom list` confirms delivery identity but cannot prove what Ghostty is displaying.
- Grok 1.0.13 documents `/clear` as an alias of `/new` in `.grok/docs/user-guide/04-slash-commands.md:13-19` and `.grok/docs/user-guide/17-sessions.md:47-57`.
- The live TUI log first shows `session/new` with the adapter-supplied UUID and later shows `/clear` issuing `session/new` without an ID. Grok returned the second UUID to the same TUI process.
- The live seat's `active_sessions.json` maps TUI PID `26164` to the second UUID, while `session.json`, `run.json`, the ACP sidecar, and HCOM remain on the first UUID.

### Plugins Manager launches one seat; it does not create the split

- `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/adapters/grok.py:21-52` declares the fixed launcher `hcom-grok`, pins `GROK_HOME`, and owns only the auth bridge/runtime contract.
- `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/runtime.py:56-68` gives every `(projectId, seatId)` a private persistent home.
- `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/runtime.py:237-270` supplies that home, the project's `HCOM_DIR`, the stable seat name, and the real project path.
- `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/launch.py:80-187` performs one structured `exec`; it has no Grok session ID and does not create a second Grok process tree.
- `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/launch.py:212-230` correctly leaves the Grok command as `hcom-grok` and does not append HCOM's built-in `--go` flag.

### Related adapter isolation defect

`scripts/hcom_grok_seat/supervisor.py:124-130` derives the global Unix socket name from only `(seat, project)`. Two isolated homes/HCOM rooms that deliberately use the same real project and stable seat can therefore collide. The socket identity must derive from the adapter state root, which is unique for both manager-owned seats and adapter-managed room seats.

### Baseline before implementation

- The pre-change adapter suite passed 152 tests; the implemented working tree now passes 283 tests, including pager-focus, stale-registry resume, commit/admission races, hard-crash recovery, socket ownership, marker-config recovery, cursor-hold cases, and ACP-created initial-session fencing.
- The targeted Plugins Manager launch/environment/multi-client smoke tests pass, corroborating that its one-seat structured launch contract is intact.
- The Plugins Manager worktree already contains unrelated user changes. Implementation must record that baseline and merge the one documentation note without overwriting or attributing those changes to this work.

## Architecture

```text
Plugins Manager
  owns projectId + seatId + private HOME/GROK_HOME + HCOM_DIR + Ghostty launch
                                  |
                                  v
hcom-grok supervisor (one seat owner)
  owns TUI pid + leader socket + HCOM cursor + current Grok binding
          |                                      |
          v                                      v
visible Grok TUI                         hidden ACP sidecar
pager status: TUI pid -> S              session/new -> S on fresh launch;
                                         session/load(S) after rebinding;
                                         session/prompt(S)
          |                                      |
          +---------------- S -------------------+
                                  |
                                  v
                  HCOM instances/session binding = S
```

The enforced steady-state invariant in `BOUND` is:

```text
validated visible session for the owned TUI pid
  == supervisor bound session
  == ACP sidecar load/prompt session
  == session.json compatibility session_id
  == run.json session_id
  == HCOM instance/process/session binding
```

### Ownership boundary

| Concern | Owner | Decision |
|---|---|---|
| Project, seat ID, private runtime home, auth link, `HCOM_DIR`, Ghostty pane | Plugins Manager | Preserve current implementation. |
| Grok leader, TUI PID, sidecar, active conversation, rebind policy | `hcom-grok` | Implement here. |
| Mail routing and lifecycle records | HCOM | Adapter mirrors its validated binding through the existing HCOM boundary. |
| Terminal contents | Nobody | Do not scrape, classify, or persist Ghostty screen text. |

Plugins Manager must not read pager status or `active_sessions.json`, store a Grok session ID, calculate leader sockets, or attempt to repair a split. It sets `HCOM_GROK_ISOLATED_HOME=1` only on its Grok seat launch. `HCOM_GROK_SEAT` remains the stable manager-owned seat contract; all real isolation and focus checks remain adapter-side.

### Transition policy

The first release follows any conversation uniquely and currently selected by the same live, supervisor-owned TUI. That covers `/new` and `/clear`, and intentionally also covers an explicit `/resume` or a same-PID/same-project fork because Grok 1.0.13 exposes no trustworthy provenance field that distinguishes those cases. A foreign-PID dashboard/fork peer, different working directory/worktree, different `GROK_HOME`, missing session, stale process, or ambiguous PID mapping is never adoptable.

Classification uses structured metadata only:

- the pager helper connects only to the supervisor-owned Unix socket; the supervisor obtains its PID/UID from the kernel, requires a stable Python process with the exact owned shim arguments and direct parent equal to the live TUI, and then writes an HMAC-authenticated record with a memory-only key;
- the record's cwd, `summary.json.info.cwd`, and configured project agree after canonical `Path.expanduser().resolve()` normalization;
- `summary.json.info.id` equals the pager UUID and `summary.json.grok_home` canonically equals the configured `GROK_HOME`; `git_root_dir` is recorded for diagnostics only;
- the URL-encoded project directory and UUID session directory resolve beneath `<GROK_HOME>/sessions` without a symlink/path escape;
- `summary.json.created_at` parses as RFC3339, but it is identity/diagnostic data, not an invented command classifier;
- the status record is re-read after summary validation so a rapid S2-to-S3 change is transient rather than adoptable.

The reader consumes no title, recap, last-turn summary, session summary, message content, terminal output, or debug-log prose. It never uses newest-directory wins. The legacy active-session reader remains separately tested and exposed for diagnostics but never routes `/resume`. Unsupported schema/version, missing/stale/post-gate evidence, ownership drift, a changing read, or any ambiguous identity holds delivery.

### Runtime state machine

```text
STARTING
  -> establish initial TUI and sidecar on S
  -> validate the TUI mapping and ACP load result
  -> BOUND(S)

BOUND(S)
  -> HCOM letter: DELIVERING(S, event)
  -> validated visible change S2: REBIND_PENDING(S -> S2)
  -> ambiguous/foreign/missing mapping beyond grace period: DEGRADED(reason)

DELIVERING(S, event)
  -> complete on S and send its correlated reply
  -> never reroute or duplicate the in-flight turn
  -> re-observe before dequeuing another letter

REBIND_PENDING(S -> S2)
  -> dequeue no new model-bound mail; leave its cursor unadvanced
  -> revalidate PID, cwd, metadata, directory, and stable target
  -> REBINDING(S -> S2)

REBINDING(S -> S2)
  -> create a fresh hidden ACP sidecar and load S2
  -> validate returned session ID/cwd and drain load replay
  -> require a new post-attach pager sample and the same S2
  -> execute the crash-ordered binding commit; close the old sidecar
  -> BOUND(S2)
  -> any failure: discard candidate sidecar and DEGRADED(reason)

DEGRADED(reason)
  -> keep the human's TUI and leader alive
  -> do not send session/prompt and do not advance undelivered mail
  -> expose blocked reason through run state, status, doctor, logs, and HCOM status
  -> retry only transient observation failures; explicit restart remains recovery

STOPPING
  -> preserve the current normal owned-process cleanup
```

The adapter will respawn only the sidecar for a rebind. It will not restart the TUI or leader, and it will not depend on an unproven second `session/load` on an already-bound ACP connection. The preferred handoff keeps the old sidecar connected until the candidate has loaded; the disposable compatibility probe must prove that the leader accepts those two sidecars concurrently. If it does not, the defined fallback pauses delivery, closes the old sidecar first, and performs the same validated attach with a brief no-sidecar window. A later optimization may reuse one sidecar only after a separate compatibility proof.

## Compatibility and safety constraints

- Keep `session.json.session_id` as the current bound ID so the previous installed release can still resume after rollback.
- Treat the fsync-and-rename of `session.json` as the only durable binding commit point. `run.json`, in-memory state, and HCOM rows are repairable mirrors, never competing sources of truth.
- Preserve the HCOM cursor and any pending reply across a conversation change. `/clear` resets Grok context, not the mailbox.
- Never move an in-flight prompt. A reply is sent from the session that produced it.
- Never delete the previous Grok session directory.
- Never advance past an undelivered request/inform while the bridge is degraded.
- Keep the TUI and leader alive when only the ACP bridge fails.
- Resolve session files from `GROK_HOME`, not an assumed `~/.grok`, and reject paths outside it.
- Keep tests away from live user harness homes and never read/copy auth, history, credentials, or cache content. Metadata fixtures must be synthetic.
- Preserve launch and local-TUI behavior for direct `hcom-grok` and managed seats. Model-bound HCOM delivery is deliberately blocked when a seat-private focus probe cannot be proven; Plugins Manager seats receive the required marker and verified home shape.
- Preserve the 100-byte Unix socket limit by hashing the resolved state-root identity rather than embedding paths.
- Resolve `GROK_HOME` as the environment override when set, otherwise `$HOME/.grok`; never derive it from `Path.home()` after configuration has been captured.
- Version-gate both the pager payload and summary identity contract. Unknown Grok versions or required shapes fail closed.

## Task 1: Add bounded pager-focus and diagnostic registry contracts

**Purpose:** Turn Grok's active-session metadata into a small, testable, read-only adapter boundary instead of spreading filesystem guesses through the supervisor.

**Files:**

- Add `scripts/hcom_grok_seat/visible_session.py`.
- Add `scripts/hcom_grok_seat/test_visible_session.py`.
- Add `scripts/hcom_grok_seat/pager_status.py`.
- Add `scripts/hcom_grok_seat/test_pager_status.py`.
- Update `scripts/hcom_grok_seat/supervisor.py` configuration to carry resolved `grok_home`.
- Update `scripts/hcom_grok_seat/operator.py` configuration/resume checks to use the same resolved `grok_home`.

**Contract:**

- Add `Config.grok_home`, resolved once from `GROK_HOME` or `$HOME/.grok`, and replace every `Path.home()/".grok"` session-directory/resume/existence helper in both supervisor and operator code with that configuration value.
- Define immutable observation/result types for `aligned`, `visible-change`, `transient-missing`, and `unsafe`.
- Before starting either the leader or TUI, inject one adapter-owned marker block into the isolated seat's `config.toml`. Grok 1.0.13's external leader caches the user configuration at startup, so staging after leader readiness leaves the attached TUI with status-line execution disabled. Use an exact one-token command pointing to a mode-0700 executable shim; keep its socket path inside that shim. Transform an existing eligible config only through the reversible journaled transaction, preserving unrelated bytes, mode, ACLs, and xattrs; recover interrupted transactions before stage/cleanup. Refuse an existing foreign status line (including `type=disabled`), external `GROK_CONFIG`, a signed seat policy replica, managed-config synchronization, or an unowned marker.
- Reuse the authenticated ACP handshake's `agentVersion` allowlist and require the structured status payload version to agree with it.
- Accept bounded status input only through the private supervisor Unix socket. Require kernel peer PID/UID, stable Python executable and exact shim arguments, direct parent equal to the owned TUI, UUID, canonical cwd, trigger, monotonic age, state-event proof, replay floor, and post-gate timestamp. The helper emits no visible output; the supervisor atomically writes and authenticates the mode-0600 record with a key kept out of configuration and argv.
- Validate the selected session through only the allowlisted summary identity fields: `chat_format_version`, `created_at`, `info.id`, `info.cwd`, and `grok_home`; `git_root_dir` may be emitted as diagnostic data only. Never read title, recap, last-turn, session-summary, or message-content fields.
- Retain the strictly parsed, stable-read `active_sessions.json` observer as a diagnostic. It must not override pager focus or block a valid same-pane resume solely because the registry is stale/duplicated.
- Require `info.id == session_id`, both canonical cwd values equal the canonical configured project, and canonical `grok_home` equals the configured home. Grok 1.0.13 exposes no usable parent/fork/worktree provenance field, so none is claimed or inferred; a different worktree is rejected by its canonical cwd.
- Treat timestamps only as structured validity/stability data. Do not use `created_at` or history to claim command provenance: the visible session selected by the owned TUI is authoritative whether it was newly created or resumed.
- Never select another PID or fall back to the most recent directory.

**Dependencies:** None.

**Acceptance criteria:**

- Pager-selected same-PID/cwd transitions representing `/clear`, `/new`, and `/resume` become `visible-change`; a same-pane fork that is indistinguishable in the supported metadata follows the same rule.
- Same ID becomes `aligned`.
- Missing/stale/replayed/bad-auth/unowned-peer pager records, wrong cwd/home, invalid UUID/time, symlink/path escape, absent session directory, rapidly changing reads, and unsupported schema never become adoptable. Trailing-slash variants of the same canonical path remain valid.
- A valid resumed pager target remains adoptable when `active_sessions.json` still names the prior bound session or contains duplicate rows.
- Explicit tests set `GROK_HOME != Path.home()/".grok"`, prove the override is used by observer, supervisor resume, and operator resume, and reject a summary with the wrong `grok_home` or an escaping session path.
- Two distinct seat homes cannot observe one another's session metadata.

**Verification:** Run the new test module and the complete adapter unit suite with synthetic temporary homes.

## Task 2: Make the ACP connection an explicit replaceable session binding

**Purpose:** Rebind future HCOM prompts without restarting or injecting keystrokes into the visible TUI.

**Files:**

- Update `scripts/hcom_grok_seat/supervisor.py`.
- Update `scripts/hcom_grok_seat/acp_session.py` only where a public load-result/replay validation seam is required.
- Update `scripts/hcom_grok_seat/test_supervisor.py` and, if touched, `scripts/hcom_grok_seat/test_acp_session.py`.

**Contract:**

- Introduce one `SidecarBinding` aggregate containing `session_id`, ACP client, `TurnCollector`, and `PermissionBroker`.
- Change `connect_sidecar(target_session_id)` to create a new sidecar, initialize/authenticate it, load exactly that session, validate returned session ID and cwd, flush/drain replay, and return the binding only after it is usable.
- Give every candidate a fresh `ResumeReplayFence(mode="load")`. Enforce the existing 4,096-event and 64-MiB replay bounds (or stricter reviewed bounds); overflow, non-finite/invalid JSON, post-seal replay, or reconciliation failure enters bridge `DEGRADED` and must not escape into whole-supervisor cleanup. This is especially load-bearing for an explicitly resumed long-history session.
- Serialize attach/commit under a binding-transition lock that also excludes delivery and stale heartbeat publication.
- Preferred path: keep the old binding alive until the candidate passes validation, then commit/swap once and close the old transport. This path is enabled only if the disposable live probe proves the leader accepts concurrent old/new sidecars.
- Supported fallback: while delivery is paused, close the old transport, create and validate exactly one candidate, and enter `DEGRADED` if it fails. The TUI and leader remain alive; no old-session prompt is sent merely to hide the failure.
- Immediately after candidate spawn, publish a `binding_transition` block in `run.json` containing phase, from/to IDs, candidate sidecar PID, process start identity, and generation. Normal failure kills the candidate. Startup/doctor safely reap a leftover candidate only after matching UID, exact leader socket/argv role, recorded process start identity, and dead prior supervisor; otherwise they report it without killing an unproven PID.
- Persist stable process-start identities for the supervisor, leader, TUI, active sidecar, their owned process-group facts, and device/inode identities for both sockets. After a hard supervisor crash, restart terminates only positively matched children/groups of a dead prior owner and removes only the recorded socket inodes. Any missing/mismatched identity blocks relaunch without signaling the process or deleting the path.
- Centralize `run.json` publication so heartbeat/status updates merge the transition block instead of overwriting candidate ownership while a bind is in progress.
- Construct the collector and permission broker for the candidate ID. No handler may retain the previous ID after the swap.
- Do not use a second load on an existing client in this release.

**Dependencies:** Task 1 supplies the validated target.

**Acceptance criteria:**

- Fake ACP tests prove a replacement sidecar performs one load for S2 and the next prompt targets S2.
- Load response ID/cwd mismatch, replay mismatch, timeout, transport failure, or permission-handler mismatch leaves S as the persisted binding and sends no prompt to S2.
- Closing a failed candidate cannot close the visible TUI or leader.
- Simulated crash/restart with a recorded candidate PID reaps only a positively identified orphan; PID reuse, mismatched argv/socket/start time, or a live prior supervisor is never killed.
- Both the concurrent-candidate path and the one-sidecar fallback are deterministic under fake ACP tests; the live gate chooses which path may be enabled in the release.

**Verification:** Run focused ACP/supervisor tests, then the complete adapter unit suite.

## Task 3: Implement the guarded idle-boundary rebind state machine

**Purpose:** Make `/clear` keep working as a normal user action while guaranteeing that HCOM never silently continues in a hidden conversation.

**Files:**

- Update `scripts/hcom_grok_seat/supervisor.py`.
- Update `scripts/hcom_grok_seat/test_supervisor.py`.

**Contract:**

- Run a lightweight observation/heartbeat task alongside delivery so a 900-second model turn cannot starve visibility state or HCOM heartbeats.
- Store background observation changes as pending/status state only. The main delivery loop performs rebinds serially between HCOM events; a cached observation is never authority to begin a prompt.
- Immediately before every request/inform delivery, record a monotonic gate and wait no more than three seconds for a valid pager sample captured after it. Require alignment or complete a pending safe rebind before calling `session/prompt`; timeout holds the letter and cursor.
- If any visible-session change occurs during a delivery, finish that exact old-session turn and correlated reply, then rebind before examining the next model-bound event.
- On ambiguity or failure, enter `DEGRADED`, keep the TUI/leader alive, publish a bounded reason, and leave the first undelivered model-bound event uncommitted.
- Entering `REBIND_PENDING`, `REBINDING`, or `DEGRADED` immediately stops the current `process_mail` batch. No later ack, quarantine, request, or inform row may advance the cursor past the first held model-bound event; the next pass begins from the unchanged cursor after alignment is restored.
- Catch bridge/ACP failures at the bridge boundary; do not let them fall into whole-supervisor cleanup unless the TUI, leader, or supervisor itself is stopping.
- Debounce rapid consecutive visible changes with a 350 ms stable state-proof window. After ACP attach, require a fresh liveness sample captured after the attach gate, then synchronously re-read the same focus under the supervisor focus-admission lock immediately before the durable commit; a changed target discards the candidate and retries from observation.
- Emit one structured transition record with from/to IDs, generation, reason=`visible_tui_selection`, TUI PID, and outcome. Do not guess the slash command. On the first delivery after every successful rebind, prepend the small seat/HCOM_DIR/no-poll bridge context from `rules_text`; the normal per-letter `prompt_text` continues to carry the reply contract. This makes an older resumed session safe without reading its history or assuming it inherited the launch system prompt.

**Dependencies:** Tasks 1 and 2.

**Acceptance criteria:**

- Aligned operation preserves current delivery/reply behavior.
- Idle `/clear`, `/new`, or explicit same-pane `/resume` causes one sidecar replacement, one state transition, and delivery of the next letter to the visible session.
- Mid-turn `/clear` never changes the in-flight prompt ID/session and never duplicates its reply.
- A post-gate pager sample catches a switch that occurs between the background poll and the delivery gate; the test keeps the prior sample before the gate and asserts no prompt is admitted until a newer aligned/change sample exists.
- Given one batch `[request that is held, ack]`, the request and ack both remain beyond `last_event_id`; no later row skips the request.
- Rapid switches, a foreign-PID dashboard/home peer, different-cwd worktree, invalid metadata, and sidecar failure block delivery rather than selecting a guess.
- A degraded bridge remains visible and locally usable in Ghostty.
- Heartbeat/run-state timestamps continue changing during a long delivery.

**Verification:** Add deterministic async tests with fake observation and ACP sources, including controlled scheduling for switch-during-delivery and rapid-switch races.

## Task 4: Commit binding state with an explicit crash order

**Purpose:** Ensure status, resume, HCOM routing, and recovery agree on the new conversation after a successful sidecar attach.

**Files:**

- Update `scripts/hcom_grok_seat/supervisor.py`.
- Update `scripts/hcom_grok_seat/operator.py`.
- Update `scripts/hcom_grok_seat/test_supervisor.py` and `scripts/hcom_grok_seat/test_operator.py`.

**Contract:**

- Extend session state with `launch_session_id`, current compatibility `session_id`, binding generation/time, previous ID, and transition reason. Keep the existing `session_id` and `project` keys valid for rollback. Process IDs and transition-in-progress diagnostics belong in `run.json`, not durable session identity.
- Declare the fsync-and-rename of `session.json` via the existing `atomic_json` helper as the single durable binding commit point and restart source of truth. There is deliberately no claim of one transaction across JSON files, process memory, and SQLite.
- Execute this serialized protocol while the transition lock blocks delivery and heartbeat binding writes:
  1. Publish the candidate process identity/target as a non-authoritative `run.json.binding_transition`, then initialize/load the candidate sidecar.
  2. Validate the ACP result ID/cwd and replay drain; freshly re-observe the TUI and require the same S2.
  3. Atomically replace `session.json` with compatibility `session_id=S2`, `previous_session_id=S`, and incremented generation. This is the commit: before it S is authoritative; after it S2 is authoritative.
  4. In one HCOM SQLite transaction, update `instances.session_id`, upsert `process_bindings.session_id`, insert the new `session_bindings` row, and delete the superseded S row for this instance. Heartbeat idempotently reasserts this mirror on restart and thereafter.
  5. Swap the in-memory `SidecarBinding` to S2 under the same lock. From this point no code path may publish or prompt S. Close the old transport when present. If its positively identified process does not exit, retain its identity in transition state and remain `DEGRADED` until it is reaped; never roll back the committed ID.
  6. Only after obsolete-sidecar cleanup succeeds, publish final `run.json` alignment and clear `binding_transition`.
- If any ordinary exception occurs before step 3, terminate the candidate and retain S. If an exception occurs after step 3, never rewrite S: hold delivery in `RECOVERING_COMMITTED(S2)`/`DEGRADED`, retain or recreate an S2 sidecar, and retry the idempotent run/HCOM mirrors.
- Startup reads `session.json` first, safely reaps a positively identified leftover candidate/old sidecar from stale `run.json`, loads the committed session, and repairs `run.json` and HCOM before processing mail. If HCOM says S2 while `session.json` says S, `session.json` wins and HCOM is repaired to S; the prescribed writer order never intentionally creates that inverse state.
- No state may claim S2 in `session.json` before ACP validation and the stable second observation. `run.json` may expose a pending target only when clearly marked non-authoritative.
- `status`, `inspect`, and `doctor` expose `bound_session_id`, `visible_session_id`, `session_alignment`, binding generation, focus source, pager trigger/sample health, diagnostic registry ID/reason, and a bounded degraded reason. Doctor fails when a running seat is divergent or lacks a validated pager focus sample.
- Preserve cursor and pending-reply files byte-for-byte across a rebind except for normal completion of the already in-flight event.

**Crash recovery table:**

| Last completed boundary | Durable authority | Restart action | Mail consequence |
|---|---|---|---|
| Before candidate validates | S | Reap a positively identified candidate; load S | Cursor unchanged; no S2 prompt |
| Candidate valid, before `session.json` replace | S | Discard candidate; load S | Cursor unchanged; no S2 claim |
| `session.json` replaced, before HCOM transaction | S2 | Load S2; reconstruct mirrors; delete stale S HCOM row | Hold mail until repair; never revert to S |
| HCOM updated, before memory/run finalization | S2 | Load S2; idempotently reconstruct process state | Hold mail until repair; no duplicate prompt |
| Final `run.json` published and transition cleared | S2 | Normal resume; reap any recorded obsolete sidecar | Existing cursor/pending-reply semantics apply |

**Dependencies:** Task 3.

**Acceptance criteria:**

- Restart resumes the last successfully visible-bound ID.
- A previous installed release can still read `session_id` after rollback.
- HCOM contains no stale old-session binding for the active instance.
- Fault injection at every table boundary resolves from `session.json` to S or S2, never an invented third state and never a lost HCOM event.
- Explicit tests cover `session.json=S2` with `run.json`/HCOM still S, the externally inconsistent inverse (`session.json=S`, HCOM S2), stale candidate PID identity, and failure to close the old sidecar.
- Heartbeat cannot republish S after the `session.json` commit because it shares the transition lock and reads the committed in-memory binding.

**Verification:** Add state-migration, SQLite binding, restart, rollback-readability, and fault-injection unit tests.

## Task 5: Close the room/socket isolation gap without moving ownership into Plugins Manager

**Purpose:** Allow identical stable seat names and real project paths to run in distinct manager projects/HCOM rooms.

**Files:**

- Update `scripts/hcom_grok_seat/supervisor.py`.
- Update `scripts/hcom_grok_seat/test_supervisor.py` and `scripts/hcom_grok_seat/test_registry.py` as appropriate.

**Contract:**

- Derive the default short socket hash from the resolved `state_root` plus user ID, not only seat and project.
- Continue honoring an explicit `HCOM_GROK_SOCKET` override and the Unix socket length check.
- Do not ask Plugins Manager to calculate or pass an adapter-internal socket.

**Dependencies:** Can be implemented alongside Task 1; must be verified before the same release.

**Acceptance criteria:**

- Same seat and project with two state roots/HCOM rooms produce different socket paths.
- Same state root deterministically produces the same path and remains protected by the existing supervisor lock.
- Managed room seats and Plugins Manager private seat homes remain isolated.

**Verification:** Unit-test the socket derivation and run the existing registry multi-room tests.

## Task 6: Preserve the Plugins Manager boundary and document the operational contract

**Purpose:** Make the ownership decision explicit without duplicating the adapter state machine in the launcher.

**Files:**

- Update `scripts/hcom_grok_seat/README.md`.
- Merge a small compatibility note into `/Volumes/Atlas/_____PLUGINS_MANAGER/docs/CLIENT_COMPATIBILITY.md` after reconciling its existing uncommitted edits.
- Update `/Volumes/Atlas/_____PLUGINS_MANAGER/src/plugins_manager/runtime.py` and its focused test to set `HCOM_GROK_ISOLATED_HOME=1` only for manager-owned Grok seats.

**Contract:**

- Document that `/new`, `/clear`, and a human-selected same-pane `/resume` rebind future HCOM delivery to the uniquely validated visible conversation; an in-flight old turn completes where it began.
- State the honest same-pane rule: Grok 1.0.13 cannot distinguish a same-PID/same-project fork by provenance, so it follows the visible session too. Foreign-PID peers, other cwd/worktree/home sessions, ambiguous metadata, and failed sidecar attach remain blocked.
- Document the status/doctor alignment fields and that HCOM threads/mail cursor survive a conversation clear.
- State that Plugins Manager owns one private home per `(projectId, seatId)` and launches `hcom-grok`; conversation identity remains adapter-private.
- State that the marker is only an admission hint: the adapter still proves the login-home boundary, exact home shape, contained state root, filesystem ownership, and exclusive supervisor claim. Shared/unproven homes retain their local TUI but block model-bound HCOM delivery rather than risk a hidden resumed conversation.
- Preserve the existing bare-launch-means-fresh and explicit `-c`/`resume` behavior. Manager-driven automatic resume is a separate product decision.

**Dependencies:** Tasks 1-5 define the final behavior.

**Acceptance criteria:** Documentation matches the tests and does not imply terminal scraping, adoption outside the exact owned-TUI/project/home contract, or stronger OS isolation than Plugins Manager provides.

**Verification:** Re-read both documents against the final CLI/status output and run existing Plugins Manager Grok launch tests.

## Task 7: Verify offline, then prove the one live visibility contract in a disposable seat

**Purpose:** Prove the fix at both protocol and human-visible boundaries without risking the current `grok-main` session or real profile data.

**Offline verification:**

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts/hcom_grok_seat -t . -p 'test_*.py'
```

In Plugins Manager, after first recording the pre-existing dirty-worktree baseline and merging around it without overwriting user changes:

```bash
ruff format --check src tests
ruff check src tests
python3 -m compileall -q src tests
.venv/bin/python -m pytest
```

**Disposable live proof:**

- Use a scratch project, scratch `HCOM_DIR`, and a fresh synthetic seat home. Link only the existing allowlisted auth node; never copy or inspect its content.
- Launch one foreground Grok seat through the same structured environment as Plugins Manager, including the isolated-home marker and mode-0700 seat directories. Confirm the general TUI environment contains no record path or authentication key.
- Confirm journaled exact marker-block injection into `config.toml` -> leader spawn -> TUI spawn -> kernel-authenticated helper -> owned pager sample -> sidecar/TUI/leader stop -> exact block/artifact cleanup. Confirm unrelated config bytes, mode, ACLs, and xattrs survive injection and cleanup, including recovery of an interrupted transaction. The status-line command is exactly the mode-0700 shim path; only that shim contains the private ingest-socket path. Neither leader nor sidecar may successfully submit a sample.
- Send a unique non-secret HCOM marker and prove it is recorded under the session currently reported by pager status.
- Run `/clear`, wait for one successful binding-generation increment, and send a second marker.
- Confirm the second marker/response belongs to the new active session and is visible in the same pane; confirm it is absent from the abandoned session after the transition.
- Test the configured close-old-first handoff, plus the guarded concurrent mode where supported; if both approaches fail, ship detection plus `DEGRADED` only.
- Exercise an explicit same-pane `/resume`; confirm pager status changes while a deliberately stale active registry does not prevent future delivery following the visible resumed session. Exercise a foreign-parent or foreign-cwd record and confirm no hidden prompt and no cursor advance.
- Force a held batch shaped `[request, ack]` and confirm the ack cannot advance the cursor past the request.
- Launch two disposable rooms with the same real cwd and seat label and confirm distinct state roots/sockets and room-local delivery.
- Fault the supervisor during candidate load and at each durable-commit boundary; confirm restart follows `session.json`, repairs HCOM, and leaves no positively identified orphaned leader/sidecar.
- Stop both seats normally and confirm HCOM stop lifecycle and no orphaned leader/sidecar.

The live proof is a release gate. If the exact injected recorder block is not loaded by the TUI, a fresh sidecar cannot load a TUI-created session, or its prompt does not render in that TUI, do not ship automatic rebinding; retain `DEGRADED` hold and escalate the missing Grok API.

**Completion evidence (2026-09-04):** a fresh disposable isolated seat passed
the production sequence `leader -> ACP session/new -> first and only TUI
--resume`. Its pager shim produced a fresh kernel-authenticated sample, a real
ACP `session/prompt` completed on that same session, and an injected `/clear`
created a new visible session which the adapter rebound successfully. These
gates used a scratch home and a linked auth node only; the live `grok-main`
seat was not touched.

## Explicit non-goals

- No Ghostty keystroke injection or screen scraping.
- No Plugins Manager session registry or Grok-specific dashboard controller.
- No HCOM protocol/database redesign beyond replacing this seat's mirrored session binding.
- No command-provenance inference. The adapter follows only the uniquely validated session selected by its exact owned TUI PID; it never adopts a foreign-PID dashboard/fork peer, foreign cwd/worktree/home, or ambiguous mapping.
- No copying, merging, or sharing Grok histories between conversations.
- No change to auth/keychain isolation.
- No global install, active-seat restart, credential change, or live `grok-main` mutation as part of implementing or reviewing this plan.

## Coverage review

| Requirement / risk | Covered by |
|---|---|
| `/clear` keeps HCOM visible | Tasks 1-3, 7 |
| No hidden-session delivery | Tasks 1, 3, 7 |
| In-flight reply integrity | Tasks 3-4 |
| Cursor/pending reply continuity | Tasks 3-4 |
| Sidecar-only recovery; TUI survives | Tasks 2-3 |
| Restart and rollback compatibility | Task 4 |
| HCOM binding cleanup | Task 4 |
| Multiple projects/HCOM rooms | Tasks 1, 5, 7 |
| Plugins Manager ownership boundary | Task 6 |
| No secrets or terminal scraping | Constraints, Tasks 1 and 7 |
| Unknown Grok behavior fails closed | Tasks 1-3, 7 |

## Audit and convergence record

- Sixteen HCOM peers were invited across Grok, Claude, Cursor, and Antigravity. Fifteen responded; every responding reviewer approved the current plan. The remaining Antigravity pane (`agy-main-solo`) had uncommitted terminal input and did not consume its queued audit request, so it is recorded as unavailable rather than silently counted as approval.
- The first audit correctly blocked the draft for nonexistent parent/worktree provenance, a false cross-file/SQLite atomicity claim, a cursor-skip hazard after a held request, a cached-observation delivery race, unproven concurrent sidecars, incomplete `GROK_HOME` handling, and unsafe orphan ambiguity. Those findings are now explicit contracts and tests in Tasks 1-4 and 7.
- A focused seven-reviewer decision was unanimous that visibility ownership is the only honest policy: follow the uniquely validated session selected by the owned TUI, including an explicit resume, instead of pretending timestamps reveal slash-command provenance.
- The convergence audit found one final live-schema issue: Grok's `git_root_dir` carried a trailing slash. The plan now canonicalizes routing paths, makes `git_root_dir` diagnostic-only, and accurately treats `active_sessions.json` as structurally pinned but unversioned. All fifteen responding reviewers approved that final delta.
- Implementation testing then found the stale-registry `/resume` counterexample. Grok, Claude, and Cursor reviewers audited the replacement pager-status design. Live probes established direct TUI parentage, environment inheritance, marker-block startup order, user-status-line precedence, and shadow-home incompatibility. The later managed-config race was resolved by the reversible isolated-seat `config.toml` marker injection described above. Those findings supersede the original active-registry routing premise as recorded above.
- Residual release risk is intentionally concentrated in Task 7: if the owned recorder, close-old-first/concurrent sidecar handoff, or visible paint gate fails, automatic rebinding does not ship; `DEGRADED` hold does.

## Builder handoff

The builder must read this source specification and the current repository state before editing. Pager status, not `active_sessions.json`, is the focus authority. If live Grok 1.0.13 behavior conflicts with the pager-load, sidecar-load, replay, or visible-render contract, stop at the Task 7 gate and report the conflict rather than substituting a heuristic. Preserve unrelated dirty changes in Plugins Manager; its only production delta in this delivery is the reviewed isolated-home launch marker.
