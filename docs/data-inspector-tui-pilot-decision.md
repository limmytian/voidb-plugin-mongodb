# MongoDB Data-Inspector TUI Pilot Decision (Requirement 83)

- **Plugin / profile type**: `voidb-plugin-mongodb` / MongoDB profiles
  (`mongodb://` or `mongodb+srv://` URI, optional SCRAM/X.509/AWS-IAM auth,
  optional default database).
- **Selected state**: `Accepted pilot` (family precedent: the Redis, MySQL,
  and PostgreSQL data-inspector pilots established that a bounded
  plugin-owned pilot with fixture, target-error, and real PTY evidence
  produced in the same requirement satisfies the Requirement 83 gate).
- **Command**: `voidb-plugin-mongodb tui` (plugin-owned, CLI-launched), also
  routed as `mongodb tui` through `voidb-cli`.
- **Scope**: document triage. This is new plugin-owned UI work built on the
  existing `MongoService` channel contract; it adds no backend operations and
  does not depend on removed router-hosted factories, shared legacy data
  widgets, or shell-owned plugin UI state.
- **Prior state**: `voidb/docs/data-inspector-cli-first-decisions.md` recorded
  MongoDB as `Later candidate`, shipping as `CLI-only`: "capability and
  fixture evidence are strong enough to make this a future candidate … the
  TUI gate is still incomplete because there is no plugin-owned
  document-triage UX brief, PTY evidence, first-frame/error transcripts, or
  TUI mutation confirmation design." That brief, the fixture/target-error
  evidence, the PTY transcripts, and the typed mutation-confirmation design
  are produced and accepted here.

## Mutation scope decision (recorded up front)

Slice 1 ships **one** mutation: a bounded **single-document delete anchored
on `_id`**. The service layer exposes `DocumentCommand::Delete { db,
collection, filter }` (the same op the `mongodb delete` CLI command uses,
backed by a filter-based delete). The TUI constrains that surface to an exact
`{ "_id": <the viewed document's _id> }` equality filter, and MongoDB bounds
an `_id` equality predicate to at most one document because `_id` is the
unique primary key of every collection. The delete therefore cannot be bulk,
unbounded, or a `deleteMany` over an operator-authored filter — the filter is
never operator-authored. Guards: the delete is refused up front when the
viewed document has no `_id` field; execution requires **typed confirmation
of the exact collection name** plus an explicit `y` (two confirmations,
`destructive` risk); `--readonly` launches stage the plan but block at
confirm; the `_id` is echoed in the confirmation popup, the staged plan, and
the result status; and the current page is refetched after execution so the
deleted document visibly disappears. Insert, update, aggregation-pipeline
console, index create/drop, database commands (`run_command`), and bulk write
are **not** part of this slice and are unreachable from the TUI (`later
candidate`, listed below). Mutation policy is recorded in
`evidence["mutation_rescan_policy"]`.

## Named human workflow

**Document triage on a live cluster.** An operator responds to a data-quality
or incident report about poisoned documents:

1. **Start**: the operator connects through an existing profile
   (`mongodb tui --profile prod-cluster --purpose triage`). The TUI opens
   read-only on the databases view (the profile's default database, when set,
   is preselected and its collections load immediately); nothing is loaded
   beyond bounded metadata lists.
2. **Browse**: the operator presses `Enter` on a database and lands in the
   collections browser with name and document-count columns, so oversized
   collections are visible before any of them is opened.
3. **Page**: the operator presses `i`/`Enter` on a collection and the
   full-screen DOCUMENT VIEWER opens on the first bounded page
   (`Find { filter, limit: 10, skip: 0, sort: { _id: 1 } }` through the
   service channel). `n` fetches the next page, `b` walks back with an offset
   history stack; pages are **replaced**, not appended, so the viewport stays
   bounded. The viewer title reports `rows X-Y of T page P`, so empty
   collections, partial final pages, and end-of-collection states are all
   visible.
4. **Narrow and inspect**: `f` accepts a server-side JSON query filter (for
   example `{"status": "failed"}`) and rescans from page 0 through the
   service's existing filter parameter; `/` filters the loaded page
   client-side with a live match count; `Enter` on a document opens the typed
   full-screen document viewer rendering `_id`, `ObjectId("…")` values,
   nested subdocuments, arrays, dates, binaries, and literal `null`s, one
   line per field, scrollable inside a bounded line buffer.
5. **Act**: for the poisoned document the operator presses `d`, **types the
   exact collection name**, and presses `y`; the delete is a single
   `_id`-anchored delete through the service's existing delete path, and the
   current page is refetched so the document disappears and the total drops.
   A failed delete (authz scope, replica lock) shows a redacted target error
   in a non-modal banner and the operator keeps working.
6. **Success state**: without leaving the TUI, the operator has walked
   database → collection → page → document and removed one identified
   document, with the refreshed page and total as confirmation.

**Why the CLI is materially worse here**: the same loop requires
`mongodb dbs`, then `mongodb collections <db>`, then one
`mongodb find <db>.<coll> --skip n --limit 10` invocation per page plus a
separate `mongodb delete <db>.<coll> '{"_id": {"$oid": "…"}}'` per candidate
document with a hand-copied `_id`. Every step pays a fresh process startup,
none share namespace/page/filter context, and the operator retypes `_id`
hex strings into shell history — the exact typo class that makes CLI
deletes dangerous. The TUI keeps the database → collection → page → document
state, per-view selection, per-page filter, query filter, and typed document
inspection in one viewport, so an operator walks a suspect collection page
by page in one sitting and deletes by pointing, not by retyping identifiers.
Generic "browse data" is not the claim; the claim is cursor-preserving triage
over repeated browse-page-inspect cycles across the collections of one
cluster.

## Gate-by-gate evidence

| Gate | Evidence in this implementation |
|---|---|
| Named human workflow | Above; exercised end to end in fixture mode and by the PTY transcript (open `appdb` → open `orders` → page 1→2 with a partial final page → inspect the typed document → typed delete of `_id …9c02` on page 2, refetch shows 11 documents and the document gone). |
| Capability-backed bounded reads | Reads are the existing service-layer operations only: `DatabaseCommand::ListDatabases`, `DatabaseCommand::ListCollections { db }`, and `DocumentCommand::Find { db, collection, filter, limit, skip, sort }` with `limit = PAGE_SIZE (10)`, `skip = page * PAGE_SIZE`, and `sort = { "_id": 1 }` so pagination is deterministic (`FindResult.total` comes from the service's own `count_documents`). The TUI adds no new backend operations; there is no free-form query, aggregation, or raw-command surface in the pilot. |
| Pagination and large-result safety | One page (10 documents) is rendered at a time; `n` fetches `skip + PAGE_SIZE` only and pages are replaced, never appended, so the viewport stays bounded; the previous skip is kept in a history stack for `b`. The viewer title reports `rows X-Y of T page P`, so the empty state (`appdb.empty_collection`: "No documents on this page…"), the partial final page (`orders`: 12 documents over 2 pages), and the end-of-collection state are all visible. Document-detail bodies are bounded by `BoundedLines` at 2,000 lines / 2 MiB with dropped-line counters in the status panel. |
| Redaction and error shape | Preflight (`--format json`) reports the connection as `"kind": "mongodb"|"mongodb+srv"` (URI scheme only), `"auth": "password"|"x509"|"aws_iam"|"none"`, and `"default_db"` with `"secret_material": "redacted"` — never the URI, hosts, or credentials. `secret_leak_markers` scan covers `mongodb_password_value`, `mongodb_secret_data_value`, `plaintext_credential_value`, `raw_connection_url` and passes (`passed: true`, 0 markers) in `build_mongodb_tui_evidence` output; errors render through `safe_error_summary` in the non-modal banner and keep the family `target error:` category text from the service error contract (validation/auth/permission/transport/timeout/plugin/target categories come from the shared capability error contract). |
| Mutation safety | First viewport is read-only. `--readonly` launch stages plans but blocks at confirm with `readonly launch blocked delete document on <db>.<collection>`. Document delete requires **typed confirmation of the exact collection name** plus explicit `y` (two confirmations, `destructive` risk), echoes the `_id` in the popup/plan/status, is refused up front when the document has no `_id` field, and sends exactly one `_id`-equality filter through the existing `DocumentCommand::Delete` path — no bulk, unbounded, or operator-authored-filter delete exists in the TUI. See the mutation scope decision above. |
| Fixture and target-error evidence | `fixtures/mongodb_tui_operations.json` covers 4 databases (including an empty database), collections with document counts (12/4/1/0, 3, 2), extended-JSON documents exercising `ObjectId`, `$date`, nested subdocuments, arrays, binary, double/bool/null scalars, paging over 10-document pages (full, partial, empty), and a deliberately erroring collection (`appdb.locked_collection` → `target error: not authorized on appdb.locked_collection …`). Startup carries a `target error: not authorized on appdb …` banner; the PTY proves the TUI stays fully responsive across it (all assertions pass with the banner present on the first frame). |
| PTY lifecycle evidence | Real PTY run (`pty.fork()` + `TIOCSWINSZ` `HHHH` at 36x130, winsize set pre-exec): assertions across consecutive clean runs, exit code 0 each run. First frame markers (`MongoDB Document TUI (pilot)`, `1:databases`, `2:collections`, `fixture-mongodb-ops`, fixture collections, hint footer, target-error banner); navigation into the collections browser and the document viewer with paging; typed-delete confirmation and refetch; resize to 24x100 keeps content visible and reports `resized mongodb view to 100x24`; `q` exits 0 with terminal restore; raw-mode cleanup is delegated to `ratatui::restore()` after the loop (raw input is not used: `raw_input = false`). |
| No legacy dependency | The command lives in the plugin (`src/tui.rs`), is launched from the plugin CLI (`mongodb tui`), consumes only `MongoService` channel events, and contains no reference to removed router-hosted data-inspector adapters or shared legacy widgets. The agent-session/agent_live machinery is untouched. |

## Interaction surface

- Views and hotkeys: `1` databases browser, `2` collections browser (Tab
  cycles); pending database/collection/document fetches show the family `⟳`
  marker in the header tabs. The profile's default database, when set, is
  preselected on connect.
- Databases browser: name/size/empty columns with a details panel; `Enter`
  opens the collections browser for the selected database.
- Collections browser: name/document-count columns; `i`/`Enter` opens the
  full-screen DOCUMENT VIEWER (one bounded page at a time from the service's
  `Find`) with `j/k`/PgUp/PgDn document cursor, `n` next page, `b` back
  (replace semantics, skip history), `f` server-side JSON query filter
  (rescans from page 0), `/` client-side page filter with live match count,
  and `r` refetch.
- Document inspect: `Enter` on a document opens the typed full-screen
  document viewer (one line per field: `ObjectId("…")`, RFC-3339 dates,
  nested subdocuments indented, arrays itemized, binary size, literal
  `(null)`) with scroll and a bounded line budget; `Esc`/`q` returns to the
  paged list viewer.
- Mutation: `d` on a document (document viewer or document detail) stages a
  typed delete (type the exact collection name, then `y`; `_id` echoed) and
  returns to the viewer it was opened from; readonly launches stage but block
  at confirm; the current page is refetched after execution.
- Confirm-quit fires while a fetch or a staged plan is active; the error
  banner is non-modal (any key acknowledges while the key keeps its meaning).

## Deferred inside the accepted pilot

- **Indexes view**: `IndexCommand::List { db, collection }` exists in the
  channel contract and is read-only, but the slice keeps the triage surface
  to the named document-triage workflow; a third view needs its own fixture,
  interaction tests, and PTY coverage. `later candidate` — it can slot into
  hotkey `3` with that evidence.
- **Aggregation console**: `DocumentCommand::Aggregate` exists, but pipeline
  authoring UX plus read-only stage validation need their own evidence;
  `later candidate`.
- **Insert / update / bulk_write / create_index / drop / `run_command`**:
  destructive or blast-radius workflows, all `later candidate`; the CLI
  (`mongodb insert/update/delete/aggregate/indexes/create-index/exec`, and
  `voidb-cli invoke run mongodb.<capability> …`) remains the supported
  general path.
- **Count view / server diagnostics**: the channel contract has no bounded
  server-info command (the Redis `ServerCommand` shape has no mongodb
  counterpart), and `dbStats` would have to go through the destructive-gated
  raw `run_command` surface; `later candidate` until a bounded diagnostics
  command exists.
- Until those ship, docs and release copy must keep pointing to the CLI and
  capability path for writes, aggregation, index management, and diagnostics;
  `plugin.toml` declares `[ui] tui = true, raw_input = false` for this
  bounded pilot only.

## Validation commands and evidence

All run in `voidb-plugin-mongodb` with `VOIDB_DISABLE_KEYRING=1` (no OS
keyring touch):

```bash
VOIDB_DISABLE_KEYRING=1 cargo test            # 21 pre-existing + TUI interaction tests
cargo clippy --all-targets -- -D warnings     # 0 warnings (CI runs -D warnings)
cargo fmt --all && cargo fmt --all -- --check # clean (CI enforces this)
cargo run --example export_contract -- --check # manifest/contract drift green
```

TUI interaction tests (in `src/tui.rs#[cfg(test)]`): fixture evidence +
secret-leak scan, preflight URI/credential redaction, non-modal startup
banner, tab hotkeys and cycling, database selection into collections,
collection paging with page/total state and skip history, query-filter
prompt rescan from page 0 with invalid-JSON rejection, client page-filter
match count, typed document rendering (ObjectId/nested/array/date/null),
typed-delete exact-collection-name gate, delete-execute + page refetch,
delete refusal without `_id`, erroring-collection find and delete
responsiveness, readonly block, empty collection state, confirm-quit,
page-key movement, and per-view selection memory.

Preflight and evidence:

```bash
target/debug/voidb-plugin-mongodb tui --fixture fixtures/mongodb_tui_operations.json --format json
target/debug/voidb-plugin-mongodb tui --fixture fixtures/mongodb_tui_operations.json --evidence evidence.json
```

Evidence JSON: `kind: mongodb_tui_fixture_evidence`, quality gate
`retained_tui_quality_gate("mongodb", ["fixture-mongodb-ops", "fixture-mongodb-local"], ["target error"], 80, 24, 10_000)`,
coverage entries including the new interactions (`database_selection`,
`collection_paging_skip_history`, `query_filter_prompt`,
`viewer_filter_match_count`, `document_viewer_typed_rendering`,
`typed_delete_confirmation`, `mutation_refetch`, …),
`secret_leak_scan.passed = true`.

PTY lifecycle record (macOS, Python 3.9 stdlib only, `struct "HHHH"`, 36 rows
x 130 cols): assertions across consecutive clean runs, exit code 0 each run;
script retained out-of-repo at `/tmp/mongodb_tui_pty_smoke.py` (run
`python3 -I /tmp/mongodb_tui_pty_smoke.py`). Covered: first frame
(non-blank, marker-checked), banner acknowledgement, database → collections →
document viewer navigation, paging forward/back with skip state and the
partial final page, client page filter with match count, typed document
detail, typed-delete confirmation and refetch, live resize with content
retention, normal `q` quit with clean exit 0 and terminal restore. Local
Python is 3.9 (no tomllib), so the python provenance tests cannot run on this
machine (CI runs 3.11+); this does not affect the PTY evidence, which uses
only stdlib.

## Migration note

This pilot is new plugin-owned UI work. It does not restore, wrap, or depend
on the router-hosted MongoDB data-inspector adapters removed in Requirement
77, and it does not reintroduce shared legacy data widgets or shell-owned
plugin UI state. The default TUI guidance for MongoDB profiles remains the
CLI/capability path; `mongodb tui` is a bounded, profile-scoped triage pilot
whose only supported mutation is a single `_id`-anchored document delete.
Release docs must not advertise the deferred indexes view, aggregation
console, insert/update workflows, or raw-command surface as available in the
TUI.
