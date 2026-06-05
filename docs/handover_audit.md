# Handover Audit — Known Architectural Defects

This is the triage entry point. Each finding (F-N) is a known weakness in the system, with a
file:line citation, a severity, and a current status. The docs throughout `docs/` link here by
anchor (e.g. `handover_audit.md#f-9-...`) — when a doc says "see F-9," this is where it lands.

**How to use this during an incident:** scan the one-line summaries below, find the one whose
symptom matches what you're seeing, jump to it, and read the "Status" line first — several of
these were resolved after the original audit and are kept here only so the cross-references stay
intact.

| # | Severity | Status | One-liner |
|---|----------|--------|-----------|
| [F-1](#f-1-webhook-ingestion-has-no-signature-validation) | — | ✅ Resolved | Webhook ingestion had no auth gate |
| [F-2](#f-2-cv-storage-uses-public-bucket-with-predictable-filenames) | Medium | ⚠️ Open (accepted) | CV storage uses a public bucket with predictable filenames |
| [F-3](#f-3-factorys-stage-options-update-is-still-destructive-march-audit-bug-3-not-fixed) | High | ✅ Resolved (guard) | Factory Stage-options replacement is destructive on re-run |
| [F-4](#f-4-no-initial-history-entry-when-an-application-is-created-march-audit-bug-2-not-fixed) | Low | ⚠️ Open (known) | No initial history entry when an application is created |
| [F-5](#f-5-storage-clients-cv-download-has-no-timeout) | Medium | ⚠️ Open | StorageClient's CV download has no timeout |
| [F-6](#f-6-factorys-destructive-notion-writes-have-no-rollback-on-supabase-failure) | Medium | ⚠️ Open (mitigated) | Factory's destructive Notion writes have no rollback on Supabase failure |
| F-7 | — | n/a | *(reserved — never assigned)* |
| [F-8](#f-8-bulk-import-auto-tags-every-candidate-as-headhunter) | Low | ⚠️ Open (by design) | Bulk import auto-tags every candidate as Headhunter |
| [F-9](#f-9-identity-by-name-merge-can-silently-combine-different-people) | High | ⚠️ Open (by design) | Identity-by-name merge can silently combine different people |
| [F-10](#f-10-concurrent-source-multi-select-append-is-a-read-modify-write-race) | Low | ⚠️ Open | Concurrent Source multi-select append is a read-modify-write race |
| [F-11](#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant) | Medium | ⚠️ Open (invariant) | ZWSP prefixes on stage names are a hidden invariant |
| [F-12](#f-12-find_child_database-silently-returns-none-past-depth-4) | Low | ⚠️ Open | `find_child_database` silently returns None past nesting depth 4 |
| [F-13](#f-13-identity-or-filter-is-built-by-unescaped-string-interpolation) | Medium | ⚠️ Open | Identity OR-filter is built by unescaped string interpolation |
| [F-14](#f-14-central-reference-handler-is-all-or-nothing--duplicates-on-retry) | Low–Med | ⚠️ Open | Central-reference handler is all-or-nothing → duplicates on retry |

> **Provenance:** F-1–F-12 were compiled at the June 2026 handover from a forensic read of the
> codebase plus the March 2026 incident report (recoverable via `git show HEAD:docs/audit_march_2026.md`).
> The March report used "Bug 1/2/3" numbering; F-3 = its Bug 3, F-4 = its Bug 2. "Status" reflects
> what the original author confirmed at handover. **F-7 was never assigned** — the sequence skips it.

---

## F-1 Webhook ingestion has no signature validation

**Status: ✅ RESOLVED (June 2026).** This was true when the audit was first drafted, but is no
longer the case. Notion *automation* webhooks are unsigned (no `X-Notion-Signature`), so HMAC
verification is impossible. Authentication is now handled by a **shared-secret URL path token**
(`WEBHOOK_PATH_TOKEN`), added in commit `8fd6832` and verified by `verify_path_token()` in
[core/webhook_router.py:39](../core/webhook_router.py) at the very top of the HTTP branch of
`lambda_handler`. The gate **fails closed**: if the token env var is unset/empty, every
Function-URL request is rejected with 401. Full design in
[.claude/rules/webhooks.md](../.claude/rules/webhooks.md) ("Security Model").

**Residual risk:** no replay protection (a captured valid URL+body can be replayed; mitigated by
TLS + handler idempotency), and rotation is manual. See `webhooks.md` "Known gaps."

---

## F-2 CV storage uses public bucket with predictable filenames

**Severity: Medium · Status: ⚠️ Open (accepted).**

**Location:** [core/storage_client.py:22](../core/storage_client.py) (`bucket_name = "resumes"`),
[storage_client.py:54](../core/storage_client.py) (`get_public_url`).

CVs are uploaded to a Supabase Storage bucket and exposed via `get_public_url()` — i.e. the bucket
is public-read. Object paths are `{unix_timestamp}_{sanitized_original_filename}`, which is only
weakly unguessable: anyone who knows the bucket URL and can guess a timestamp + a candidate's CV
filename could fetch the document. These are real people's résumés (PII).

**Recommendation:** move to a private bucket with signed (time-limited) URLs, or at minimum
randomize the path with a UUID instead of a timestamp. Weigh against the fact that the public URL
is currently stored directly on the Notion page and in Supabase `cv_url` for one-click access.

---

## F-3 Factory's Stage-options update is still destructive (March audit Bug 3, not fixed)

**Severity: High · Status: ✅ RESOLVED via concurrency guard (confirmed by author, June 2026).**

**Location:** [scripts/factory_worker.py:341-344](../scripts/factory_worker.py) (the destructive
write), guarded by [factory_worker.py:255-260](../scripts/factory_worker.py).

`configure_process()` sets the Workflow DB's `Stage` select by **replacing the entire option set**
(`{"Stage": {"select": {"options": stage_options}}}`). In the March 2026 incident this wiped live
options like "0.4 On Hold" / "0.5 Back-up" and Notion silently remapped candidates sitting on those
stages.

**Why it's now safe:** the destructive write is only reachable on a process's *first*
configuration. The guard at `factory_worker.py:255-260` calls `get_process_by_name()` and
**returns early** for any process already registered in Supabase — so a re-run (whether from the
hourly safety net or a manual re-trigger) never re-executes the Stage-options replacement. The fix
is the guard, not a change to the write itself.

> ⚠️ **Do not "tidy up" the `get_process_by_name` early-return** at `factory_worker.py:255-260` —
> it is load-bearing. Removing or weakening it reopens this defect. The one remaining window is the
> [F-6](#f-6-factorys-destructive-notion-writes-have-no-rollback-on-supabase-failure) scenario
> (Notion writes succeed, Supabase registration fails → not yet registered → retry re-runs the
> destructive write); in that window the options are re-derived from the same guidelines, so it is
> idempotent unless the guidelines table changed between attempts.

---

## F-4 No initial history entry when an application is created (March audit Bug 2, not fixed)

**Severity: Low · Status: ⚠️ Open (known, deliberately not fixed).**

**Location:** [core/supabase_client.py:187](../core/supabase_client.py) (`create_application`
inserts `current_stage` but never writes a `NzymeRecruitingProcessHistory` row).

The stage-transition audit log only records *transitions*. The initial stage a candidate enters at
has no history row, so "when did this candidate first appear at stage X" is unanswerable for the
first stage from the history table alone. The March audit recommended writing an initial history
row on creation; this was not implemented.

**Recommendation:** if/when stage analytics need a complete audit trail, add a single history
insert in `create_application` with `from_stage = None, to_stage = initial_stage`. Low urgency.

---

## F-5 Storage client's CV download has no timeout

**Severity: Medium · Status: ⚠️ Open.**

**Location:** [core/storage_client.py:31](../core/storage_client.py)
(`response = httpx.get(notion_url)` — no `timeout=` argument).

When `upload_cv_from_url` downloads a CV from Notion's temporary file URL, the `httpx.get` call has
no timeout. If Notion's file CDN hangs, this call blocks until the **300 s Lambda limit** kills the
whole invocation — taking down every other candidate in that batch, not just the slow one. Every
other external call in the codebase that matters either sets a timeout or runs inside the Observer's
`_api_request` retry wrapper; this one was missed.

**Recommendation:** add `timeout=30` (or similar) to the `httpx.get`, and treat a timeout the same
as a failed download (return `None` → candidate becomes AI-pending and is retried later).

---

## F-6 Factory's destructive Notion writes have no rollback on Supabase failure

**Severity: Medium · Status: ⚠️ Open (mitigated by idempotency).**

**Location:** [scripts/factory_worker.py:333-420](../scripts/factory_worker.py) — all the DB
renames, Stage-option writes, and content writes happen *before* `register_process()` (~line 400);
the dashboard page is only marked `Processed=true` on Supabase success.

Notion has no transactions. If every Notion write succeeds but the Supabase `register_process()`
call then fails, the dashboard page stays `Processed=false`, so the next safety-net run re-executes
**all** the destructive Notion configuration — including the [F-3](#f-3-factorys-stage-options-update-is-still-destructive-march-audit-bug-3-not-fixed)
Stage-options replacement (and at that point the process is *not* yet registered, so the F-3 guard
doesn't protect it).

**Why it's mostly fine:** the re-run re-derives the same values from the same template/guidelines,
so it's idempotent *unless the guidelines changed between attempts*. The exposure is a narrow
partial-failure window.

**Recommendation:** register in Supabase *first* (or make the Notion configuration idempotent by
merging Stage options instead of replacing — which would also fully close F-3).

---

## F-8 Bulk import auto-tags every candidate as Headhunter

**Severity: Low · Status: ⚠️ Open (by design — revisit if the assumption changes).**

**Location:** [scripts/harvester.py:428](../scripts/harvester.py) — the bulk splitter hardcodes
`PROP_HEADHUNTER: {"checkbox": True}` on every Form entry it creates.

Every candidate produced by splitting a bulk upload is unconditionally flagged `Headhunter=true`,
which later drives Source attribution to `"Headhunter - {firm}"`. This encodes the assumption that
**all bulk uploads are headhunter-sourced**. If a non-headhunter bulk upload ever happens, those
candidates get a wrong Source tag.

**Recommendation:** if bulk uploads start coming from non-headhunter channels, make the flag a
per-upload choice rather than a hardcoded `True`. See Source Attribution rules in
[.claude/rules/architecture.md](../.claude/rules/architecture.md).

---

## F-9 Identity by name merge can silently combine different people

**Severity: High · Status: ⚠️ Open (by design — deliberate tradeoff, see [D-5](technical/06_decision_log.md)).**

**Location:** [core/supabase_client.py:338](../core/supabase_client.py)
(`resolve_candidate_identity`), and the direct-entry path in
[scripts/harvester.py](../scripts/harvester.py) (`_process_direct_candidate_inner`, which logs a
`[DIRECT]` WARNING on a name-only merge).

The 4-rule identity engine merges on **name match when there's no email conflict** — so two
different people with the same name (and at least one missing an email) merge into one candidate
record. This is a deliberate bet (D-5): optimize for *not creating duplicates* at the cost of *rare
wrong merges*, because real CVs often lack reliable email and email-only matching would spawn
duplicates whenever a candidate enters through two channels.

**Why it's dangerous when it does happen:** a wrong merge is silent and hard to undo — governance
lists, process history, and experience tags all comingle. The name-only-merge WARNING is logged but
nobody watches the logs.

**Recommendation:** if wrong merges become a real problem, add a CloudWatch metric/alarm on the
`[DIRECT]` name-merge WARNING, or introduce a lightweight review state for name-only merges.

---

## F-10 Concurrent Source multi-select append is a read-modify-write race

**Severity: Low · Status: ⚠️ Open.**

**Location:** the Harvester reads existing Source tags off the Main DB page
(`_read_existing_source_tags` in [scripts/harvester.py](../scripts/harvester.py)) and
`NotionBuilder.build_candidate_payload()` appends to them. Read-then-write, no locking.

Source is a multi-select that's *appended* to (never overwritten) so tags accumulate across
ingests. But the append is read-modify-write against Notion with no concurrency control: two
overlapping ingests for the same candidate (e.g. webhook + EventBridge) can both read the same
prior tag set and the second write can clobber a tag the first added. Low probability and low
impact (a lost Source tag), which is why it's untouched.

**Recommendation:** accept it, or serialize Source writes per candidate. Not worth infrastructure.

---

## F-11 ZWSP prefixes on stage names are a hidden invariant

**Severity: Medium · Status: ⚠️ Open (deliberate mechanism, see [D-12](technical/06_decision_log.md)).**

**Location:** [scripts/factory_worker.py:329-331](../scripts/factory_worker.py)
(`ZWSP = chr(0x200B); stage["name"] = ZWSP * i + stage["name"]`).

Stage option names carry N leading zero-width spaces (U+200B) by index, so Notion's alphabetical
`select` sort yields lifecycle order without a second visible number. The consequence: stage-name
strings contain **invisible characters**. Any `==` comparison against a string copied from logs,
typed by hand, or read from a different source will silently fail. This was a contributing factor
in the March 2026 incident (a Supabase `current_stage` of `"0.1 Identified"` *without* ZWSPs didn't
match the Notion value *with* ZWSPs, so the Observer logged spurious "transitions").

> ⚠️ **If you ever compare a stage name to a literal, normalize first** (strip U+200B) on both
> sides. The ZWSPs are real data, not display formatting.

**Recommendation:** if migrating off this, see D-12's reversibility note — strip ZWSPs from Supabase
+ Notion, stop adding them in the Factory, in one coordinated PR.

---

## F-12 find_child_database silently returns None past depth 4

**Severity: Low · Status: ⚠️ Open.**

**Location:** [core/notion_client.py:218](../core/notion_client.py) (`find_child_database`, BFS
hard-capped at `max_depth = 4`).

The breadth-first search for a child database gives up past 4 levels of nesting and returns `None`,
which every caller treats as "the DB doesn't exist." A child DB nested one toggle deeper than
expected silently becomes invisible to the code rather than raising. 4 is deep enough for the
current Notion templates, but it's an undocumented assumption about template structure.

**Recommendation:** if a template is ever restructured deeper, this is the first place to look when
a child DB "disappears." Consider logging a warning when the depth cap is hit (vs. silently
returning None).

---

## Additional findings (June 2026 handover)

These two were surfaced during the handover code review, not the original audit. They aren't yet
cross-referenced from elsewhere in the docs.

### F-13 Identity OR-filter is built by unescaped string interpolation

**Severity: Medium · Status: ⚠️ Open (rationale not confirmed at handover).**

**Location:** [core/supabase_client.py:143-150](../core/supabase_client.py) (the `or_(...)` filter is
built as `f"notion_page_id.eq.{notion_page_id},email.eq.{email_clean}"`); same pattern in
`resolve_process_by_notion_db_id` (~line 505).

The email (only `.strip()`ed, not escaped) is interpolated directly into PostgREST `or_` filter
syntax. PostgREST uses `,` as a logical separator and `()` for grouping inside `or_`, so an email
or value containing those metacharacters could break the filter or alter its matching — a
correctness/injection hazard. In practice emails rarely contain `,` or `)`, which is presumably why
it's gone unnoticed, but the author did not confirm the input is guaranteed clean.

**Recommendation:** validate/escape the value before interpolation, or use the client's structured
filter builders instead of an f-string. Low effort.

### F-14 Central-reference handler is all-or-nothing → duplicates on retry

**Severity: Low–Medium · Status: ⚠️ Open (rationale not confirmed at handover).**

**Location:** [scripts/observer.py:458](../scripts/observer.py) (`_handle_central_reference`), with
the `global_success` gate around lines 526-552.

`_handle_central_reference` only marks the reference page `Processed=true` if **every** per-workflow
write succeeds. If one write fails partway, `Processed` is never set, so the whole reference
re-runs on the next sweep — **re-creating the reference rows that already succeeded** the first
time. There's no per-page dedup/idempotency check.

**Recommendation:** make reference creation idempotent (check-then-create per workflow page) or mark
progress per-page rather than all-or-nothing. Until then, a partial failure produces duplicate
reference entries that need manual cleanup.

---

## Related: the March 2026 incident report

Findings F-3 and F-4 originate in a detailed forensic report (`docs/audit_march_2026.md`, recoverable
via `git show HEAD:docs/audit_march_2026.md`). That report uses the pre-refactor **Spanish method
names** (`crear_aplicacion`, `procesar_candidato`, `registrar_cambio_stage`, …) which no longer
exist in the codebase — the methods were renamed to English (`create_application`,
`process_candidate`, `register_stage_change`, …). Read it for the timeline and root-cause technique,
but map the names forward.
