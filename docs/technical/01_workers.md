# 01 — The Three Workers

Each worker is a class instantiated lazily in `main_lambda.lambda_handler` only when its trigger fires. Workers share the same client dependencies — `NotionClient`, `SupabaseManager`, `StorageClient`, `CVAnalyzer`, optional `ExaClient` — and receive them via constructor injection.

This doc covers each worker in the same shape: **entry points → what it reads → what it writes → idempotency guards → failure modes**.

> ⚠️ **Line numbers in `harvester.py`/`observer.py` citations are approximate.** These two files
> are large and churn often, so the cited line can drift by tens of lines after edits. The method
> *names* are accurate — if a citation lands in the wrong place, grep for the named method
> (`def <name>`) rather than trusting the number. `factory_worker.py` and `supabase_client.py`
> citations are kept current.

---

## Factory ([scripts/factory_worker.py](../../scripts/factory_worker.py))

### Purpose
Sets up a new recruiting process. One Process Dashboard page → one set of child Notion DBs (Workflow, Form, Bulk, Feedback) + one row in `NzymeRecruitingProcesses` Supabase table.

### Entry points
| Entry | Defined at | Invoked from |
|---|---|---|
| `run_once()` | [factory_worker.py:464](../../scripts/factory_worker.py) | EventBridge `task=factory` schedule (hourly safety net) |
| `run_from_webhook(page_id)` | [factory_worker.py:455](../../scripts/factory_worker.py) | Webhook with `HANDLER_PROCESS_DASHBOARD` and feature flag `WEBHOOK_PROCESS_DASHBOARD_ENABLED=true` |

Both entries converge on `_process_dashboard_page(page)` ([factory_worker.py:422](../../scripts/factory_worker.py)), which is the shared core.

### What it reads
- The Process Dashboard page's `properties` — specifically `Name`, `Process Type` (select), `Process Visibility` (select), `Governance: Edit & View Access` (people), `Headhunter` (relation to Headhunters DB).
- The Process Dashboard data source's `templates` endpoint at `/data_sources/{ds}/templates` ([factory_worker.py:49](../../scripts/factory_worker.py)) to fuzzy-match the `Process Type` against template names prefixed `"PROCESS TEMPLATE - "`.
- The page's child blocks (after template apply) to discover the four child databases by title substring (`"workflow"`, `"form"`, `"bulk"|"import"`, `"feedback"`) and two child pages (`"job"|"role"`, `"interview stages"`).
- The matching guidelines document for this `Process Type` from the Guidelines DB (env: `NOTION_GUIDELINES_DB_ID`) — both the Interview Stages page (for stage parsing + assessment characteristics) and the Role & Candidate Description page (for JD content cloning).
- The default template of the Workflow DB, drilling for a `"Past Experience"` child DB (the AI matrix definitions).

### What it writes
**Notion (via `update_database`, `update_page`, `update_data_source`, `append_block_children`, the `template` PATCH):**
- Sets `Open/Closed = "Open"` on the Process Dashboard page (right after the template PATCH, [factory_worker.py:88](../../scripts/factory_worker.py)).
- Renames the four child DBs to `"Feedback Tool & Workflow - {process_name}"`, `"Single Candidate Application Upload Form - {process_name}"`, `"Bulk Candidate Application Upload Form - {process_name}"`, `"Bulk & Single Feedback Upload Form - {process_name}"`.
- Replaces the Workflow DB's `Stage` select options with the parsed stages from guidelines, with zero-width-space prefixes for ordering ([factory_worker.py:329-331](../../scripts/factory_worker.py) — see [F-11](../handover_audit.md#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant)).
- Clones JD content and Interview Stages content (chunked at 100 blocks) into the corresponding child pages.
- Sets `Processed [Do not touch] = true` on the Dashboard page **only if** Supabase registration succeeded.

**Supabase:**
- Inserts a row into `NzymeRecruitingProcesses` via `register_process` ([supabase_client.py:24](../../core/supabase_client.py)) with: process_name, process_type, notion_workflow_id, notion_form_id, notion_bulk_id, notion_feedback_id, status=`"Open"`, matrix_characteristics (JSONB), assessment_characteristics (JSONB), is_confidential, governance_people (JSONB), headhunter_name. Duplicate-key violation on `notion_workflow_id` is treated as success.

### Idempotency guards
1. **Notion checkbox guard** ([factory_worker.py:432-434](../../scripts/factory_worker.py)) — `_process_dashboard_page` skips if `Processed [Do not touch]` is already true.
2. **Page-has-child-DBs guard** ([factory_worker.py:442-451](../../scripts/factory_worker.py)) — if the page already has `child_database` blocks, the template PATCH is skipped (because re-applying a template would duplicate content). `configure_process` still runs to retry the post-template setup.
3. **Supabase guard** ([factory_worker.py:256-260](../../scripts/factory_worker.py)) — `configure_process` checks `get_process_by_name(process_name)` first. If the process already exists in Supabase, mark Dashboard `Processed=true` and return without mutating anything.
4. **Child-DB retry** ([factory_worker.py:270-305](../../scripts/factory_worker.py)) — finding the Workflow + Form child DBs after template apply is retried up to 4 times with 8-10s sleeps, accommodating Notion's async template instantiation.

### Failure modes
- **Supabase succeeds but Dashboard `Processed=true` fails** ([factory_worker.py:417](../../scripts/factory_worker.py)) — Dashboard page reappears in `find_pending_requests` on the next safety-net run. The Supabase guard at line 256 catches this on retry and just sets the checkbox.
- **Notion writes succeed but Supabase fails** — Dashboard `Processed` stays false; on retry, the Supabase guard misses (no row), the destructive Notion ops re-run. See [F-6](../handover_audit.md#f-6-factorys-destructive-notion-writes-have-no-rollback-on-supabase-failure).
- **Child DBs never appear within 4 retries** ([factory_worker.py:308-310](../../scripts/factory_worker.py)) — `[CRITICAL]` log, return without registering. Page remains pending forever. Manual intervention: re-apply the template, or delete the page and recreate it.
- **No template matches `Process Type`** ([factory_worker.py:445-448](../../scripts/factory_worker.py)) — error logged, page aborted. Cause: a new `Process Type` option was added without a corresponding `PROCESS TEMPLATE - {suffix}` page.

### `run_once()` vs the webhook path
| Aspect | `run_once()` | `run_from_webhook(page_id)` |
|---|---|---|
| Discovery | Queries Process Dashboard with `Processed=false AND Process Type is_not_empty` ([factory_worker.py:206-217](../../scripts/factory_worker.py)) | Receives `page_id` directly from the webhook router |
| Batch size | All pending pages in the result | Exactly 1 |
| Schedule | Hourly | Within seconds of page creation |
| Use case | Safety net for missed webhooks | Primary path |

---

## Harvester ([scripts/harvester.py](../../scripts/harvester.py))

### Purpose
Ingest new candidates into the system. Process CVs through OpenAI, resolve identity, write to both Notion Main DB and Supabase.

### Entry points
| Entry | Defined at | Invoked from |
|---|---|---|
| `run_once()` | [harvester.py:1404](../../scripts/harvester.py) | EventBridge `task=harvester` (every 10 min) |
| `process_single_from_webhook(page_id, process_context)` | [harvester.py:1027](../../scripts/harvester.py) | Webhook with `HANDLER_FORM_SUBMISSION` (Form DB entry created) |
| `process_bulk_imports(processes)` | [harvester.py:358](../../scripts/harvester.py) | Webhook with `HANDLER_BULK_SUBMISSION` (Bulk DB entry created), and as Step 1 of `run_once` |

`process_candidate` ([harvester.py:448](../../scripts/harvester.py)) is the shared candidate-processing core, called from both `run_once`'s Step 2 and `process_single_from_webhook`.

### What it reads
- **`NzymeRecruitingProcesses`** — all active (`status="Open"`) processes for `run_once`, or the specific process passed in `process_context` for the webhook path.
- **Workflow DB** — pages with `Processed=false AND ID is_not_empty` (standard processing) or `Processed=false AND ID is_empty AND Name is_not_empty` (direct entry, Step 2.5).
- **Form DB** — the auxiliary Form entry matching the Workflow page's `ID` field. Pulls `Name`, `Email`, `LinkedIn`, `Headhunter` checkbox, and the CV file.
- **Bulk DB** — pages with `Processed=false`. Each row holds a `CVs` files property with multiple PDFs; the splitter creates one Form-DB entry per file.
- **Main DB** — when reading existing candidates for identity merge (e.g., `_read_existing_source_tags`).
- **Workflow page's child blocks** — to find the `Past Experience [AI-generated]` matrix DB ([harvester.py:674](../../scripts/harvester.py)) and the `Process Outcome Form` DB ([harvester.py:626](../../scripts/harvester.py)).
- **Workflow page's CV** — fallback for AI-pending reprocessing when the Main DB page has no CV ([harvester.py:777-797](../../scripts/harvester.py)).

### What it writes
**Notion:**
- Workflow page: `Processed=true` (always, in `finally`), `Name` (corrected from AI), `CV` file URL (Supabase Storage public URL), `Stage` (initial), `Candidate Relation` (back to Main DB).
- Main DB: full candidate payload via `NotionBuilder.build_candidate_payload` — 30+ properties including experience tags by sector, education, languages, process history, Source multi-select append, Governance people property.
- `Past Experience [AI-generated]` child DB on the workflow page: per-row `AI Score` and `AI Comments`.
- Bulk import: creates new Form DB entries, hardcoded `Headhunter=true` for every file ([harvester.py:350](../../scripts/harvester.py) — see [F-8](../handover_audit.md#f-8-bulk-import-auto-tags-every-candidate-as-headhunter)).

**Supabase:**
- `NzymeTalentNetwork` via `manage_candidate` — SQL columns (name, email, phone, linkedin_url, cv_url, source, creator, assessment) + `candidate_data` JSONB.
- `NzymeRecruitingApplications` via `create_application` — candidate_id, process_id, notion_page_id, current_stage, status, notion_outcome_id (later, via `update_application_outcome_id`).
- **Not**: `NzymeRecruitingProcessHistory` (see [F-4](../handover_audit.md#f-4-no-initial-history-entry-when-an-application-is-created-march-audit-bug-2-not-fixed)).

**Supabase Storage:**
- `resumes` bucket — CV uploaded via `storage_client.upload_cv_from_url`, filename `{int(time.time())}_{safe_name}`.

### Idempotency guards
1. **Workflow page `Processed` checkbox** — set in `finally` of `process_candidate` ([harvester.py:386-390](../../scripts/harvester.py)) and `_process_direct_candidate` ([harvester.py:1054-1058](../../scripts/harvester.py)). Always set, even on partial failure.
2. **Application existence check before processing** ([harvester.py:374-378](../../scripts/harvester.py)) — `get_application_by_notion_id` short-circuits if another invocation already processed this workflow page.
3. **Second check before AI** ([harvester.py:411-413](../../scripts/harvester.py)) — narrows the TOCTOU window where two invocations both pass guard 2.
4. **`create_application` existence check** ([supabase_client.py:197-203](../../core/supabase_client.py)) — INSERTs only if no `(candidate_id, process_id)` row exists; returns the existing app ID otherwise.
5. **`manage_candidate` duplicate-catch** ([supabase_client.py:174-183](../../core/supabase_client.py)) — UNIQUE on `notion_page_id` catches concurrent inserts; fetches the winning row.
6. **Skeleton guard** ([harvester.py:536-558](../../scripts/harvester.py)) — when the new candidate has no AI data (CV failed) and the existing Main DB page already has experience data, the experience properties are removed from the payload to avoid overwriting good data with skeleton blanks.

### Three processing paths
`_process_candidate_inner` ([harvester.py:392](../../scripts/harvester.py)) chooses between three paths:

| Path | Trigger | AI source | `AI Pending` set? |
|---|---|---|---|
| **A. Full CV** | Form has CV file | OpenAI `process_cv(local_path, matrix_characteristics)` | No (unless AI itself fails) |
| **B. LinkedIn fallback** | No CV, Form has `linkedin_url`, `ExaClient` configured | Exa fetches profile → OpenAI `process_linkedin(text, matrix)` | No (unless both fail) |
| **B'. Minimal skeleton** | No CV and no LinkedIn (or both failed) | None — uses `_create_minimal_candidate_data(form_data)` | Yes |

Failure on AI (e.g., OpenAI 429 rate limit) sets `AI Pending=true`; reprocessing picks it up later — see [04_ai_pipeline.md](04_ai_pipeline.md) for details.

### Step 2.5: Direct entry processing
`_process_direct_candidates` ([harvester.py:1101](../../scripts/harvester.py)) handles candidates added *directly* to a Workflow DB without going through a Form. The filter is the complement of standard processing (`ID is_empty AND Name is_not_empty`). The processing flow mirrors `process_candidate` but:
- Reads `name` and `CV` from the Workflow page itself, not from a Form entry.
- Source attribution is **not written** by code for direct entries (was previously `"Direct Entry - {creator}"` but that path is now manual).
- Higher duplicate risk on name-only identity resolution because email is often missing (see [F-9](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people)).
- Logs include `[DIRECT]` prefix for grep-ability.

### Failure modes
- **CV download/upload fail** — function returns `None, None, False` ([harvester.py:236, 242](../../scripts/harvester.py)) → `process_candidate` returns without writing, `finally` marks Processed=true. The workflow page is closed but no Supabase row exists. Manual remediation: uncheck `Processed`, re-attach CV, wait for next sweep.
- **AI parse fails (429, malformed JSON, etc.)** — skeleton record created with `AI Pending=true`. Both Notion and Supabase get partial data; reprocessing picks it up.
- **Notion `update_page` returns non-200** — logged with `Notion {update|create} FAILED`, but the function returns and `finally` marks Processed. The Supabase write below the `if res_op.status_code != 200: ... return` is skipped — so Supabase has nothing while Notion has stale state.
- **Lambda timeout during bulk split** ([harvester.py:361](../../scripts/harvester.py) — 10s sleep per file) — files past the timeout point are never split; batch row gets marked `Processed=true` at the end of the loop *only if the loop completes*. On timeout mid-loop, the batch row is left `Processed=false` and the next run will re-split from scratch, creating duplicate Form entries for already-split files.

### `run_once()` vs the webhook path
| Aspect | `run_once()` | `process_single_from_webhook` |
|---|---|---|
| Step 1 | `process_bulk_imports(all active processes)` | Skipped |
| Step 2 | Standard candidate scan across all processes (capped at `MAX_CVS_PER_RUN=15`) | One process only |
| Step 2.5 | Direct-entry candidates across all processes | Skipped |
| Step 3 | `_reprocess_ai_pending()` (top 5 pending) | Skipped |
| Triggering page | None (scheduled scan) | The Form DB page that triggered the webhook |
| Source of `process_context` | Iterate `get_active_processes()` | Passed in from the router (Supabase lookup) |

---

## Observer ([scripts/observer.py](../../scripts/observer.py))

### Purpose
Watch for state changes after ingestion. The largest worker by far — 1667 lines — because it owns the variety of post-ingestion events: stage transitions, outcome processing, feedback ingestion, AI-pending reprocessing on late-arriving CVs, dispatch between processes, feedback assessment generation, process status sync, governance restoration on process close.

### Entry points
| Entry | Defined at | Invoked from |
|---|---|---|
| `run_once()` | [observer.py:1649](../../scripts/observer.py) | EventBridge `task=observer` (every 10 min, +3 offset) |
| `handle_webhook_event(handler_name, page_id, process_context=None)` | [observer.py:1596](../../scripts/observer.py) | Any of six webhook handlers (see table) |

The webhook map ([observer.py:1600-1607](../../scripts/observer.py)) routes to:
- `HANDLER_MAIN_CANDIDATE` → `_handle_main_candidate`
- `HANDLER_PROCESS_DASHBOARD` → `_handle_process_dashboard` (status sync)
- `HANDLER_CENTRAL_REFERENCE` → `_handle_central_reference`
- `HANDLER_WORKFLOW_ITEM` → `_handle_workflow_item`
- `HANDLER_FEEDBACK_FORM` → `_handle_feedback_form`
- `HANDLER_OUTCOME_FORM` → `_handle_outcome_entry`

### Two surveillance engines
`run_once` uses two scanning patterns:

**Sniper** ([observer.py:112](../../scripts/observer.py)) — direct query on a known data source ID with a `last_edited_time OR created_time` filter for the last `LOOKBACK_MINUTES` (default 11). Used for: Main DB, Process Dashboard, Central References, every active process's Workflow DB, every active process's Feedback DB.

**Radar** ([observer.py:155](../../scripts/observer.py)) — search Notion's `/v1/search` for data sources by exact-name match, then for each matching data source walk up to find the candidate-page ancestor (the page that has a `Stage` property) and process unprocessed rows. Used only for `"Process Outcome Form"` — because Outcome Forms are per-application child DBs, not per-process, and there's no central registry of their IDs (though `notion_outcome_id` on `NzymeRecruitingApplications` partially provides one — see Outcome Form path below).

### What it reads (per handler)

| Handler | Notion reads | Supabase reads |
|---|---|---|
| `_handle_main_candidate` | Main DB page properties (Assign to Active Process relation, AI Pending checkbox, CV files, LinkedIn URL, processed checkbox, all candidate properties) | `get_candidate_by_notion_page_id`, `get_applications_by_candidate_id`, `get_process_by_name` (during dispatch) |
| `_handle_process_dashboard` | Dashboard page Name, Open/Closed select | `get_process_by_name` |
| `_handle_workflow_item` | Workflow page `Assessment Requested`, `Stage`, process_context's `assessment_characteristics` | `get_application_by_notion_id` |
| `_handle_feedback_form` | Feedback form Name, File attachments | `resolve_candidate_identity`, applications join |
| `_handle_central_reference` | Reference page properties (Candidate Email, Candidate Name, Referrer fields, Context, Relationship, Timing, Reference Outcome) | `resolve_candidate_identity`, applications query |
| `_handle_outcome_entry` | Outcome Form Discarded/Disqualified/Lost select, Explanation, Processed checkbox; parent's Stage schema for fuzzy match | `get_outcome_context`, `update_rejection_reason` |

### What it writes (per handler)

| Handler | Notion writes | Supabase writes |
|---|---|---|
| `_handle_main_candidate` | Main DB enriched payload (CV → AI → all properties) OR dispatch (creates Form DB entry, clears `Assign to Active Process` relation) | `manage_candidate` (always, as fallback sync), `update_candidate_email` (on email backfill) |
| `_handle_process_dashboard` | None | `update_process_status_by_name`, and on close of confidential process: `_handle_confidential_process_close` updates governance + history |
| `_handle_workflow_item` | When assessment requested: new "Feedback Assessment [AI-generated]" page in Gathered Feedback child DB, with summary + scored matrix; unchecks `Assessment Requested`. Otherwise: nothing direct (Supabase stage update). | `register_stage_change` (optimistic-locked) |
| `_handle_feedback_form` | Workflow page `Headhunter's Feedback` file; new page in `Gathered Feedback` child DB with parsed markdown; Feedback form `Processed=true` | `headhunter_feedback_url` on application row |
| `_handle_central_reference` | New page in each active workflow's `Candidate References [Input here feedback received]` child DB; Reference page `Processed=true` | `update_candidate_email` on backfill |
| `_handle_outcome_entry` | Workflow page `Stage` (fuzzy-matched), `Next Steps` (cleared); new page in Confidential Assessments DB with relation to Main DB; Outcome page `Processed=true` (in `finally`) | `update_rejection_reason` |

### Idempotency guards
1. **Outcome Processed checkbox** ([observer.py:548-549](../../scripts/observer.py)) — guard at top, then `try/finally` always sets `Processed=true`.
2. **Optimistic lock on stage change** ([supabase_client.py:243-257](../../core/supabase_client.py)) — `UPDATE ... WHERE current_stage=old_stage`. If a concurrent invocation already applied the transition, the WHERE matches zero rows and the history insert is skipped.
3. **Feedback Assessment dedup** ([observer.py:702-710](../../scripts/observer.py)) — scans existing pages in Gathered Feedback child DB for any titled `"AI-generated"`; skips and unchecks `Assessment Requested` if found.
4. **Reference Processed checkbox** ([observer.py:447](../../scripts/observer.py)) — early return if already processed; only marks Processed after all per-application writes succeed (`global_success` flag).
5. **Feedback Form Processed checkbox** ([observer.py:332](../../scripts/observer.py)) — early return; set to true at the end ([observer.py:439](../../scripts/observer.py)) regardless of partial success per file.

### Failure modes
- **Identity not resolved on feedback** ([observer.py:373-375](../../scripts/observer.py)) — feedback file is logged and skipped, but the Feedback form still gets `Processed=true` at the end of the outer loop. Lost feedback. Manual recovery: edit the Feedback form to set the candidate name correctly, uncheck Processed, re-trigger.
- **Outcome stage not found in schema** ([observer.py:1546-1548](../../scripts/observer.py)) — `_fuzzy_match_stage` returns the input string unchanged; the `update_page` write will fail because the value isn't an option in the Stage select. The error is logged; Outcome Form gets `Processed=true` anyway (the `finally` block).
- **AI returns no data on feedback PDF** ([observer.py:364-366](../../scripts/observer.py)) — that file is skipped but the outer Feedback Form gets `Processed=true`.
- **Confidential Assessment creation fails after stage updated** ([observer.py:608-653](../../scripts/observer.py)) — exception caught and logged inside `_create_confidential_assessment`; the outer outcome processing continues (stage already updated, rejection reason already saved). Resulting state: candidate is correctly rejected in Notion/Supabase, but no Confidential Assessment page exists. Manual recovery: create the page by hand from the Confidential DB template.
- **Late-arriving CV on AI Pending** — `_logic_reprocess_ai_pending` reads CV from the Main DB page first; if absent, falls back to the Workflow page CV. If neither has a CV but AI Pending is true, it just logs `Could not parse CV or LinkedIn` and returns — AI Pending stays true, will retry on next sweep.

### `run_once()` vs the webhook path
| Aspect | `run_once()` | `handle_webhook_event` |
|---|---|---|
| Triggers | Sniper over Main DB, Dashboard, References, every Workflow DB, every Feedback DB; Radar for Outcome Forms | Single page ID, single handler |
| Lookback | `LOOKBACK_MINUTES=11` filter on `last_edited_time` / `created_time` | None — webhook says exactly which page changed |
| Outcome handling | Radar engine walks ancestor to find candidate page | `process_context` includes `candidate_id` (the application's workflow page ID, resolved by the router from `notion_outcome_id`) |
| Use case | Catches everything the webhook missed; covers Outcome Forms whose IDs aren't yet stored on the application row | Primary path for everything except Outcome Forms on newly-created processes |

---

## Cross-worker interactions

Three places where workers depend on each other or on shared state:

1. **AI Pending reprocessing exists in both Harvester and Observer.**
   - `Harvester._reprocess_ai_pending` ([harvester.py:801](../../scripts/harvester.py)) — runs on EventBridge sweeps; queries Notion Main DB for `AI Pending=true`, batches top 5, re-runs `process_cv`, updates Notion + Supabase, fills strategic assessments on workflow pages.
   - `Observer._logic_reprocess_ai_pending` ([observer.py:935](../../scripts/observer.py)) — runs when a Main DB candidate page is edited (e.g., a recruiter uploads a CV to an existing AI-pending candidate); re-runs `process_cv` or `process_linkedin`, updates Notion + Supabase, fills strategic assessments across all active workflow pages.

   These two functions duplicate a lot of logic (downloading CV, calling AI, updating ~25 Notion properties, merging JSONB). They drift over time. The Observer version handles "CV added later by user" while the Harvester version handles "OpenAI was down at ingest time, try again later".

2. **`_handle_process_dashboard` calls Factory and Observer in sequence** ([main_lambda.py:88-103](../../main_lambda.py)) — when a Dashboard page changes, both workers run: Factory to (potentially) configure a new process, Observer to sync Open/Closed status to Supabase.

3. **Source attribution is read-modify-write** — the Harvester's main path appends new Source tags to the existing Main DB page's tag list, which means concurrent ingests on the same candidate can lose data (see [F-10](../handover_audit.md#f-10-concurrent-source-multi-select-append-is-a-read-modify-write-race)).
