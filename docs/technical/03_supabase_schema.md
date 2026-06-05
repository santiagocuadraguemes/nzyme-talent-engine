# 03 — Supabase Schema

Supabase is the system of record. Notion is the UI; Supabase is the durable, queryable backend. Four tables hold all the structured state, plus one Storage bucket for blob (CV) data.

Project ID: `yphbrpbwpakjduhmoimw` (referenced in [.claude/rules/testing.md](../../.claude/rules/testing.md)).

The schema is not stored in a migration file in this repo — it lives in Supabase's project state and is modified through the Supabase UI or `mcp__supabase__apply_migration`. The descriptions below are reconstructed from `core.supabase_client` query/update patterns. **Verify in Supabase before any schema change.**

---

## NzymeTalentNetwork — Candidate profiles

The Main DB's Supabase mirror. One row per real human (after identity resolution).

### Columns (SQL)

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | Default `gen_random_uuid()` |
| `name` | text | Set on insert + UPDATE in `manage_candidate` |
| `email` | text | UNIQUE conceptually, but **not** a DB constraint (verified by grep — only `notion_page_id` has UNIQUE) |
| `phone` | text | Often null |
| `linkedin_url` | text | Often null |
| `cv_url` | text | Supabase Storage public URL (`https://yphbrpbwpakjduhmoimw.supabase.co/storage/v1/object/public/resumes/{key}`) |
| `notion_page_id` | text | **UNIQUE constraint** — see [supabase_client.py:174-183](../../core/supabase_client.py) handles violation |
| `source` | text | Flat string, written *only on first ingest* (Notion's Source multi-select is authoritative for updates — [supabase_client.py:157-159](../../core/supabase_client.py) drops `source` from payload on UPDATE) |
| `creator` | text | Set from Notion's `Creator` field via `NotionParser.parse_candidate_properties`; users manage this in Notion |
| `assessment` | text | Set from Notion's `Assessment` select |
| `candidate_data` | JSONB | The big bag — see schema below |
| `updated_at` | timestamptz | Set to `now()` on every update |
| `created_at` | timestamptz | Default `now()` |

### `candidate_data` JSONB shape
Written by `DomainMapper.map_to_supabase_candidate` ([core/domain_mapper.py:54-122](../../core/domain_mapper.py)). Example payload:

```json
{
  "name": "Pablo Rodríguez",
  "email": "pablo@example.com",
  "linkedin_url": "https://linkedin.com/in/pablo-rodriguez",
  "total_years_range": "10-15 Years",
  "languages": ["Spanish", "English", "French"],
  "recruiting_processes_history": ["NZ Tech Lead 2026Q1", "PC RB Solutions CEO 2026Q1"],
  "proposed_teams_roles": ["Tech - Lead", "PortCo - CEO"],
  "general": {
    "international_locations": ["Spain", "United Kingdom"],
    "industries_specialized": ["Tech", "Energy"]
  },
  "education": {
    "bachelors": ["Engineering"],
    "masters": ["Business"],
    "university": ["MIT", "IE Business School"],
    "mba": ["IE Business School"]
  },
  "experience": {
    "consulting": {
      "companies": ["McKinsey", "BCG"],
      "roles": [],
      "years_range": "5-7 Years",
      "has_experience": true
    },
    "pe": {
      "companies": [],
      "roles": [],
      "years_range": "No",
      "has_experience": false
    },
    "finance": {
      "companies": [],
      "roles": ["CFO", "VP Finance"],
      "years_range": "3-5 Years",
      "has_experience": true
    },
    "...": "16 more experience keys"
  },

  "ai_pending": true,
  "ai_pending_cv_url": "https://.../resumes/123_cv.pdf",
  "ai_pending_process_name": "NZ Tech Lead 2026Q1"
}
```

The three `ai_pending*` keys at the bottom appear only when AI parsing failed at ingest time. They're debugging metadata — the authoritative AI-pending state lives on the Notion Main DB page's `AI Pending` checkbox ([architecture rationale](../../.claude/rules/architecture.md#ai-pending-reprocessing)). When reprocessing succeeds, the keys are deleted ([harvester.py:927-929](../../scripts/harvester.py), [observer.py:1097-1099](../../scripts/observer.py)).

### Why JSONB and not normalized columns?
The candidate has 17 experience categories (sector × functional split), each holding a small bag (companies/roles, year range, boolean). Normalizing to a `candidate_experience(candidate_id, category, years_range, has_experience)` + a separate `candidate_experience_companies` would require 34 tables of join logic to get back the shape Notion needs. The tradeoff: queries like "find candidates with PE + Finance experience" become string-match-on-JSONB instead of SQL joins — Supabase / Postgres can index JSONB with GIN, so this is performant enough for the dataset size (~thousands of candidates).

The cost shows up in two places: (a) the AI-pending reprocessing has to update large JSONB blobs without losing keys, and (b) any analytical query has to know the JSONB shape, which isn't documented anywhere outside this file.

### Reads
- `resolve_candidate_identity(email, name)` — 4-rule engine ([supabase_client.py:338-386](../../core/supabase_client.py)): email match → name exact → fuzzy name (accent-insensitive) → none.
- `get_candidate_by_notion_page_id(page_id)` — used by AI-pending reprocessing.

### Writes
- `manage_candidate(candidate_data, notion_page_id)` — UPSERT-ish: queries via OR filter on `notion_page_id` OR `email`, then UPDATE or INSERT.
- `update_candidate_email(candidate_id, new_email)` — backfill from references.

### Concurrency
- UNIQUE on `notion_page_id` catches duplicate inserts.
- No UNIQUE on `email` — see [F-9](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people) on identity-by-name merge risks.
- No UNIQUE on `name` (intentional — many people share names).

---

## NzymeRecruitingProcesses — Active recruiting processes

One row per process configured by the Factory.

### Columns

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | Default `gen_random_uuid()` |
| `process_name` | text | Matches the Process Dashboard page title verbatim |
| `process_type` | text | Matches the `Process Type` select value (e.g., `"Tech - Lead"`) |
| `notion_workflow_id` | text | **UNIQUE constraint** (`nzymerecruitingprocesses_workflow_unique`) — the only workspace-DB ID guaranteed unique per process |
| `notion_form_id` | text | Form DB on the process page |
| `notion_bulk_id` | text | Bulk DB on the process page (nullable) |
| `notion_feedback_id` | text | Feedback DB on the process page (nullable) |
| `status` | text | `"Open"` or `"Closed"` (synced by Observer's `sync_process_status`) |
| `matrix_characteristics` | JSONB | `[{"characteristic": "...", "definition": "..."}, ...]` — extracted from the `Past Experience` template at process creation |
| `assessment_characteristics` | JSONB | Same shape — extracted from the `Assessment Characteristics` child DB inside the Interview Stages guidelines page |
| `is_confidential` | boolean | Default false; set true if Process Visibility = "Confidential" |
| `governance_people` | JSONB | Array of Notion user IDs (only populated for confidential processes) |
| `headhunter_name` | text | The Headhunters DB page title at process creation time (snapshot) |
| `created_at` | timestamptz | Default `now()` |
| `updated_at` | timestamptz | Manually set to `now()` on `update_process_status_by_name` |

### Webhook routing relies on this table

The Webhook Router's dynamic registry queries this table with an OR across four ID columns:

```sql
SELECT *
FROM "NzymeRecruitingProcesses"
WHERE (notion_workflow_id = $1 OR notion_feedback_id = $1
       OR notion_form_id = $1 OR notion_bulk_id = $1)
  AND status = 'Open';
```

Only `Open` processes match — webhooks for closed processes are intentionally dropped ([supabase_client.py:498-518](../../core/supabase_client.py)). Implication: closing a process makes its Workflow DB webhooks invisible to the router. If you need to re-open processing for a closed process, flip `status` back to `Open` first.

### `matrix_characteristics` example
```json
[
  {"characteristic": "Strategic Thinking", "definition": "Ability to see the big picture..."},
  {"characteristic": "Execution", "definition": "Track record of shipping..."},
  {"characteristic": "Leadership", "definition": "Has built and led teams of 10+..."}
]
```
Used by `CVAnalyzer.process_cv(..., matrix_characteristics=...)` to build the dynamic part of the system prompt and to enforce the assessment count in the Pydantic response.

### `governance_people` example
```json
["00000000-1111-2222-3333-444444444444", "55555555-6666-7777-8888-999999999999"]
```
Array of Notion user UUIDs. Read by `Harvester.process_candidate` and `_handle_main_candidate` for confidential candidate page-level visibility.

---

## NzymeRecruitingApplications — Candidate ↔ process links

The N:M table. One row per (candidate, process) pair.

### Columns

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `candidate_id` | UUID FK → `NzymeTalentNetwork.id` | |
| `process_id` | UUID FK → `NzymeRecruitingProcesses.id` | |
| `notion_page_id` | text | **UNIQUE constraint** — this is the Workflow DB page ID |
| `current_stage` | text | Current stage option name (with ZWSP prefix from Notion) |
| `status` | text | `"Active"` (only value used today) |
| `notion_outcome_id` | text | The Outcome Form child DB's ID on the workflow page (set by Harvester after first ingest; backfillable via [tools/backfill_outcome_ids.py](../../tools/backfill_outcome_ids.py)) |
| `rejection_reason` | text | Format: `"[<stage_name>] <explanation>"` — set by `update_rejection_reason` on outcome processing |
| `headhunter_feedback_url` | text | Supabase Storage URL of the headhunter's raw feedback PDF (set on feedback ingest) |
| `created_at` | timestamptz | Default `now()` |
| `updated_at` | timestamptz | Manually set on stage change and rejection update |

### Critical gotcha: no initial history entry
When `create_application` inserts a row with `current_stage=initial_stage`, **no `NzymeRecruitingProcessHistory` row is inserted**. The history starts at the first transition. See [F-4](../handover_audit.md#f-4-no-initial-history-entry-when-an-application-is-created-march-audit-bug-2-not-fixed).

### Concurrency
- UNIQUE on `notion_page_id` catches concurrent creates ([supabase_client.py:220-225](../../core/supabase_client.py)).
- `create_application` pre-checks for existing `(candidate_id, process_id)` row to avoid overwriting `current_stage` on duplicate creates ([supabase_client.py:197-203](../../core/supabase_client.py)) — this is the fix for the March 2026 audit's Bug 1.
- Stage updates use optimistic locking: `WHERE current_stage=old_stage` (see next section).

---

## NzymeRecruitingProcessHistory — Stage transition audit log

Append-only log. One row per *successful* stage transition.

### Columns

| Column | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `application_id` | UUID FK → `NzymeRecruitingApplications.id` | |
| `from_stage` | text | Nullable (would be null on initial entry — but as noted, the initial entry is never written today) |
| `to_stage` | text | |
| `timestamp` | timestamptz | Default `now()` |

### How it's written
Only `register_stage_change` ([supabase_client.py:239-262](../../core/supabase_client.py)) inserts here. The function uses optimistic locking:

```python
res = self.client.table("NzymeRecruitingApplications").update({
    "current_stage": new_stage,
    "updated_at": "now()"
}).eq("id", app_id).eq("current_stage", old_stage).execute()

if not res.data:
    return False    # someone else won the race

self.client.table("NzymeRecruitingProcessHistory").insert({
    "application_id": app_id,
    "from_stage": old_stage,
    "to_stage": new_stage
}).execute()
```

If the optimistic UPDATE matched zero rows (another invocation already moved the stage), the history insert is skipped. This is the deduplication mechanism for concurrent Observer invocations.

### Failure modes
- The optimistic UPDATE returning zero rows is treated as "already applied, no-op" — but it's also indistinguishable from "the application doesn't exist." If the caller passed a wrong `app_id`, the silent failure is invisible.
- The two writes (UPDATE applications, INSERT history) are not transactional. If the INSERT fails (e.g., FK violation, network blip), `current_stage` was updated but no history row exists.

---

## Supabase Storage — `resumes` bucket

Not a table, but part of the schema.

| Aspect | Value |
|---|---|
| Bucket name | `resumes` |
| Public | **Yes** (required for the `get_public_url` workflow to function with Notion file blocks) — see [F-2](../handover_audit.md#f-2-cv-storage-uses-public-bucket-with-predictable-filenames) |
| Key format | `{int(time.time())}_{safe_name}` — sanitized to ASCII-alphanum + `._-` |
| MIME detection | `mimetypes.guess_type(safe_name)` with fallback `application/octet-stream` |
| Upsert behavior | `upsert: "true"` — same key overwrites |
| Retention | None — files persist indefinitely |

The public URL format is `https://yphbrpbwpakjduhmoimw.supabase.co/storage/v1/object/public/resumes/{key}`.

---

## Cross-table reads

A handful of helper methods join tables for specific reads:

| Helper | Joins | Used by |
|---|---|---|
| `get_applications_by_candidate_id(candidate_id)` | `Applications` JOIN `Processes` to enrich with `process_name`, `matrix_characteristics` | AI-pending reprocessing |
| `get_active_confidential_processes_for_candidate(candidate_id)` | `Applications` → `Processes` filtered by `is_confidential=true AND status='Open'` | Governance computation |
| `get_outcome_context(workflow_page_id)` | `Applications` → `Processes` (`process_name`) → `TalentNetwork` (`name`, `notion_page_id`) | Confidential Assessment creation |
| `resolve_process_by_notion_db_id(database_id)` | `Processes` OR across 4 columns | Webhook router dynamic registry |
| `resolve_application_by_outcome_db_id(database_id)` | `Applications` on `notion_outcome_id` | Webhook router application registry |

Supabase has no foreign key enforcement at the application layer in the helpers — every `process_id` and `candidate_id` referenced should exist, but if the parent row is deleted (cascading deletes are configured at the DB level, presumably), the helper silently returns empty.

---

## What's NOT in Supabase

- The Notion permission rule that enforces `Governance: Edit & View Access` — lives in Notion.
- The fact that a Process Dashboard page exists — there's no Supabase `dashboard_pages` table; Notion is authoritative for the queue.
- The mapping from `Process Type` → template — derived at runtime from Notion's template list.
- Process Dashboard page IDs — the Factory doesn't store the dashboard page ID it processed.

If you ever need to rebuild Supabase from scratch, you can mostly reconstruct it by re-running the Factory and Harvester against existing Notion data. The exceptions: `NzymeRecruitingProcessHistory` (no source of truth in Notion) and the rejection reasons / headhunter feedback URLs would need to be backfilled from Notion's stored content.
