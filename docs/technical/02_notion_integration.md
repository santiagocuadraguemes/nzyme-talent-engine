# 02 — Notion Integration

This is the highest-value document in the technical set. Notion is not just an API — it's a UI, an automation engine, and a permissions system, all of which this codebase relies on. The contracts between code and Notion are mostly implicit. This document tries to make them explicit.

If you read only one doc before touching the code, read this one.

---

## How code talks to Notion

A single class — `core.notion_client.NotionClient` — wraps every Notion HTTP call:

| Method | Wraps | Used for |
|---|---|---|
| `get_page(page_id)` | `GET /v1/pages/{id}` | Read a page's properties + parent metadata |
| `get_data_source_id(database_id)` | `GET /v1/databases/{id}` → `data_sources[0].id` | Resolve the 2025-09-03 API's two-tier ID model |
| `get_database_schema(data_source_id)` | `GET /v1/data_sources/{id}` → `properties` | Read column types (Stage options, etc.) |
| `get_page_blocks(block_id)` | `GET /v1/blocks/{id}/children?page_size=100` | Read child blocks (for child_database discovery) |
| `append_block_children(block_id, children, after=)` | `PATCH /v1/blocks/{id}/children` | Add JD content, Interview Stages content, feedback markdown |
| `query_data_source(ds_id, filter_params)` | `POST /v1/data_sources/{id}/query` | Filtered reads (with pagination) |
| `update_page(page_id, properties=)` | `PATCH /v1/pages/{id}` | Set property values |
| `update_database(database_id, title=)` | `PATCH /v1/databases/{id}` | Rename a DB |
| `update_data_source(data_source_id, properties=)` | `PATCH /v1/data_sources/{id}` | Mutate schema (Stage options) |
| `create_page(database_id, properties)` | `POST /v1/pages` | Insert a row |
| `create_database(parent_page_id, title, properties_schema)` | `POST /v1/databases` | Create a child DB (not actually used in current code — templates do this) |
| `find_child_database(parent_block_id, db_title_match)` | BFS through `get_page_blocks` | Locate a child DB by title (case-insensitive substring) |

The shared `httpx.Client` ([notion_client.py:31](../../core/notion_client.py)) holds the `Authorization` header and `Notion-Version: 2025-09-03`, with a 30s timeout. Pagination is handled inside `query_data_source` via `start_cursor` ([notion_client.py:101-117](../../core/notion_client.py)).

The Observer also makes raw `httpx.request` calls via `_api_request` ([observer.py:83](../../scripts/observer.py)) — those add a 429 rate-limit retry loop with `Retry-After` honored. The rest of the codebase doesn't have that retry.

## The two-tier ID model (data sources vs databases)

In `Notion-Version: 2025-09-03`, every database has one or more *data sources*. A database is a container; a data source is a queryable schema. Most existing databases have exactly one data source. Operations that read or query rows talk to the data source ID; operations that rename or relink the DB talk to the database ID.

In this codebase:
- **Database ID → Data Source ID** is resolved via `get_data_source_id(db_id)`, which calls `GET /v1/databases/{id}` and returns `data_sources[0].id`.
- Static env vars (`NOTION_MAIN_DB_ID`, `NOTION_PROCESS_DASHBOARD_DB_ID`, etc.) store **database IDs**. Workers resolve to data sources lazily.
- `create_page` is special: it tries to resolve to a data source ID first, but falls back to using the database ID directly with `parent.type="database_id"` ([notion_client.py:170-184](../../core/notion_client.py)).
- The webhook router normalizes IDs by stripping dashes (`_normalize_id`) — Notion's webhook payloads sometimes include dashes and sometimes don't.

If a database is later given a second data source (a Notion feature on the roadmap), `data_sources[0]` is no longer deterministic. Today this isn't a problem; in the future it could be.

## The Notion boundary table

Every important interaction with Notion, what the code expects vs. what Notion actually does, and what fails if Notion changes:

| # | Action | Code does | Notion does (autonomously) | Failure mode |
|---|---|---|---|---|
| 1 | **Form submission** | Receives webhook for the Form DB entry creation | Native automation in Notion creates a Workflow DB entry, links `ID` field, and sends webhook | If Notion's automation is paused/broken, Workflow page never appears → Harvester `process_single_from_webhook` retries 3× / 5s then logs "EventBridge will catch them" and exits. EventBridge sweep also finds nothing because no Workflow page exists. Candidates submitted in this window are silently dropped. ([harvester.py:974-985](../../scripts/harvester.py)) |
| 2 | **Process Dashboard page creation** | `run_from_webhook(page_id)` applies template via `PATCH /v1/pages/{id}` with `{template, erase_content: true}` | Notion creates child DBs (Workflow/Form/Bulk/Feedback) and child pages (JD, Interview Stages) **asynchronously**. The PATCH returns 200 before they exist. | Code retries up to 4× with 8-10s sleeps ([factory_worker.py:270-305](../../scripts/factory_worker.py)). If Notion's async lag exceeds 38s, child DBs aren't found → `[CRITICAL]` log, page abandoned in pending state. The March 2026 child-DB async-lag incident (recoverable via `git show HEAD~1:docs/continuation_prompt.md`) was an exact instance. |
| 3 | **Template application via PATCH** | `_apply_template_to_page` does `PATCH /v1/pages/{id}` with `template_id` + `erase_content: true` | Notion clones the template's body and child DBs into the page. The page's `properties` (Stage select on Workflow DB, etc.) come from the database schema, not the template. | If `erase_content` semantics ever change (e.g., partial erase), idempotent retries would duplicate content. Currently safe. |
| 4 | **Stage parsing from guidelines** | Reads a table block in the Interview Stages document, builds a list of options, prepends ZWSPs by index | Notion stores select options as JSON; the table block is just rendered content | If a guidelines author changes the table format (e.g., adds a "Notes" column, or uses bullets instead of tables), `parse_stages_from_page` returns empty or malformed stages. The Factory sets an empty Stage select — every subsequent stage write fails. ([guidelines_parser.py:276-387](../../core/guidelines_parser.py)) |
| 5 | **ZWSPs encoded in stage names** | Prepends N copies of U+200B to the Nth stage name | Notion sorts select options alphabetically by name; ZWSPs sort before printable characters, giving a stable ordering | Invisible characters in stage names. Any code that compares stages by `==` to a copy-pasted string from CloudWatch logs will fail mysteriously. (See [F-11](../handover_audit.md#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant).) |
| 6 | **Stage options replacement** | `update_data_source({"Stage": {"select": {"options": stage_options}}})` | Notion replaces the entire option set; any pages whose Stage references a deleted option get **silently remapped** to the closest surviving option | March 2026 PC RB Solutions incident: stages `0.4 On Hold`, `0.5 Back-up` were deleted, candidates remapped to closest match. [F-3](../handover_audit.md#f-3-factorys-stage-options-update-is-still-destructive-march-audit-bug-3-not-fixed). |
| 7 | **Child DB discovery via BFS** | `find_child_database` walks blocks 4 levels deep, matches title case-insensitively | Notion allows arbitrary nesting (toggles, columns, callouts) | If a Notion editor wraps a child DB in one extra toggle past depth 4, `find_child_database` silently returns None. Downstream code treats this as "DB not present." ([F-12](../handover_audit.md#f-12-find_child_database-silently-returns-none-past-depth-4)) |
| 8 | **Webhook payload shape A (native)** | Detects `entity.type=="page"` → reads `entity.id` + `data.parent.id` (when `parent.type=="database"`) or `data.parent.database_id` | Notion sends native workspace webhooks for page.created / page.updated / page.content_updated events | If Notion adds new event types, the router parses them but the database ID may be missing → logged as "Unrecognized database" and dropped silently. ([webhook_router.py:100-112](../../core/webhook_router.py)) |
| 9 | **Webhook payload shape C (automation)** | Detects `source.type=="automation"` → reads `data.id` + `data.parent.database_id` | Notion sends webhook from a configured "Send webhook" action in a workspace automation | If the automation is reconfigured to use a different parent type or to send only metadata, shape detection still matches but `database_id` may be None → silent drop. |
| 10 | **`Processed [Do not touch]` checkbox** | Factory checks at the top of `_process_dashboard_page`; sets to true only after Supabase + Notion writes succeed | Used as a system-managed flag; the "[Do not touch]" suffix is documentation for users | If a user touches it (resets to false), the Factory re-runs `configure_process` — Supabase guard catches the already-registered case, marks Processed=true again. If the *Supabase row* was also deleted, all the destructive Notion ops re-run. ([F-3](../handover_audit.md#f-3-factorys-stage-options-update-is-still-destructive-march-audit-bug-3-not-fixed), [F-6](../handover_audit.md#f-6-factorys-destructive-notion-writes-have-no-rollback-on-supabase-failure)) |
| 11 | **`Processed` checkbox on Workflow / Form / Feedback / Outcome / Reference / Bulk** | Set to true in a `finally` block after processing | Used to filter unprocessed entries in subsequent sweeps | If the `finally` block itself fails (e.g., Notion API down at exactly the wrong moment), the page reappears as unprocessed; on the next sweep, the existence check (`get_application_by_notion_id`) prevents reprocessing for the Workflow path but other paths may double-process. |
| 12 | **Source multi-select (append semantics)** | Reads current Source tags, appends new tag (case-insensitive dedup), writes back | Multi-select stored as ordered list; Notion has no append API — only full set replacement | Concurrent ingests on the same candidate lose tags (read-modify-write race). [F-10](../handover_audit.md#f-10-concurrent-source-multi-select-append-is-a-read-modify-write-race). |
| 13 | **Creator multi-select (Main DB)** | **Never written** by code | User-managed; humans tag candidates here | If code starts writing Creator (e.g., by accident in a refactor), user data is overwritten. Documented invariant in [.claude/rules/notion-schema.md](../../.claude/rules/notion-schema.md). |
| 14 | **Confidential Assessment back-relation** | `create_page` in Confidential Assessments DB with `relation: [{id: main_db_page_id}]` | Notion **auto-populates** the inverse relation on the Main DB page (the `Assessment` related property) | If Notion ever changes back-relation semantics to be async or opt-in, the candidate's Main DB page will lack the link to the Confidential Assessment. UI breaks; data is technically still correct. [observer.py:629-636](../../scripts/observer.py) |
| 15 | **Governance: Edit & View Access (people property)** | Sets to either (a) all team groups (non-confidential candidate) or (b) the process's individual governance people (confidential) | Notion enforces page-level visibility based on this property | If Notion changes how the People property interacts with permissions, confidential candidates leak. Today, the Main DB has a permission rule keyed on this property — that rule lives in the Notion UI, not in code. **Code never queries the rule; it just trusts it exists.** |
| 16 | **Notion Permission Group IDs** | Env var `NOTION_ALL_TEAM_GROUP_IDS` comma-separated list, parsed by `core.notion_client.get_all_team_group_ids` | Notion permission groups are workspace-level; their IDs don't appear in the API's standard endpoints | If a group is deleted or its ID rotates, governance writes silently fail (the People property accepts unknown IDs without error, but the rule doesn't match → candidate effectively invisible to everyone). |
| 17 | **Last-edited-time filter (Observer Sniper)** | Filters `last_edited_time > now - 11 minutes` | Notion updates `last_edited_time` on **any** edit, including edits to child DB content — writes to `Past Experience [AI-generated]` cascade to the parent Workflow page's `last_edited_time` | Cascade was contributory to the March 2026 NZ Rotational incident: AI-pending reprocessing wrote to child DB → parent's `last_edited_time` updated → Observer saw the page in lookback window → mismatch on stage (which had a bug at the time) → corrective transition logged. Bug is fixed but the cascade is still there. |
| 18 | **Notion-hosted file URLs (CV uploads)** | `find_cv_in_auxiliary` reads `file.url` from a `files` property; immediately downloads via `httpx.get(notion_url)` | Notion file URLs are short-lived signed URLs (~1 hour TTL typically) | If processing is delayed (e.g., Harvester run picks up a 2-hour-old form entry, or AI-pending retry hits a stale URL), the download 403s. The retry loop in `_reprocess_ai_pending` re-reads the page's CV property fresh, so usually fine — but no defense against the *first* attempt failing on a fresh URL because of a slow OpenAI call beforehand. [F-5](../handover_audit.md#f-5-storage-clients-cv-download-has-no-timeout) |
| 19 | **Template list endpoint** | `GET /v1/data_sources/{ds}/templates` returns a list of `{id, name}` | Notion exposes templates per data source; the endpoint is documented but its behavior on databases with zero templates isn't | If the Process Dashboard has no templates, `_resolve_template_id_for_process_type` returns None and the page is abandoned. Adding a new `Process Type` option without a matching `PROCESS TEMPLATE - {suffix}` template silently breaks every new process of that type. |
| 20 | **Fuzzy template matching** | `difflib.SequenceMatcher` ratio between `process_type.lower()` and each template's name suffix | Notion has no semantic match concept | Two templates with similar suffixes (e.g., `"Lead"` vs `"Senior Lead"`) for a process type `"Tech - Senior Lead"` could score close. Logged top + runner-up scores at INFO. |
| 21 | **`Past Experience [AI-generated]` matrix population** | After template apply, the Factory reads rows of `"Past Experience"` child DB (the template version), extracts characteristics + definitions, stores as Supabase `matrix_characteristics` JSONB | The matrix DB is templated content; rows are pre-filled by the template definer | If a Process Type's template lacks a `"Past Experience"` child DB, `_extract_matrix_from_template` retries and gives up; `matrix_characteristics` is null. Strategic assessments skip entirely for that process — log line `No strategic assessment data to fill (skipping)`. |
| 22 | **Multi-step Outcome flow** | Outcome Form on Workflow page → user fills `Discarded/Disqualified/Lost` select → webhook (or sniper) fires → Observer fuzzy-matches outcome to a Stage option | Notion stores the Outcome Form as a per-application child DB. The form is created by the *template*, not by code. | If a Workflow template stops including the Outcome Form child DB, `find_child_database(page_id, "Process Outcome Form")` returns None at ingest time → `notion_outcome_id` never set on the application → Outcome webhooks never route correctly → must rely on Radar engine. ([harvester.py:626-631](../../scripts/harvester.py)) |
| 23 | **Outcome fuzzy stage matching** | `_fuzzy_match_stage` does substring match (`partial_text in opt["name"]`) | Notion stores Stage options with ZWSP prefixes | Substring `"Discarded completely for Nzyme"` matches the ZWSP-prefixed `"​​​​Discarded completely for Nzyme"` option. Works because substring ignores prefix. Would break if the outcome label and stage name diverge in content (e.g., "Disqualified only for this role" not appearing verbatim in any stage). |
| 24 | **Reference distribution (Central → workflow)** | For each active application of the candidate, find `"Candidate References [Input here feedback received]"` child DB on the workflow page, create a new row with the reference details | Notion has no concept of "distribute one row to many DBs" — code does it manually | If one of N applications has the child DB at depth > 4 ([F-12](../handover_audit.md#f-12-find_child_database-silently-returns-none-past-depth-4)), `global_success=False` and the *entire* reference is never marked Processed; the next sweep tries again and creates duplicates in the applications where the child DB was found. |
| 25 | **Headhunter firm name resolution** | Reads `Headhunter` relation on Process Dashboard → first related page's title (in Headhunters DB) → stores as `NzymeRecruitingProcesses.headhunter_name` | Notion relations are bi-directional but only the linked-page ID is in the relation; the firm name is the page's title | If the linked Headhunters DB page is renamed, the stored `headhunter_name` becomes stale (it's snapshot at process creation, never refreshed). The Source tag `"Headhunter - {firm}"` written into Main DB is also stale. |
| 26 | **AI Pending checkbox + JSONB metadata** | `_reprocess_ai_pending` queries Notion (not Supabase) for the checkbox; Supabase JSONB `ai_pending`, `ai_pending_cv_url`, `ai_pending_process_name` are debugging metadata | Notion checkbox is the truth, because Supabase JSONB can be overwritten by Observer's `manage_candidate` syncs before reprocessing runs | If anyone migrates the AI-pending queue to Supabase as source of truth, race condition reappears. The current arrangement is intentional; see [.claude/rules/architecture.md](../../.claude/rules/architecture.md). |
| 27 | **Cross-DB candidate dispatch** | `_logic_dispatch_candidate_to_form` reads Main DB candidate's properties + the destination process's Form DB schema; creates a new Form entry in the destination with whatever subset of properties the destination DB supports | Notion has no cross-DB copy primitive | If the destination DB lacks a `Name` (title) column under that exact name, code falls back to first title-type column ([observer.py:1356-1362](../../scripts/observer.py)). If the destination DB has none at all, dispatch aborts with `Destination has no Title column`. |
| 28 | **"Assess this candidate" trigger** | Workflow page `Assessment Requested` checkbox → webhook → Observer reads `process_context.assessment_characteristics` → builds Feedback Assessment matrix | Checkbox is set by Notion button/automation; Observer unchecks it after processing | Checkbox is dual-purpose: (a) it's set by a button users press, (b) it's reset by code. If a user manually unchecks during processing, the next change fires another webhook → infinite loop? No — Observer dedup at [observer.py:702-710](../../scripts/observer.py) catches existing AI assessments. |

## Webhooks: the three payload shapes

Notion sends webhooks in three different formats. [WebhookRouter.parse_event](../../core/webhook_router.py) sniffs each before extracting IDs.

### Shape A — Native workspace webhook
```json
{
  "type": "page.updated",
  "entity": {"id": "...", "type": "page"},
  "data": {"parent": {"id": "...", "type": "database"}}
}
```
Detected by: `entity.type == "page"`.

### Shape B — Fallback / legacy
```json
{
  "data": {"id": "...", "parent": {"database_id": "..."}}
}
```
Detected by: neither shape A nor C.

### Shape C — Automation webhook
```json
{
  "source": {"type": "automation", "automation_id": "..."},
  "event_id": "...",
  "data": {
    "object": "page",
    "id": "...",
    "parent": {"type": "data_source_id", "database_id": "..."},
    "properties": { /* inline */ }
  }
}
```
Detected by: `source.type == "automation"`.

All three shapes extract the same `(page_id, database_id)` pair and feed the rest of the routing pipeline. Shape C also includes inline `properties` — currently unused; workers re-fetch the page anyway for consistency.

There's a fourth implicit "shape" — the challenge handshake:
```json
{"challenge": "<token>"}
```
which `parse_event` detects via `"challenge" in body` ([webhook_router.py:78-80](../../core/webhook_router.py)) and the Lambda echoes back to verify URL ownership.

## Notion native automations (configured in the UI, not in code)

Three native automations live in the Notion workspace, *not* in this repo:

1. **Process Dashboard: on page creation → send webhook** — the trigger for Factory's `run_from_webhook` path. The Lambda Function URL is configured as the webhook destination.
2. **Form DB: on page creation → create Workflow DB entry + populate ID** — this is what turns a CV submission into an application. Without this automation, the Harvester has nothing to find. The `process_single_from_webhook` retry loop exists to wait for this automation.
3. **Workflow DB / Outcome Form / Feedback Form / Bulk DB / Main DB / References DB: on relevant change → send webhook** — these are configured per database in Notion's automation UI. Each one targets the same Lambda Function URL. The router uses the source `database_id` to dispatch.

If any of these automations is paused, deleted, or reconfigured to point elsewhere, the corresponding webhook path stops working — and the only signal is that things go through the EventBridge safety net instead (10-minute lag instead of seconds). There's no Notion API to verify automation health from code; the team has to check the Notion UI's automations panel manually.

## Notion permissions (governance)

The `Governance: Edit & View Access` people property on the Main DB has a Notion permission rule attached that scopes page visibility to the listed users/groups. The rule is configured in the Notion UI per-database — **code does not create or manage the rule itself**, only the value of the property.

For non-confidential candidates, the property is set to all team groups (env var `NOTION_ALL_TEAM_GROUP_IDS` — comma-separated permission group UUIDs). For confidential candidates, it's set to the process's individual governance people (a People list on the Process Dashboard).

If the env var holds a wrong/stale group ID, the rule still applies but the group doesn't grant the expected access — candidates effectively become invisible. There's no health check for this.

## File handling: Notion → Supabase Storage

Notion-hosted files have URLs of the form `https://prod-files-secure.s3.us-west-2.amazonaws.com/{uuid}/...?X-Amz-Algorithm=...&X-Amz-Expires=3600&X-Amz-Signature=...` — pre-signed S3 URLs with ~1 hour TTL.

The code's approach is:
1. Read the URL from a Notion `files` property (`file.url` for uploaded, `external.url` for linked).
2. Immediately download via `httpx.get(notion_url)` ([storage_client.py:31](../../core/storage_client.py)).
3. Upload to Supabase Storage (`resumes` bucket, public, key = `{int(time.time())}_{safe_name}`).
4. Replace the Notion `CV` property with the Supabase public URL (so future reads don't depend on the original Notion URL).

This works because the URL is fresh at webhook receive time. It can fail if:
- AI processing between read and upload takes long enough for the Notion URL to expire (especially the `_reprocess_ai_pending` path which re-reads from the Main DB page that may already hold an old Notion URL).
- The Lambda doesn't have outbound internet (shouldn't happen — Lambda Function URLs have NAT by default — but VPC misconfiguration could break it).

## What's NOT mediated through this codebase

| Thing | Where it actually lives |
|---|---|
| Notion automations (page creation triggers) | Notion UI |
| Notion permission rules (Edit & View Access enforcement) | Notion UI |
| Process Dashboard templates (`PROCESS TEMPLATE - {suffix}`) | Notion UI, in the Process Dashboard's templates list |
| Workflow / Form / Bulk / Feedback template structure | Inside each `PROCESS TEMPLATE - ...` template |
| Guidelines documents (Interview Stages, Role & Candidate Description) | Notion Guidelines DB |
| Permission group definitions | Notion UI |
| Confidential Assessments DB schema (Assessment select options like "4. Discarded") | Notion UI |
| Stage colors and ordering | Encoded in Notion via ZWSP prefix order set by code |

When something breaks and the cause isn't in the code, the cause is almost always in the Notion UI configuration of one of these.
