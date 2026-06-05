# 00 — System Overview

Nzyme Talent Engine is a serverless Python application that connects a Notion workspace (used as the recruitment UI) to a Supabase Postgres database (used as the system of record for analytics and audit). It runs as a single AWS Lambda function with three logical workers triggered by two complementary mechanisms — scheduled EventBridge rules and Notion workspace webhooks.

## One-page diagram

```
                              ┌─────────────────────────────────┐
                              │           NOTION                │
                              │  (User-facing surface — forms,  │
                              │   workflow DBs, dashboards)     │
                              └─────────────────────────────────┘
                                  │              ▲
                  webhook         │              │     read/write
                  (POST JSON)     ▼              │     pages, DBs,
                              ┌─────────────────────┐  blocks, schemas
                              │                     │
       EventBridge ────────▶  │   AWS Lambda        │ ─────────▶  OpenAI
       (cron schedules)       │   nzyme-talent-     │             gpt-5-mini
                              │   management       │             (CV parsing)
                              │                    │
                              │   Function URL     │ ─────────▶  Exa.ai
                              │   (webhook         │             (LinkedIn enrichment)
                              │    receiver)       │
                              │                    │
                              └─────────────────────┘
                                       │
                                       │ SQL + Storage
                                       ▼
                              ┌─────────────────────┐
                              │   SUPABASE          │
                              │   - Postgres tables │
                              │   - Storage bucket  │
                              │     (CV PDFs)       │
                              └─────────────────────┘
```

The Lambda is *the* compute. There are no other services in this stack — no SQS, no Step Functions, no separate ingestion service. Every entry point (EventBridge schedule, webhook POST, manual `aws lambda invoke`) lands in `main_lambda.lambda_handler` and is routed from there.

## The three workers

| Worker | Source file | Responsibility |
|---|---|---|
| **Factory** | [scripts/factory_worker.py](../../scripts/factory_worker.py) | Sets up a new recruiting process. Reads a "Process Dashboard" page, fuzzy-matches its `Process Type` to a Notion template, applies the template (which creates child Workflow/Form/Bulk/Feedback databases), renames them, fills the JD and Interview Stages pages from the Guidelines database, parses the stages and writes them to the Workflow DB's `Stage` select, and registers the process in Supabase. |
| **Harvester** | [scripts/harvester.py](../../scripts/harvester.py) | Ingests new candidates. Downloads CVs from Notion's file-upload storage, uploads them to Supabase Storage, parses them with OpenAI (Pydantic-structured output), resolves the candidate's identity against the Talent Network table (4-rule engine), creates or merges a Main DB record, creates an application linking the candidate to the process, and writes a strategic assessment matrix back to the workflow page. |
| **Observer** | [scripts/observer.py](../../scripts/observer.py) | Watches for changes after ingestion. Detects stage transitions, processes Outcome Form rejections, ingests interviewer feedback PDFs, runs AI-pending reprocessing when a CV is added late, dispatches candidates between processes, generates on-demand feedback assessments, and syncs process Open/Closed status back to Supabase. |

Each worker has two entry points: a `run_once()` method (called by EventBridge) and a webhook-shaped method (`run_from_webhook` / `process_single_from_webhook` / `handle_webhook_event`). The same business logic runs through both paths — the entry point only differs in *how the work was queued* and *whether the worker scans all processes or just one page*.

## The two triggers

### EventBridge schedules
Three cron rules, all in `eu-west-1`:

| Rule | Schedule | Worker |
|---|---|---|
| `nzyme-factory-schedule` | `cron(0 * * * ? *)` (hourly) | Factory (safety net) |
| `nzyme-harvester-schedule` | `cron(0/10 * * * ? *)` (every 10 min) | Harvester (primary) |
| `nzyme-observer-schedule` | `cron(3/10 * * * ? *)` (every 10 min, offset by 3) | Observer (primary) |

The Harvester and Observer are scheduled because their work is naturally periodic (ingest a batch of CVs, sweep for recent changes). The Factory is scheduled as a *safety net* — it catches Process Dashboard pages whose creation webhook was missed for any reason. Day-to-day, Factory pages are processed within seconds of creation via the webhook path, not on the hourly tick.

### Workspace webhooks
A single Lambda Function URL — `https://vi6n7zvmytou7djtx7ixmobc4e0ittqz.lambda-url.eu-west-1.on.aws/` — receives every Notion webhook. The router parses three different payload shapes (see [05_webhook_router.md](05_webhook_router.md)) and dispatches to handlers gated by per-handler feature flags. Eight handlers map to specific databases:

| Handler | Triggers on | Worker that runs |
|---|---|---|
| `process_dashboard` | New page or status change on Process Dashboard | Factory + Observer (status sync) |
| `main_candidate` | Any change to a Main DB candidate page | Observer |
| `central_reference` | Reference submitted in central References DB | Observer |
| `workflow_item` | Stage change or "Assessment Requested" toggle on Workflow page | Observer |
| `feedback_form` | PDF uploaded to a process's Feedback Form | Observer |
| `form_submission` | New row in a Form DB (CV submission) | Harvester |
| `bulk_submission` | New row in a Bulk DB (multi-file upload) | Harvester (splits into individual Form entries) |
| `outcome_form` | Outcome Form entry (per-application) | Observer |

Webhooks give near-instant response; EventBridge guarantees eventual consistency.

## Data flow (the happy path)

A candidate submits their CV through a process's Form DB. Here's what happens:

1. **Notion native automation** on the Form DB fires when the page is created. Notion's automation creates a corresponding Workflow DB entry (the "application") and sends a webhook to the Lambda Function URL.
2. **`lambda_handler` routes the webhook** through `WebhookRouter` → `HANDLER_FORM_SUBMISSION` → `HarvesterRelational.process_single_from_webhook(page_id, process_context)`.
3. **Harvester queries the Workflow DB** for unprocessed candidates with a populated `ID` field (link back to the Form entry). It retries up to 3× / 5s apart to give Notion's automation time to create the Workflow entry.
4. **`process_candidate` runs the inner pipeline:** fetch CV from Notion's file storage → upload to Supabase Storage (`resumes` bucket) → call OpenAI with Pydantic-structured output → resolve identity via the 4-rule engine in `core.supabase_client.resolve_candidate_identity` → create or merge the Main DB page → write a Supabase candidate row → create the application row → write a strategic assessment matrix back to the workflow page.
5. **`PROP_CHECKBOX_PROCESSED` is set to true** in a `finally` block, so the page never reappears in subsequent sweeps even if processing failed partway.
6. **The Observer sweeps every 10 minutes** for any pages whose `last_edited_time` falls in the last ~11 minutes. It detects the new application, no-ops on stage change (Supabase already matches), and continues.

When the recruiter later changes the candidate's `Stage` in the Workflow DB, the Observer's `_handle_workflow_item` (triggered by webhook or by the next sweep) compares the new Notion stage to the Supabase `current_stage`, calls `register_stage_change` with optimistic locking, and inserts a row into `NzymeRecruitingProcessHistory`.

When the candidate is eventually closed out, an Outcome Form entry (a child DB on the workflow page) is filled in. The Observer's `_handle_outcome_entry` fuzzy-matches the outcome to a Stage option, writes the explanation to Supabase as a rejection reason, and creates a Confidential Assessment page in a separate Notion DB — relying on Notion's bidirectional relation to back-populate the Main DB candidate.

## What lives where

| Layer | Component | What it owns |
|---|---|---|
| **UI / CRM** | Notion workspace | Forms, dashboards, workflow tables, JD/Interview Stages pages, Confidential Assessments DB, References DB. Where humans work. |
| **Orchestration / compute** | AWS Lambda (`nzyme-talent-management`, eu-west-1) | All Python code in `core/` and `scripts/`. Stateless; warm contexts reuse `httpx.Client` and Supabase clients. |
| **System of record** | Supabase Postgres | `NzymeTalentNetwork` (candidates), `NzymeRecruitingProcesses` (processes), `NzymeRecruitingApplications` (candidate ↔ process links), `NzymeRecruitingProcessHistory` (stage transitions). See [03_supabase_schema.md](03_supabase_schema.md). |
| **CV blob storage** | Supabase Storage bucket `resumes` | PDF + DOCX files keyed by `{unix_timestamp}_{safe_name}`. Public URLs embedded in Notion. |
| **AI** | OpenAI gpt-5-mini via `openai.beta.chat.completions.parse` | CV parsing (`CVData` Pydantic model), feedback PDF → markdown, feedback assessment (CV + interview feedback → scored matrix). |
| **LinkedIn enrichment** | Exa.ai | Fetches LinkedIn profile markdown via URL when no CV is available (optional — gated by `EXA_API_KEY`). |
| **Observability** | CloudWatch Logs + optional Logfire | Per-invocation request ID set by `core.logger.set_request_id`. |

## What this doc set covers

- [01_workers.md](01_workers.md) — one section per worker, entry points, read/write surfaces, idempotency
- [02_notion_integration.md](02_notion_integration.md) — the most important file in the set; the table of every Notion interaction and its failure mode
- [03_supabase_schema.md](03_supabase_schema.md) — every table, column, JSONB shape, unique constraint
- [04_ai_pipeline.md](04_ai_pipeline.md) — OpenAI usage, Pydantic models, AI-pending reprocessing
- [05_webhook_router.md](05_webhook_router.md) — three payload shapes, registry layers, feature flags
- [06_decision_log.md](06_decision_log.md) — non-obvious architectural choices, alternatives considered
- [07_runbook.md](07_runbook.md) — deploy, rollback, log tailing, feature flag rollout, recovery paths
