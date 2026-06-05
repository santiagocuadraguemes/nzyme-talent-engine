# 06 — Decision Log

Non-obvious architectural choices and why they exist. Some "Why this won" entries were originally inferred from code shape and git history rather than explicit documentation — those are marked **INFERRED**. The intent is to give the next engineer something to push back against rather than mistaking the current state for the only possible state.

> **Handover note (June 2026):** the entries originally marked **INFERRED** (D-1, D-2, D-4, D-5,
> D-8, D-10, D-12) were reviewed and **confirmed by the original author** during the handover — the
> rationale in each now reflects the author's actual reasoning, not a guess. The INFERRED tags are
> kept only to record that the wording was reconstructed, not authored. D-13 through D-15 and the
> "Footguns & assumptions" section at the end were captured directly from the author at handover.

---

## [D-1] Single Lambda function with three workers

**Choice:** All three workers (Factory, Harvester, Observer) live in one Lambda function, routed by event shape inside `lambda_handler`.

**Alternatives considered:**
- Three separate Lambda functions, one per worker, each with its own EventBridge rule + Function URL.
- Step Functions orchestrating workers as states.
- ECS containers behind an ALB.

**Why this won:** **INFERRED** — single deploy unit, single env var blast radius, shared client connection caching across warm invocations. Three separate Lambdas would need their own deploy pipelines, three CloudWatch log groups, and three sets of `NotionClient`/`SupabaseManager` cold-start overheads. The shared `httpx.Client` in `NotionClient` saves connection pool reuse across warm invocations of any worker.

**Consequences:**
- Good: one place to look for any incident; deploy is atomic across workers.
- Bad: an OpenAI outage in the Harvester can starve the Observer (concurrent execution limits); a single bad commit affects all three workers; cold starts are heavier (must import all dependencies even if only the Factory runs).

**Reversibility:** Medium. Splitting into three would require duplicating env vars, splitting the deploy script, and reconfiguring three EventBridge rules. Not a one-way door.

---

## [D-2] Notion as the UI, Supabase as the system of record

**Choice:** All user-facing surfaces are Notion databases. Supabase is read by code and read by analytics (BI tools, ad-hoc queries) but not by humans directly.

**Alternatives considered:**
- Custom web UI (React + Supabase or React + Postgres directly).
- Notion-only, no Supabase. All state in Notion JSONB-style structures.
- Airtable or another lightweight no-code tool instead of Notion.

**Why this won:** **INFERRED** — Nzyme's recruiting team was already using Notion for everything. Building a custom UI would have meant fighting the team's existing workflow; using Notion meant the system could be adopted without retraining anyone. Supabase was added because Notion's query model is too thin for analytics (no joins, multi-select fields are not indexable from outside, history isn't tracked).

**Consequences:**
- Good: zero UI development; new dashboards and views are built by recruiters in Notion, not engineers; data is visible immediately without backend work.
- Bad: every concept has two homes (Notion + Supabase), forcing every write to be a coordinated pair; reconciliation logic (Observer's sniper, AI-pending reprocessing) exists primarily to keep them aligned; Notion is the only place where some things live (templates, automations, permissions) — a Notion outage takes the whole system down.

**Reversibility:** Hard. Migrating off Notion means rebuilding workflows the recruiting team has internalized over months/years. Probably never reversed without a major business reason.

---

## [D-3] Dual triggers: EventBridge schedules + workspace webhooks

**Choice:** Each worker has two entry paths — a periodic scheduled invocation that scans-and-acts, and an event-driven webhook for near-real-time response.

**Alternatives considered:**
- Webhooks only.
- Schedules only.

**Why this won:** Webhooks give recruiters fast feedback (a CV is parsed within seconds of submission). Schedules guarantee eventual consistency — if a webhook drops, fails, or the feature flag is off, the next scheduled sweep catches it. The CLAUDE.md explicitly describes the Factory schedule as "safety net only."

**Consequences:**
- Good: resilience to webhook failures; safe to roll back any webhook handler (flag → false) without losing eventual processing.
- Bad: the same business logic runs through two code paths (`run_once()` and `process_single_from_webhook`); concurrency guards have to handle both invocation orders; cognitive overhead — every change to a worker means thinking about both paths.

**Reversibility:** Easy in either direction. Disabling EventBridge means full reliance on webhooks (risky given F-1); disabling webhooks means everything gets 10-minute lag back.

---

## [D-4] Hybrid SQL columns + JSONB blob in `NzymeTalentNetwork`

**Choice:** First-class SQL columns for identity fields (name, email, phone, linkedin_url, cv_url, source, creator, assessment), with everything else (17 experience categories, education, languages, history) in a `candidate_data` JSONB column.

**Alternatives considered:**
- Fully normalized: separate tables for `candidate_experience(candidate_id, category, years_range, has_experience)`, `candidate_companies(candidate_experience_id, name)`, etc.
- Fully JSONB: everything inside `candidate_data`, including email/name/phone.

**Why this won:** **INFERRED** — fully normalized would produce 6+ tables that need joining for every common query, and the experience schema (17 mutually-exclusive sector/functional categories with nested company/role lists) doesn't fit a single normalized table cleanly. Fully JSONB would make identity-based lookups (email match in `resolve_candidate_identity`) slow and prevent UNIQUE constraints on `notion_page_id` / `email`.

The split is pragmatic: columns are things you query *by* (identity, source, assessment), JSONB is everything you query *for* (experience tags, languages, history).

**Consequences:**
- Good: identity queries use index lookups; experience shape can evolve without migrations; one table holds the whole candidate.
- Bad: JSONB shape isn't enforced by the database — see [03_supabase_schema.md](03_supabase_schema.md) for the contract; updates to JSONB are read-modify-write (race-prone for concurrent writes); analytics needs to know the JSONB schema, which isn't formally documented anywhere.

**Reversibility:** Hard for the experience subtree (lots of write sites would need updating). Moderate for other JSONB keys.

---

## [D-5] Four-rule identity resolution, no manual review queue

**Choice:** `resolve_candidate_identity(email, name)` follows a 4-rule decision tree to merge or insert. No human-in-the-loop confirmation step.

**Alternatives considered:**
- Strict: only match on email. Different names with same email = same person; different emails = different people, never merge.
- UI-mediated: when name matches but emails differ, write to a `pending_review` table; a recruiter manually approves the merge.

**Why this won:** **INFERRED** — most real-world CVs lack email parsing accuracy (LinkedIn profiles often skip the email, headhunters sometimes redact contact info before forwarding). Strict email-only matching would create duplicates anytime a candidate enters through two channels. UI-mediated review would have required building a UI in Notion + a queue mechanism, neither of which the team wanted to maintain.

The four rules optimize for *not creating duplicates* at the cost of *occasionally merging different people with similar names* (see [F-9](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people)). The bet was that the cost of duplicate consolidation later is higher than the cost of occasional wrong merges, given Spanish-name-common-collision is rare enough.

**Consequences:**
- Good: low operational overhead; no queue to manage; bulk imports of name-only candidates merge cleanly into existing records.
- Bad: wrong merges are silent and hard to undo (governance lists, process history, experience tags all comingle); name-only merges in `_process_direct_candidate` log a WARNING but no one reads it.

**Reversibility:** Easy to make stricter (just change `resolve_candidate_identity`); hard to retroactively un-merge candidates whose data has already been comingled.

---

## [D-6] AI-pending reprocessing reads from Notion, not Supabase

**Choice:** When `_reprocess_ai_pending` queries for candidates to retry, it queries the Notion Main DB filtered by `AI Pending=true`. Supabase has `candidate_data.ai_pending` JSONB key, but it's debugging metadata only.

**Alternatives considered:**
- Query Supabase first: `WHERE candidate_data->>'ai_pending' = 'true'`.
- Maintain a separate `ai_pending_queue` table in Supabase.

**Why this won:** Explicit documentation in [.claude/rules/architecture.md](../../.claude/rules/architecture.md): "On subsequent runs, `_reprocess_ai_pending()` picks these up from Notion (not Supabase JSONB) to avoid a race condition where the Observer overwrites JSONB keys before reprocessing occurs." Specifically, when the Observer runs on a Main DB page change and calls `manage_candidate` to sync, the entire JSONB is replaced — wiping the `ai_pending*` keys before the Harvester's next scheduled run.

**Consequences:**
- Good: race-free as documented; Notion is a stable index for pending work.
- Bad: every reprocess run incurs a Notion API call; if Notion is down, no reprocessing can happen; the JSONB keys are misleading-looking dead data unless you read this doc.

**Reversibility:** Easy in code (one query change). But it would require fixing the underlying race — adding a Supabase column with a trigger that the Observer doesn't touch.

---

## [D-7] Optimistic locking on stage transitions

**Choice:** `register_stage_change` updates `current_stage` with `WHERE id=? AND current_stage=?`, then inserts the history row only if the update affected 1 row.

**Alternatives considered:**
- Pessimistic locking: `SELECT ... FOR UPDATE` (not supported by Supabase REST client directly).
- No locking — accept that concurrent transitions create duplicate history entries.
- Idempotency keys: hash `(application_id, old_stage, new_stage)` and check on insert.

**Why this won:** Optimistic locking is dead-simple in SQL, has no infrastructure dependency, and handles the most common concurrency case (Observer EventBridge + Observer webhook both detecting the same stage change). The loser of the race just exits silently.

**Consequences:**
- Good: no duplicate history rows; no advisory locks to manage.
- Bad: the "loser" gets no signal — caller has no idea whether the transition succeeded or was already applied (`register_stage_change` returns False in both "already changed by someone else" and "actually failed" cases); History row's `timestamp` reflects the *first* successful update, not subsequent attempts.

**Reversibility:** Easy. Could swap to a different concurrency strategy without changing the public method signature.

---

## [D-8] Feature flag per webhook handler (default false)

**Choice:** Every webhook handler is gated by a `WEBHOOK_*_ENABLED` env var, defaulted to false at deploy. Enabling a flag requires `aws lambda update-function-configuration`.

**Alternatives considered:**
- No flags — handlers always-on.
- Global webhook killswitch (one flag).
- Per-handler flags stored in Supabase (queryable at runtime).

**Why this won:** **INFERRED** — incremental rollout. The webhook architecture was added on top of an existing EventBridge-only system. Each handler was validated against production data before enablement, and the flag let the team revert any individual handler in seconds (env var change ≈ 1 minute total — no redeploy needed).

**Consequences:**
- Good: surgical rollback; new handlers can be deployed dark; flag-off state cleanly degrades to EventBridge-only.
- Bad: the GET-modify-PUT gotcha for env var updates ([07_runbook.md](07_runbook.md#changing-env-vars)); every new handler needs the flag added in three places (constants, main_lambda dict, .env.example); per-handler flags don't compose with other configuration (no way to enable a flag for "process X only" without code changes).

**Reversibility:** Easy to remove individual flags; less easy to switch to Supabase-backed config (would need to refactor every flag check).

---

## [D-9] Always-mark-Processed via try/finally

**Choice:** Both `process_candidate` ([harvester.py:382-390](../../scripts/harvester.py)) and `_handle_outcome_entry` ([observer.py:553-561](../../scripts/observer.py)) wrap the inner logic in `try/finally` that **always** sets the Notion `Processed` checkbox to true, even on exception.

**Alternatives considered:**
- Mark Processed only on success; let failed pages reappear in the next sweep.
- Mark Processed inside the success branch + add a separate dead-letter queue for failed pages.
- Don't mark Processed; rely on Supabase application row existence as the dedup mechanism.

**Why this won:** Documented in [.claude/rules/architecture.md](../../.claude/rules/architecture.md) under "Concurrency Guards": "Always marks Processed=true on the Workflow page, even if processing fails or returns early. This prevents infinite reprocessing loops under EventBridge."

The chosen pattern optimizes for *not breaking the queue* over *retrying transient failures*. If something fails on a candidate page, that candidate is dropped — but the whole batch keeps moving.

**Consequences:**
- Good: no infinite loops; one bad CV doesn't block downstream candidates; CloudWatch logs are the only signal of failure.
- Bad: silent data loss on partial failure; recovery requires manually unchecking Processed and waiting; failure mode is invisible to recruiters (the candidate just doesn't appear in the Main DB).

**Reversibility:** Easy in code. The right architectural answer is a dead-letter queue (a Supabase `harvester_failures` table or an SQS DLQ), which is meaningful work but not large.

---

## [D-10] Governance people stored as JSONB array, not a join table

**Choice:** `NzymeRecruitingProcesses.governance_people` is a JSONB array of Notion user UUIDs.

**Alternatives considered:**
- Join table: `process_governance_people(process_id, notion_user_id)`.
- Separate user reference table: `NzymeUsers(notion_user_id, name)` joined to processes.

**Why this won:** **INFERRED** — the only operation performed on governance_people is read-as-list and write-as-list. There's no query like "find all processes user X has access to" — that query exists, but it's done by `get_active_confidential_processes_for_candidate` which goes through applications, not directly through governance. A join table buys nothing here. Notion user IDs are opaque UUIDs — no name/email/role to denormalize.

**Consequences:**
- Good: trivial to read/write; no migrations when governance lists change.
- Bad: can't query "who has access to what" without scanning all processes; no FK constraint on user IDs (deleted users still appear in lists).

**Reversibility:** Easy. A join table would be a 30-minute migration.

---

## [D-11] OpenAI structured output via Pydantic, not free-form JSON

**Choice:** Every AI call uses `client.beta.chat.completions.parse(response_format=PydanticModel)`. The Pydantic model defines both the response shape and the per-field instructions (via docstrings).

**Alternatives considered:**
- Free-form: ask the model for JSON, parse with `json.loads`, validate post-hoc.
- Function calling: define a function the model calls with structured args.
- Separate prompts per field type: simpler schemas, multiple round-trips.

**Why this won:** Structured output is constrained at decoding time — the model literally cannot output JSON that doesn't match the schema. This eliminates an entire class of "AI returned malformed JSON" bugs. The Pydantic class doubles as the documentation: the field descriptions are sent verbatim to the model as part of the schema, so the prompt and the schema can't drift.

**Consequences:**
- Good: type-safe at the SDK boundary; the model never produces invalid shapes; field instructions live next to field definitions.
- Bad: requires the OpenAI beta SDK + `gpt-5-mini`+; switching providers (Anthropic, open-source models) means rewriting; very large Pydantic schemas inflate the request size.

**Reversibility:** Hard. The Pydantic-based pattern is woven into every AI call site; replacing it would require a parallel parsing layer.

---

## [D-12] ZWSP prefixes for stage ordering

**Choice:** Stage option names are prefixed with N copies of U+200B (zero-width space) by index, e.g., `"0.1 Identified"`, `"​ 0.2 Engagement"`, `"​​ 0.3 Assessment"`.

**Alternatives considered:**
- Add an `Order` number column to a separate Stages reference DB; sort views by that.
- Use Notion's status property type (which has explicit ordering) instead of select.
- Encode order in the visible name itself: `"01 - Identified"`.

**Why this won:** **INFERRED** — Notion's select property sorts alphabetically, and the team wanted lifecycle ordering preserved in the UI without visible numeric prefixes in addition to the stage's own number (`"0.1"`, `"1.1"`, etc.). Status property has constraints around its option groupings ("Not started" / "In progress" / "Done") that don't map cleanly to recruiting stages. The "Add an `Order` column" option requires either a separate stage reference DB or a complex select schema — both more invasive than the ZWSP hack.

**Consequences:**
- Good: stages appear in the correct order in Notion's UI; no extra schema; no UI changes; minimal code (3 lines).
- Bad: invisible characters in strings; copy-paste from logs breaks comparison ([F-11](../handover_audit.md#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant)); was a contributing factor in the March 2026 incident; new developers don't know this exists until they hit a mystery bug.

**Reversibility:** Medium. Migrating off would mean: parse the existing ZWSP-prefixed values in Supabase, strip them, write back, change the Factory to stop adding them, change Notion's existing Stage options. Doable in a single PR.

---

## [D-13] Hardcoded waits and retry counts are tuned to Notion's async latency

**Choice:** Several blocking `time.sleep(...)` + fixed-retry-count patterns are scattered through the workers, with the specific durations baked in as literals:
- `time.sleep(4)` before reading the just-created strategic-assessment table ([harvester.py:749](../../scripts/harvester.py), and the mirror in [observer.py](../../scripts/observer.py) `_fill_strategic_assessment`).
- 3× `time.sleep(5)` waiting for a webhook-referenced Notion page to materialize ([harvester.py](../../scripts/harvester.py), in `process_single_from_webhook` ~line 1052, and the form-entry wait ~line 480).
- 4× `time.sleep(8–10)` waiting for template child-DBs to instantiate ([factory_worker.py:270-305](../../scripts/factory_worker.py)).
- `time.sleep(10)` per file in the bulk splitter ([harvester.py:439](../../scripts/harvester.py)).

**Why this won (author-confirmed):** these are **empirically tuned to observed Notion async-propagation latency** — Notion's "Send webhook" automations and template instantiation are eventually-consistent, and the API returns 200 *before* the downstream page/child-DBs are queryable. The numbers are not arbitrary; they're the values that reliably outran Notion's lag in production.

**Consequences:**
- Good: the workers don't read half-instantiated Notion state; EventBridge is the backstop if a wait still loses the race.
- Bad: the values are **load-bearing magic numbers** — shortening them to "speed things up" reintroduces the races they were sized to beat; they also eat into the 300 s Lambda budget (the bulk-split `sleep(10)` is the worst offender — see [01_workers.md](01_workers.md) on the duplicate-on-timeout failure mode).

**Reversibility:** Medium. The right replacement is poll-until-ready (with a ceiling) instead of fixed sleeps — but only attempt that with real Notion-latency measurements in hand, not by guessing.

> ⚠️ **Do not blindly reduce these sleeps.** They look like lazy "fix the race by waiting" hacks; they are actually calibrated to Notion's propagation delay.

---

## [D-14] Throughput caps (`MAX_CVS_PER_RUN=15`, AI-pending batch of 5) are sized to the 300 s Lambda timeout

**Choice:** A standard Harvester run processes at most `MAX_CVS_PER_RUN = 15` CVs ([harvester.py:43](../../scripts/harvester.py)), and `_reprocess_ai_pending` retries at most 5 pending candidates per run ([harvester.py:821](../../scripts/harvester.py), `pending_pages[:5]`).

**Why this won (author-confirmed):** both caps exist to keep a single run comfortably inside the **300 s Lambda execution limit**. Each CV is a download + AI parse + multi-write fan-out (Notion + Supabase + storage), so the per-item cost is high and variable; the caps bound worst-case wall-clock so a run can't be killed mid-batch by the timeout. (They are *not* primarily an OpenAI rate-limit or cost control — the binding constraint is Lambda duration.)

**Consequences:**
- Good: runs finish before the timeout; no half-processed batch from a mid-run kill.
- Bad: throughput is capped (~90 CVs/hour at the 10-min cadence — see [04_ai_pipeline.md](04_ai_pipeline.md)); a large backlog drains slowly; the two numbers are independent literals with no shared "items per run" budget.

**Reversibility:** Easy in code, but raising either without raising the Lambda timeout (or shrinking per-item cost) risks timeout-kills mid-batch. If you raise them, also revisit the D-9 try/finally behavior and the bulk-split timeout risk.

---

## [D-15] Fuzzy template match has no minimum-similarity floor — process convention guarantees an exact match

**Choice:** The Factory picks the highest-`SequenceMatcher.ratio()` `"PROCESS TEMPLATE - {suffix}"` template for a given Process Type, with **no minimum-score threshold** ([factory_worker.py:43-74](../../scripts/factory_worker.py), `_resolve_template_id_for_process_type`). It always applies the nearest match, and the template apply is destructive (`erase_content: true`).

**Why this won (author-confirmed):** by **operational convention, every Process Type already has an exactly-named template** created alongside it (see [notion-schema.md](../../.claude/rules/notion-schema.md), "Process Dashboard Templates" — adding a Process Type option *requires* creating the matching template). So in practice the top match is always a near-1.0 exact hit; a low-scoring "nearest" match never occurs. A confidence floor would guard against a situation the process is designed to prevent.

**Consequences:**
- Good: no special-casing; the logged similarity score (`Template match for '...': score=..., runner-up=...`) lets a human verify after the first run of a new type.
- Bad: the safety depends entirely on the **human convention** holding. If someone adds a Process Type *without* its template (or typos one), the Factory will silently apply the closest unrelated template and destructively erase the page with it — with only a CloudWatch log line as the signal.

**Reversibility:** Easy — add a `min_score` floor (e.g. abort + log if best ratio < 0.6) if the convention ever proves fragile. Until then it's intentional.

> ⚠️ **The invariant is "every Process Type has a same-named template."** This is enforced by process, not code. If you add Process Types programmatically, add the floor first.

---

## Footguns & assumptions (captured at handover, June 2026)

Smaller decisions that aren't full architectural choices but *will* trip up the next engineer. Recorded here so they're not "improved" into bugs.

- **The `candidate_id` key in the outcome path holds a *workflow page id*, not a candidate UUID.** `_handle_workspace_webhook` repackages the application row's `notion_page_id` as `{"candidate_id": ...}` ([main_lambda.py:136](../../main_lambda.py)), consumed in `_process_outcome_inner`. **Footgun:** the name lies. Anyone treating it as a Supabase candidate id will misuse it. Left as-is (works), but flagged — see [05_webhook_router.md](05_webhook_router.md) Tier 3.

- **`Notion-Version: "2025-09-03"` is pinned but never fully verified.** [notion_client.py:28](../../core/notion_client.py) carries a comment to the effect of "check this is correct, I assume it is." The entire two-tier data-source/database handling (see [02_notion_integration.md](02_notion_integration.md)) depends on this version string. **Status (author):** genuinely unverified — treat as a **known risk to validate** before any Notion API upgrade. If you confirm it's correct, remove the doubtful comment so it stops misleading.

- **Observer hardcodes Notion property/DB-title strings instead of using `core/constants.py`.** Literals like `"Processed"`, `"Stage"`, `"Gathered Feedback"`, `"Past Experience [AI-generated]"`, and the radar's DB-title queries live inline ([observer.py:197-203](../../scripts/observer.py) and throughout), violating the project's own "constants for property names" rule (CLAUDE.md). **Status (author):** this is **unfinished migration / cleanup debt**, not a deliberate exception — it should be lifted into `constants.py` like the rest. Until then, renaming any of these in Notion silently breaks the Observer.

- **`domain_mapper` keeps long-form fallback keys (`investment_banking`, `private_equity`, `venture_capital`).** [domain_mapper.py:103-105](../../core/domain_mapper.py) reads `raw_exp.get("ib") or raw_exp.get("investment_banking")` etc. The current `ExperienceBreakdown` AI schema only emits the short keys (`ib`/`pe`/`vc`). **Status (author): keep them** — they re-map **older stored `candidate_data` JSONB** that was written under a previous schema and may be re-mapped on a later run. Not dead code; do not delete.

- **`_find_candidate_ancestor` caps the Notion parent-walk at 8 hops** ([observer.py:1507](../../scripts/observer.py)). Rationale for *8 specifically* was not captured at handover — treat it as a safety guard against runaway walks, not a measured maximum. If an Outcome/Feedback DB is ever nested deeper, candidate attribution silently returns `None`; consider logging when the cap is hit. (Compare [F-12](../handover_audit.md#f-12-find_child_database-silently-returns-none-past-depth-4), the analogous depth-4 cap in `find_child_database`.)

- **`query_data_source` returns partial results on a mid-pagination failure** ([notion_client.py:94-117](../../core/notion_client.py)) rather than raising — consistent with the codebase's "don't crash the batch" philosophy (cf. D-9), but a truncated page list is indistinguishable from a complete one. Rationale not explicitly confirmed at handover; assume intentional, but be wary anywhere a *complete* list matters (e.g. stage parsing).

- **AI text-truncation caps (25k CV / 50k feedback / 10k per-feedback chars)** ([ai_parser.py](../../core/ai_parser.py), ~lines 181/271/365/435/444) are documented in [04_ai_pipeline.md](04_ai_pipeline.md) with their silent-loss behavior, but the *origin of the specific numbers* wasn't captured at handover. Treat them as adjustable safe caps; the model is not warned about truncation, so a very long CV silently loses its tail.
