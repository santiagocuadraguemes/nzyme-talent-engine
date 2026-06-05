# 05 — Webhook Router

The webhook router ([core/webhook_router.py](../../core/webhook_router.py)) decides what to do with incoming Notion webhook POSTs. It's a small file (~195 lines) but it's the linchpin: every event-driven write into Notion flows through it.

This doc explains the three payload shapes, the three-tier resolution strategy (static → dynamic → application registry), how feature flags gate dispatch, and how to safely add a new handler.

---

## End-to-end webhook flow

```
Notion webhook arrives
        │
        ▼
[1] main_lambda.lambda_handler detects HTTP (CASE C)
        │
        ▼
[2] WebhookRouter() — instantiated WITHOUT Supabase client
        │
        ▼
[3] router.parse_event(event)
        │   ├─ JSON parse the body (string or dict)
        │   ├─ Check for "challenge" key → handshake
        │   ├─ Sniff payload shape (A, B, or C)
        │   └─ Extract: page_id, database_id, event_type, source meta
        │
        ▼
[4] router.resolve_handler(database_id)
        │   ├─ Static registry: env var DB IDs (no Supabase call)
        │   └─ (returns handler_name + None as process_context)
        │
        ▼
[5] If static missed:
        │   ├─ Initialize SupabaseManager
        │   ├─ Attach to router.supa
        │   ├─ router.resolve_handler(database_id) AGAIN
        │   │     ├─ Dynamic registry: Supabase OR across 4 columns on Processes
        │   │     │     (returns handler + the process row as process_context)
        │   │     └─ Application registry: lookup by notion_outcome_id
        │   │           (returns HANDLER_OUTCOME_FORM + the application row)
        │   └─ Return (handler_name, process_context)
        │
        ▼
[6] _handle_workspace_webhook(handler_name, page_id, process_context)
        │   ├─ Feature flag check: _is_webhook_enabled(handler_name)
        │   │     └─ If disabled: return 200, body "Handler {x} disabled"
        │   └─ Dispatch to specific worker method
        │
        ▼
[7] Worker handles the event, returns 200/500
```

---

## Step 3: `parse_event` — three payload shapes

Notion sends webhooks in three different formats. Each is detected by a distinct discriminator.

### Shape A — Native workspace webhook
Sent by Notion's native workspace-level webhook subscriptions.

```json
{
  "type": "page.updated",
  "entity": {"id": "<page-id>", "type": "page"},
  "data": {"parent": {"id": "<db-id>", "type": "database"}}
}
```
**Discriminator:** `entity.type == "page"` ([webhook_router.py:100](../../core/webhook_router.py))
**Page ID source:** `entity.id`
**Database ID source:** `data.parent.id` (when `parent.type == "database"`) or `data.parent.database_id`

### Shape B — Fallback / legacy
Catchall for payloads that don't match A or C.

```json
{
  "data": {"id": "<page-id>", "parent": {"database_id": "<db-id>"}}
}
```
**Discriminator:** neither `entity.type == "page"` nor `source.type == "automation"`
**Page ID source:** `data.id`
**Database ID source:** `data.parent.database_id` or `data.parent.id` if `parent.type == "database"`

### Shape C — Automation webhook
Sent by Notion automations configured with a "Send webhook" action.

```json
{
  "source": {"type": "automation", "automation_id": "<uuid>"},
  "event_id": "<uuid>",
  "data": {
    "object": "page",
    "id": "<page-id>",
    "parent": {"type": "data_source_id", "database_id": "<db-id>"},
    "properties": { /* inline page properties */ }
  }
}
```
**Discriminator:** `source.type == "automation"` ([webhook_router.py:88](../../core/webhook_router.py))
**Page ID source:** `data.id`
**Database ID source:** `data.parent.database_id`
**Note:** Shape C includes `properties` inline. Workers re-fetch the page anyway for consistency — the inline properties are currently unused.

### Challenge handshake
Special-cased before any shape detection ([webhook_router.py:77-80](../../core/webhook_router.py)):
```json
{"challenge": "<token>"}
```
Lambda echoes the token back in the response body, completing Notion's URL verification.

---

## Step 4 & 5: Three resolution tiers

`resolve_handler(database_id)` returns a tuple `(handler_name, process_context)`. It tries three sources in order; each later tier requires a Supabase client.

### Tier 1: Static registry (no DB call)

Built from env vars at router init time ([webhook_router.py:33-44](../../core/webhook_router.py)):

| Env var | Handler |
|---|---|
| `NOTION_MAIN_DB_ID` | `HANDLER_MAIN_CANDIDATE` |
| `NOTION_PROCESS_DASHBOARD_DB_ID` | `HANDLER_PROCESS_DASHBOARD` |
| `NOTION_REFERENCES_DB_ID` | `HANDLER_CENTRAL_REFERENCE` |

Keys are normalized (dashes stripped) for format-agnostic comparison. Process context is None for static hits — these are workspace-level DBs not tied to a specific process.

### Tier 2: Dynamic registry (Supabase, Processes table)

If static missed and a Supabase client is attached, `_classify_process_db` ([webhook_router.py:179-193](../../core/webhook_router.py)) queries `NzymeRecruitingProcesses` and matches the database_id against one of four columns:

| Column | Handler |
|---|---|
| `notion_workflow_id` | `HANDLER_WORKFLOW_ITEM` |
| `notion_feedback_id` | `HANDLER_FEEDBACK_FORM` |
| `notion_form_id` | `HANDLER_FORM_SUBMISSION` |
| `notion_bulk_id` | `HANDLER_BULK_SUBMISSION` |

Order matters: the first column to match wins. If for some pathological reason two columns held the same ID, workflow would take priority.

The query filters by `status='Open'` ([supabase_client.py:510](../../core/supabase_client.py)) — closed processes are intentionally ignored. To re-enable a closed process's webhooks, set `status='Open'` in Supabase first.

Process context on dynamic hit = the full process row (passed to handlers, used for matrix_characteristics, governance_people, headhunter_name, etc.).

### Tier 3: Application registry (Supabase, Applications table)

If dynamic also missed, last-resort lookup against `NzymeRecruitingApplications.notion_outcome_id` ([webhook_router.py:167-173](../../core/webhook_router.py)). On hit, handler is always `HANDLER_OUTCOME_FORM` and process_context is the application row (which has `notion_page_id` = the workflow page ID; `_handle_workspace_webhook` repackages this as `{"candidate_id": notion_page_id}` at [main_lambda.py:136-138](../../main_lambda.py)).

> ⚠️ **Footgun:** that repackaged dict key is literally named `candidate_id`, but it holds the
> **workflow page id**, not a Supabase candidate UUID. `_process_outcome_inner` and its helpers use
> it as a Notion page id. Don't pass it where a candidate id is expected. (Left as-is because it
> works; flagged so you don't trust the name.)

This tier exists because Outcome Form DBs are per-application — there isn't one per process, there's one per workflow page. There's no central registry of Outcome DB IDs in Notion; we record them in Supabase as candidates are processed ([harvester.py:626-631](../../scripts/harvester.py)). The [tools/backfill_outcome_ids.py](../../tools/backfill_outcome_ids.py) script backfills for applications created before this column existed.

### Tier 4: implicit fallback — "Unrecognized database"

If all three tiers miss, `resolve_handler` returns `(None, None)` and `main_lambda` returns 200 with body `"Unrecognized database"` ([main_lambda.py:248-249](../../main_lambda.py)). The webhook is acknowledged but silently dropped.

---

## Why the two-pass routing in `lambda_handler`

[main_lambda.py:233-243](../../main_lambda.py) calls `resolve_handler` twice:

```python
# Try static resolution first (no DB client needed)
handler_name, process_ctx = router.resolve_handler(database_id)

# Dynamic resolution if static didn't match
if not handler_name:
    s_client = SupabaseManager()
    router.supa = s_client
    handler_name, process_ctx = router.resolve_handler(database_id)
```

The reason: Supabase client init is ~50ms (connection setup, JWT decode). For Main DB / Dashboard / References webhooks — which are very common — that's wasted latency. Skipping it for static hits halves cold-path webhook time.

The downside: the code reads as if there are two separate resolution methods, when really it's the same method called with vs without the supa attribute set.

---

## Step 6: feature flags

Every handler has a corresponding env var that gates dispatch ([main_lambda.py:33-43](../../main_lambda.py)):

| Handler | Flag | Default |
|---|---|---|
| `HANDLER_PROCESS_DASHBOARD` | `WEBHOOK_PROCESS_DASHBOARD_ENABLED` | `false` |
| `HANDLER_MAIN_CANDIDATE` | `WEBHOOK_MAIN_CANDIDATE_ENABLED` | `false` |
| `HANDLER_CENTRAL_REFERENCE` | `WEBHOOK_CENTRAL_REFERENCE_ENABLED` | `false` |
| `HANDLER_WORKFLOW_ITEM` | `WEBHOOK_WORKFLOW_ENABLED` | `false` |
| `HANDLER_WORKFLOW_ITEM` (intake sub-path) | `WEBHOOK_WORKFLOW_INTAKE_ENABLED` | `false` |
| `HANDLER_FEEDBACK_FORM` | `WEBHOOK_FEEDBACK_ENABLED` | `false` |
| `HANDLER_FORM_SUBMISSION` | `WEBHOOK_FORM_SUBMISSION_ENABLED` | `false` |
| `HANDLER_BULK_SUBMISSION` | `WEBHOOK_BULK_SUBMISSION_ENABLED` | `false` |
| `HANDLER_OUTCOME_FORM` | `WEBHOOK_OUTCOME_ENABLED` | `false` |

All default to `false`. When a flag is off, the webhook is acknowledged with 200 + body `"Handler {x} disabled"` and the EventBridge schedule is responsible for picking up the change later.

**The Workflow handler is three-way.** A `page.created`/`page.updated` on a Workflow DB always
routes to `HANDLER_WORKFLOW_ITEM` (the router keys on the database, not the event type). The
Observer then disambiguates:
1. **New-candidate intake** — `Processed=false` and no application row yet. Hands off to
   `HarvesterRelational.process_workflow_intake()` (applies the Workflow template + processes the
   CV). This sub-path is gated by its **own** flag `WEBHOOK_WORKFLOW_INTAKE_ENABLED`, which is
   nested inside `WEBHOOK_WORKFLOW_ENABLED` — *both* must be `true` for form-direct intake to run.
   When intake is off, the event falls through to (2)/(3), which no-op for a page with no app row.
2. **Assessment request** — "Assessment Requested" checkbox set → feedback assessment.
3. **Stage change** — otherwise, compare Stage against the stored application stage.

Only the webhook path can trigger intake; the scheduled Observer sweep never calls
`handle_webhook_event` (the Harvester's Step 2.5 is the EventBridge safety net for direct-entry
candidates). See [.claude/rules/webhooks.md](../../.claude/rules/webhooks.md) and
[.claude/rules/architecture.md](../../.claude/rules/architecture.md) "Direct Ingestion & Form-Direct
Intake."

### Why feature-flag every handler

When the workspace webhooks were rolled out, each handler had to be individually validated against production data to confirm it wouldn't double-process or corrupt state. The flag pattern lets you:
- Roll out one handler at a time and observe its behavior in CloudWatch.
- Roll back any one handler instantly by setting its flag to false (env var update, no redeploy).
- Run in EventBridge-only mode for incident recovery without disabling webhook ingest entirely.

### Flipping a flag in production

Env vars are mutable via `aws lambda update-function-configuration --environment`. **The gotcha (also noted in CLAUDE.md):** this command replaces the *entire* env var map, so always:

```bash
# 1. GET first
aws lambda get-function-configuration \
  --function-name nzyme-talent-management \
  --region eu-west-1 \
  --query 'Environment.Variables' > /tmp/env-current.json

# 2. Modify in place (set the flag you want)
# ... edit /tmp/env-current.json ...

# 3. PUT the full set
aws lambda update-function-configuration \
  --function-name nzyme-talent-management \
  --region eu-west-1 \
  --environment "Variables=$(cat /tmp/env-current.json)"
```

Forgetting step 1 will wipe every other env var, including `NOTION_KEY`, `SUPABASE_KEY`, all the DB IDs, and the other feature flags. The Lambda will fail to initialize on next invocation.

There's no env-var validation in this codebase — wrong/missing values surface as `ValueError("Missing ...")` from constructor calls or as KeyError accessing `os.getenv(...)` for required keys. The Factory and Harvester will silently process nothing if `NOTION_PROCESS_DASHBOARD_DB_ID` becomes None.

---

## Adding a new handler safely

A worked example: imagine adding a "Hot Candidates" DB that triggers a re-rank action.

### Steps
1. **Add a handler name constant** in [core/constants.py](../../core/constants.py):
   ```python
   HANDLER_HOT_CANDIDATES = "hot_candidates"
   ```
2. **Add the feature flag** in [main_lambda.py:33-43](../../main_lambda.py):
   ```python
   HANDLER_HOT_CANDIDATES: "WEBHOOK_HOT_CANDIDATES_ENABLED",
   ```
3. **Choose your registry tier:**
   - If the DB is workspace-level (one fixed ID): add to the static registry in `WebhookRouter._build_static_registry` with a new env var like `NOTION_HOT_CANDIDATES_DB_ID`.
   - If the DB is per-process: add a new column on `NzymeRecruitingProcesses` (e.g., `notion_hotlist_id`), update `_classify_process_db` to check it, and have the Factory populate it.
   - If the DB is per-application: follow the Outcome Form pattern — add column on `NzymeRecruitingApplications`, add a lookup method to `SupabaseManager`, add a tier-3 check to `resolve_handler`.
4. **Add the dispatch case** in `_handle_workspace_webhook` ([main_lambda.py:74-172](../../main_lambda.py)):
   ```python
   if handler_name == HANDLER_HOT_CANDIDATES:
       # ... initialize whatever clients you need ...
       worker.handle_hot_candidate(page_id, process_context)
       return {"statusCode": 200, ...}
   ```
5. **Implement the handler method** on whichever worker owns the logic (Harvester / Observer / Factory).
6. **Add `WEBHOOK_HOT_CANDIDATES_ENABLED=false`** to [.env.example](../../.env.example) so other devs know the flag exists.
7. **Wire up the Notion automation** in the Notion UI: select the DB, add a "Send webhook" automation, target the Lambda Function URL.
8. **Update the docs:** add the handler row to [.claude/rules/webhooks.md](../../.claude/rules/webhooks.md) and the table in [02_notion_integration.md](02_notion_integration.md).

### Rollout

1. Deploy code with `WEBHOOK_HOT_CANDIDATES_ENABLED=false` (default).
2. In Notion, configure the automation but disable it.
3. Test locally via `fake_workspace_webhook` ([main_lambda.py:329-340](../../main_lambda.py)) to verify routing.
4. In Notion, enable the automation. Trigger one event manually. Confirm webhook arrives → router parses → "Handler hot_candidates disabled" log appears.
5. Set `WEBHOOK_HOT_CANDIDATES_ENABLED=true` via `update-function-configuration` (GET-modify-PUT).
6. Watch CloudWatch for ~30 minutes. If anything goes wrong, set flag back to false.

---

## Security model

The Lambda Function URL is publicly reachable, and Notion *automation* webhooks are **unsigned**
(no `X-Notion-Signature`), so HMAC signature verification is impossible for this emitter. Auth is
instead a **shared-secret URL path token**: the secret lives in the request path
(`https://<function-url-host>/<WEBHOOK_PATH_TOKEN>`), transmitted over TLS, and is checked by
`verify_path_token()` ([webhook_router.py:39](../../core/webhook_router.py)) at the very top of the
HTTP branch of `lambda_handler` ([main_lambda.py:202-210](../../main_lambda.py)) — **before** the
challenge handshake and any routing. Mismatch → HTTP 401 before any work.

The gate **fails closed**: if `WEBHOOK_PATH_TOKEN` is unset/empty, *every* Function-URL request is
rejected (loud `ERROR` log). There is no "accept everything" fallback — a missing secret never
degrades open. EventBridge schedules have no HTTP path and are unaffected.

This resolves the original [F-1](../handover_audit.md#f-1-webhook-ingestion-has-no-signature-validation)
("no auth gate"). **Residual gaps (by design):** no replay protection (a captured valid URL+body can
be replayed — mitigated by TLS + handler idempotency guards), and rotation is manual (regenerate the
token, update the Lambda env var, re-paste the new URL into every Notion automation). Full design,
including secret hygiene and the planned move to SSM/Secrets Manager, is in
[.claude/rules/webhooks.md](../../.claude/rules/webhooks.md) ("Security Model — Shared-Secret URL
Path Token") and CLAUDE.md.
