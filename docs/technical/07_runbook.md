# 07 — Runbook

Operational reference for deploying, observing, recovering, and debugging the Lambda. Most of this is also in [CLAUDE.md](../../CLAUDE.md) and [.claude/rules/testing.md](../../.claude/rules/testing.md); this doc consolidates and adds context.

All AWS commands assume `eu-west-1` and the IAM user `nzyme-santiago-IAM` (account `416418941636`) is configured via `aws configure`.

---

## Deploy

```powershell
powershell -ExecutionPolicy Bypass -File scripts/deploy.ps1
```

What the script does ([scripts/deploy.ps1](../../scripts/deploy.ps1)):
1. Computes SHA256 of `requirements.txt`. If matches cached hash in `package/.requirements-hash`, skips pip install (saves ~30s on every deploy that doesn't change dependencies).
2. Otherwise: removes `package/`, runs `pip install -r requirements.txt -t package/ --platform manylinux2014_x86_64 --only-binary=:all: --implementation cp --python-version 3.11`, saves new hash.
3. Copies `main_lambda.py`, `core/`, `scripts/` into `package/`. Removes `package/scripts/deploy.ps1` so it doesn't end up in the zip.
4. Strips `__pycache__` directories.
5. Creates `lambda.zip` (~46 MB).
6. Uploads to `s3://nzyme-talent-engine-deploy/lambda.zip`.
7. Calls `aws lambda update-function-code --s3-bucket --s3-key` (S3 path is used because direct upload of a 46 MB zip times out).

### Verify the deploy actually replaced code

`update-function-code` returns 200 even if Lambda is still processing the new code. The authoritative check is `CodeSha256`:

```bash
# Before deploy
aws lambda get-function-configuration --function-name nzyme-talent-management \
  --region eu-west-1 --query CodeSha256 --output text
# (capture value)

# Deploy

# After deploy
aws lambda get-function-configuration --function-name nzyme-talent-management \
  --region eu-west-1 --query CodeSha256 --output text
# (capture value)
```

If the two SHAs match, the deploy did not replace the code — usually because the zip was bit-identical, or because S3 upload silently used a stale cached zip. Re-run the deploy script and check again.

### When the deploy script fails

| Symptom | Likely cause | Action |
|---|---|---|
| `pip install failed` | Missing C build tools or network | Re-run; if persistent, install Visual C++ Build Tools |
| `S3 upload failed!` | AWS credentials expired | `aws sts get-caller-identity` to check; refresh credentials |
| `Lambda update failed!` | Function in updating state | Wait 60s; re-run |
| Zip size > 50 MB | New dependency bloat | Inspect `package/` for unexpectedly large libs; consider Lambda layers |

---

## Tail logs

```bash
# Last 5 minutes, follow live
aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 5m --follow

# Filter for a specific worker
aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 30m \
  --filter-pattern '"[Harvester]"'

# Errors only
aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
  --filter-pattern 'ERROR'

# Find a specific candidate processing
aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
  --filter-pattern '"<candidate-name>"'
```

Every log line is prefixed with `[LEVEL] [Module] [request_id]` (see [core/logger.py:47-49](../../core/logger.py)). The request ID is set by `set_request_id(context.aws_request_id)` at the top of `lambda_handler` — use it to correlate log lines from the same invocation when warm-start log streams are interleaved.

To bump verbosity for one invocation: edit env var `LOG_LEVEL=DEBUG` (then back to INFO when done — DEBUG produces ~5× more output and is loud).

---

## Manually invoke the Lambda

```bash
# Force a Harvester sweep (EventBridge-style payload)
aws lambda invoke --function-name nzyme-talent-management --region eu-west-1 \
  --payload '{"task":"harvester"}' --cli-binary-format raw-in-base64-out /tmp/out.json
cat /tmp/out.json

# Other tasks: "observer", "factory"
```

For webhook-shaped invocations, prefer `curl` against the Function URL — that exercises the full router path:

```bash
curl -X POST https://vi6n7zvmytou7djtx7ixmobc4e0ittqz.lambda-url.eu-west-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "page.updated",
    "entity": {"id": "<page-uuid>", "type": "page"},
    "data": {"parent": {"id": "<db-uuid>", "type": "database"}}
  }'
```

(Note: the Function URL is gated by the shared-secret **path token** — the real URL is
`https://<function-url-host>/<WEBHOOK_PATH_TOKEN>`. A POST to the bare host (no/incorrect token) is
rejected with **401** before any routing. Include the token segment when testing, and never paste
it into shared channels. See [F-1](../handover_audit.md#f-1-webhook-ingestion-has-no-signature-validation)
and `webhooks.md` "Security Model.")

---

## Pause / resume scheduled workers

When debugging locally, you usually want to prevent EventBridge from racing with your manual runs:

```bash
aws events disable-rule --name nzyme-harvester-schedule --region eu-west-1
aws events disable-rule --name nzyme-observer-schedule --region eu-west-1
aws events disable-rule --name nzyme-factory-schedule --region eu-west-1

# Re-enable when done
aws events enable-rule --name nzyme-harvester-schedule --region eu-west-1
```

Disabling rules does **not** prevent webhook invocations — the Function URL is always live. To stop webhooks entirely, set every `WEBHOOK_*_ENABLED` flag to false (see "Changing env vars").

---

## Changing env vars

`aws lambda update-function-configuration --environment` **replaces the entire env var map**. Always GET first, modify, then PUT.

```bash
# 1. GET current env vars to a file
aws lambda get-function-configuration \
  --function-name nzyme-talent-management \
  --region eu-west-1 \
  --query 'Environment.Variables' \
  --output json > /tmp/lambda-env.json

# 2. Edit /tmp/lambda-env.json — change the value you need

# 3. PUT the full set
aws lambda update-function-configuration \
  --function-name nzyme-talent-management \
  --region eu-west-1 \
  --environment "Variables=$(cat /tmp/lambda-env.json)"

# 4. Verify
aws lambda get-function-configuration \
  --function-name nzyme-talent-management \
  --region eu-west-1 \
  --query 'Environment.Variables.<YOUR_KEY>' --output text
```

If you skip step 1, you wipe every env var — the Lambda will fail to start on next invocation (`Missing NOTION_KEY in .env`, etc.).

### Required env vars
See [.env.example](../../.env.example) for the canonical list. Critical:
- `NOTION_KEY` — Notion integration token
- `SUPABASE_URL`, `SUPABASE_KEY` — Supabase service credentials
- `OPENAI_API_KEY` — OpenAI API
- `NOTION_MAIN_DB_ID`, `NOTION_PROCESS_DASHBOARD_DB_ID`, `NOTION_GUIDELINES_DB_ID`, `NOTION_REFERENCES_DB_ID`, `NOTION_CONFIDENTIAL_DB_ID` — DB IDs
- `NOTION_ALL_TEAM_GROUP_IDS` — comma-separated permission group UUIDs for governance

Optional:
- `EXA_API_KEY` — without it, LinkedIn enrichment is disabled
- `LOGFIRE_TOKEN` — without it, OpenAI calls aren't traced beyond CloudWatch
- `LOG_LEVEL` (default `INFO`)
- `OBSERVER_LOOKBACK_MINUTES` (default `11`)

Feature flags (all default `false`):
- `WEBHOOK_PROCESS_DASHBOARD_ENABLED`
- `WEBHOOK_MAIN_CANDIDATE_ENABLED`
- `WEBHOOK_CENTRAL_REFERENCE_ENABLED`
- `WEBHOOK_WORKFLOW_ENABLED`
- `WEBHOOK_FEEDBACK_ENABLED`
- `WEBHOOK_FORM_SUBMISSION_ENABLED`
- `WEBHOOK_BULK_SUBMISSION_ENABLED`
- `WEBHOOK_OUTCOME_ENABLED`

---

## Feature flag rollout procedure

Use when flipping a `WEBHOOK_*_ENABLED` from false to true.

1. **Pick a low-traffic window** — late evening, weekends.
2. **Tail logs** in a separate terminal: `aws logs tail ... --since 5m --follow`.
3. **Enable the flag** via the GET-modify-PUT pattern above.
4. **Trigger a single test event** in Notion (e.g., for `WEBHOOK_WORKFLOW_ENABLED`: change one test candidate's Stage on a non-production test process). Confirm in logs that the webhook arrives → router resolves → handler executes → returns 200.
5. **Watch for 15-30 minutes**. Look for:
   - `[WEBHOOK] {handler_name} disabled by feature flag` → flag is not actually on (env var GET-modify-PUT bug).
   - `Error in webhook handler` → handler crashed; check stack trace.
   - Duplicate processing — both webhook and EventBridge are firing for the same page; the concurrency guards should catch it, but watch for `Application already exists` or `Stage already changed (optimistic lock)` logs as confirmation.
6. **If anything looks wrong**, flip the flag back to false. EventBridge picks up where webhooks left off.
7. **If it's stable for a few hours**, leave it on.

---

## Rollback

Lambda keeps previous versions automatically.

```bash
# List versions
aws lambda list-versions-by-function --function-name nzyme-talent-management --region eu-west-1
```

If an alias is in use:
```bash
# Roll back the alias to version N
aws lambda update-alias --function-name nzyme-talent-management \
  --name prod --function-version <N> --region eu-west-1
```

If no alias (current default):
```bash
# Re-deploy from git
git checkout <prior-commit-sha>
powershell -ExecutionPolicy Bypass -File scripts/deploy.ps1
git checkout main  # or wherever you were
```

(The repo's current deploy doesn't use aliases — every deploy updates `$LATEST` directly. There's no per-version invocation routing.)

---

## Common incident playbooks

### "A candidate didn't appear in Notion"

1. **Was the Form entry created?** Check the Form DB in Notion. If yes, continue. If no, the Notion native automation (Form → Workflow page) didn't fire — check Notion's automation panel for that DB.
2. **Was the Workflow page created?** Open the relevant process's Workflow DB. Filter to recent entries. If the page exists with `Processed=false`, the Harvester hasn't picked it up yet — wait for the next EventBridge sweep (≤10 min) or manually invoke with `{"task":"harvester"}`.
3. **Is the Workflow page `Processed=true` but no Main DB entry?** Processing happened but failed. Tail logs filtered by the candidate name:
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern '<candidate-name>'
   ```
   Look for `Notion {update|create} FAILED` or `Failed to manage candidate`. The candidate is in limbo: Workflow page closed, no Main DB record. Recovery: uncheck `Processed` on the Workflow page, re-run Harvester. The duplicate-application guard will resolve correctly.
4. **No log entries at all?** The Lambda was never invoked for this page. Check that `WEBHOOK_FORM_SUBMISSION_ENABLED=true`; if not, the EventBridge sweep is the only path (10-minute lag).
5. **Logs show "Application already exists" but candidate isn't visible?** Check the Main DB filtered by name. The candidate likely got merged into an existing record (identity resolution rule 2/3/4). See [F-9](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people).

### "Stage change didn't sync to Supabase"

1. Open the Workflow page; confirm the Stage was actually changed.
2. Query Supabase:
   ```sql
   SELECT current_stage FROM "NzymeRecruitingApplications"
   WHERE notion_page_id = '<workflow-page-id>';
   ```
   If stages match — sync happened. Done.
3. If they don't match: check logs filtered by `[WORKFLOW]`:
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern '"[WORKFLOW]"'
   ```
   Look for `Stage change: ...` log. If absent, the Observer never saw the change — either the webhook flag is off, or the Observer sweep hadn't run yet (10-min cadence + 11-min lookback).
4. Manually invoke: `aws lambda invoke ... --payload '{"task":"observer"}'`.
5. Re-check Supabase. If still mismatched, run the optimistic-lock query directly:
   ```sql
   SELECT id, current_stage, updated_at FROM "NzymeRecruitingApplications"
   WHERE notion_page_id = '<workflow-page-id>';
   ```
   Compare to the Notion stage character-by-character (watch for ZWSPs — see [F-11](../handover_audit.md#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant)).

### "A process won't initialize"

1. Check the Process Dashboard page properties: `Process Type` set? `Processed [Do not touch]` = false?
2. Tail logs for `[FactoryWorker]`:
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 30m \
     --filter-pattern '"[FactoryWorker]"'
   ```
3. Look for:
   - `Template match for '...': '<template name>' (score=<low>, runner-up=<high>)` — fuzzy match picked a wrong template. Solution: rename either the template or the process type.
   - `No template resolved for Process Type` — no `PROCESS TEMPLATE - {suffix}` matched. Add the template.
   - `Main child DBs not found (Workflow/Form)` — template applied but Notion didn't propagate the child DBs within 38s. Wait, then re-trigger by setting `Processed=false`.
   - `Failed to register process` — Supabase duplicate constraint, almost always means the process was already registered. Check Supabase `NzymeRecruitingProcesses` for matching `process_name`.
4. Manual recovery: `aws lambda invoke ... --payload '{"task":"factory"}'` and watch logs.

### "Bulk import dropped half the candidates"

1. Identify the Bulk DB entry. Was it marked `Processed=true`?
2. If yes but only some Form entries were created: the Lambda timed out mid-loop (10s sleep × N files; >27 files exceeds 300s).
3. **Don't uncheck the batch's Processed checkbox** — that creates duplicates for files that were already split.
4. Instead, manually upload the missing files to the Form DB.
5. Long-term fix: see [F-8](../handover_audit.md#f-8-bulk-import-auto-tags-every-candidate-as-headhunter) and the bulk-split refactor needed.

### "OpenAI quota exhausted"

1. Symptom: many candidates marked `AI Pending=true`.
2. Wait for quota refresh / increase limits.
3. Reprocessing happens automatically:
   - Harvester sweep processes 5 AI-pending per run; with 6 runs/hour, that's 30/hour throughput.
   - Observer-triggered reprocessing fires when a recruiter edits a Main DB page (typical: they upload a CV after the fact).
4. To force aggressive reprocessing: temporarily increase the batch size at [harvester.py:743](../../scripts/harvester.py) from 5 to a higher number, redeploy, watch logs, revert.

---

## Recovery: manually fix a stuck Workflow page

If a Workflow page is in a bad state (e.g., `Processed=true` but no Main DB record, or `AI Pending=true` but the CV was never attached):

1. **Find the candidate UUID** in Supabase:
   ```sql
   SELECT id, name, notion_page_id, candidate_data->>'ai_pending' AS ai_pending
   FROM "NzymeTalentNetwork"
   WHERE name ILIKE '%<partial-name>%';
   ```
2. **Find the application:**
   ```sql
   SELECT a.id, a.current_stage, p.process_name
   FROM "NzymeRecruitingApplications" a
   JOIN "NzymeRecruitingProcesses" p ON a.process_id = p.id
   WHERE a.candidate_id = '<candidate-uuid>';
   ```
3. **Decide what's wrong** — usually the candidate has a Main DB entry but no Workflow link, or vice versa.
4. **In Notion**, uncheck `Processed` and (if applicable) `AI Pending` on the affected page.
5. **Manually invoke** the relevant worker:
   ```bash
   aws lambda invoke --function-name nzyme-talent-management --region eu-west-1 \
     --payload '{"task":"harvester"}' --cli-binary-format raw-in-base64-out /tmp/out.json
   ```
6. Tail logs to confirm reprocessing.
7. If Supabase has duplicate candidate rows (rare, see [F-9](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people)), the audit/cleanup procedure in [.claude/rules/testing.md](../../.claude/rules/testing.md) ("Cleanup Procedure") shows the FK-aware delete order.

---

## Health checks

There's no built-in health endpoint. Quick smoke tests:

```bash
# Lambda config sanity
aws lambda get-function-configuration --function-name nzyme-talent-management \
  --region eu-west-1 --query '{State, LastUpdateStatus, CodeSha256}'

# EventBridge rules are enabled
for rule in nzyme-harvester-schedule nzyme-observer-schedule nzyme-factory-schedule; do
  aws events describe-rule --name $rule --region eu-west-1 --query '{State, ScheduleExpression}'
done

# Recent invocation count (last hour)
aws logs filter-log-events --log-group-name /aws/lambda/nzyme-talent-management \
  --region eu-west-1 --start-time $(date -d '1 hour ago' +%s)000 \
  --filter-pattern 'INVOCATION START' --query 'length(events)'

# Recent errors (last hour)
aws logs filter-log-events --log-group-name /aws/lambda/nzyme-talent-management \
  --region eu-west-1 --start-time $(date -d '1 hour ago' +%s)000 \
  --filter-pattern '[CRITICAL]' --query 'length(events)'
```

For a Notion-side health check:
```bash
python tools/notion_schema.py  # prints the Main DB schema
```
If this fails, `NOTION_KEY` is invalid or the integration has been removed from the database.

For Supabase: run a simple query via the MCP integration (project `yphbrpbwpakjduhmoimw`) — `SELECT count(*) FROM "NzymeRecruitingProcesses"`. If it returns a number, Supabase credentials are alive.

---

## Version tagging convention

After every major deploy:
```bash
git tag -a v0.X.0 -m "Short description of changes"
git push origin main --tags
```
Tag format: `vMAJOR.MINOR.PATCH`. Increment MINOR for features, PATCH for bugfixes.

There's no continuous deployment — every release is manual. The tag is the only marker of what was deployed when.
