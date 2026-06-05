# 03 — When Things Break

A symptom-first guide. You're triaging at 9pm; this is what you check, in what order, for the things most likely to go wrong.

For each symptom: what it looks like, where to look first, common causes, what to do.

---

## "A candidate's CV was submitted but doesn't appear in the Talent Network"

### What you see
- A teammate says they submitted Pablo's CV an hour ago. You search the Main DB for "Pablo" — nothing.
- Or: the candidate is in the Workflow DB for the process, but their data is half-empty.

### Where to look first
1. **The Form DB for that process.** Is there a row with Pablo's name? If yes — Form submission worked, move to step 2. If no — the form upload itself failed; have the submitter try again.

2. **The Workflow DB for that process.** Is there a row with Pablo's name and a populated `ID` field?
   - **No row:** Notion's Form-to-Workflow automation didn't fire. Check Notion's automation panel for that process (someone may have paused it). Until that's fixed, candidates submitted to this form aren't reaching the engine. Manually create the Workflow entry as a stopgap.
   - **Row exists but `Processed=false`:** The engine hasn't picked it up yet. Wait up to 10 minutes (next Harvester sweep). If still nothing, jump to step 3.
   - **Row exists, `Processed=true`, but Pablo isn't in the Main DB:** Processing happened but failed silently. Jump to step 4.

3. **CloudWatch logs filtered to the Harvester.**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 30m \
     --filter-pattern '"[Harvester]"'
   ```
   Look for log lines mentioning Pablo or the process name. If completely silent, the Harvester didn't run for this candidate.

4. **Logs filtered to Pablo's name** (last 1 hour):
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern 'Pablo'
   ```
   Common error patterns:
   - `Notion update FAILED` / `Notion create FAILED` — engine tried to write Pablo's record but Notion rejected it. Look for the status code; 400 means malformed payload (probably a Notion schema mismatch), 401/403 means the Notion integration lost access.
   - `Match by Name: 'Pablo Rodríguez'. Assuming same candidate (Merge).` — engine merged Pablo into an existing record. Check if there's already a "Pablo Rodríguez" in the Main DB; this is a feature, not a bug, but might be the wrong person.
   - No errors at all — the candidate is somewhere; check filters on the Main DB view (someone may have applied a view filter that hides Pablo).

### What to do
- **Form-to-Workflow automation paused:** unpause in Notion's automation panel. Re-create the affected Workflow entries manually.
- **Wrong-person merge:** in Supabase, find the candidate row, change its `notion_page_id`, and create a new Main DB page for the right Pablo. Then re-trigger ingestion by unchecking `Processed` on the Workflow page.
- **AI failed:** the candidate is probably visible in the Main DB but with `AI Pending=true` and no experience data. The engine retries automatically on the next sweep. If urgent, manually invoke the Harvester (see runbook).
- **Stalled, no error:** check that the EventBridge rules are enabled. Look at `nzyme-harvester-schedule` in AWS — if it's disabled, that explains the lack of action.

### Who might know
- If it's a Notion automation problem, anyone with Notion admin access can pause/unpause.
- If it's an AWS problem, you need Lambda/CloudWatch access.

---

## "A stage change isn't showing up in the analytics"

### What you see
- A recruiter moved a candidate from "1.1 Fit Interview" to "1.2 Final Interview" half an hour ago. The analytics dashboard still shows them at "1.1".

### Where to look first
1. **Open the candidate's workflow page.** Confirm the Stage actually changed. (Sometimes Notion's autosave fails — the dropdown shows the new value but it didn't save.)

2. **Query Supabase directly:**
   ```sql
   SELECT current_stage, updated_at FROM "NzymeRecruitingApplications"
   WHERE notion_page_id = '<workflow-page-id>';
   ```
   If the value matches Notion — sync happened, the dashboard is stale (refresh it). If not — proceed.

3. **CloudWatch logs filtered to the workflow handler:**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern '"[WORKFLOW]"'
   ```
   Look for `Stage change: ... -> ...` for this candidate. If absent, the Observer never saw the change.

### Common causes
- **Webhook flag off.** If `WEBHOOK_WORKFLOW_ENABLED=false`, stage changes only sync via the 10-minute Observer sweep. Wait or enable the flag.
- **Lookback window missed it.** The Observer's sweep looks back 11 minutes. If a change happened 12 minutes before the sweep, it's missed. Next sweep catches it. (This shouldn't happen often — windows are sized to overlap.)
- **The two stage strings actually match.** Sometimes Notion's stage and Supabase's stage *look* the same but aren't (zero-width-space characters — see [04_glossary.md](04_glossary.md) under "ZWSP"). If you've been editing stage options manually in Notion, you may have lost the ZWSP prefix. The fix is to copy the option name from a candidate whose stage *did* sync (paste includes the invisible characters).

### What to do
- Run the Observer manually:
  ```bash
  aws lambda invoke --function-name nzyme-talent-management --region eu-west-1 \
    --payload '{"task":"observer"}' --cli-binary-format raw-in-base64-out /tmp/out.json
  ```
  Re-check Supabase.
- If still mismatched, ask an engineer to compare the strings character-by-character (look for ZWSPs).

---

## "A new process isn't getting set up"

### What you see
- Someone created a new page in the Process Dashboard. It's been there 30 minutes. `Processed [Do not touch]` is still false. The four child databases (Workflow / Form / Bulk / Feedback) didn't appear.

### Where to look first
1. **Process Dashboard page properties.** Is `Process Type` filled in? (The Factory only acts on pages with a Process Type set.)

2. **CloudWatch logs filtered to the Factory:**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 30m \
     --filter-pattern '"[FactoryWorker]"'
   ```
   Look for log lines mentioning the new process's name.

### Common causes
- **No matching template.** Log: `No template resolved for Process Type 'XYZ'`. Add a `PROCESS TEMPLATE - XYZ` template in the Process Dashboard's template list, then unfortunately you also have to delete the half-set-up page and re-create it (or wait for the next hourly Factory tick).
- **Wrong template picked.** Log: `Template match for 'PortCo - Senior CFO': 'PROCESS TEMPLATE - PortCo - CFO' (score=0.84, runner-up=0.83)`. The fuzzy match was close — it picked CFO when it should have picked Senior CFO. If you intended Senior CFO, rename or add the right template.
- **Notion didn't propagate child DBs in time.** Log: `Main child DBs not found (Workflow/Form). Check the template.` The template was applied, but Notion took >40 seconds to materialize the child DBs. Recovery: manually uncheck `Processed [Do not touch]` and wait for the next hourly Factory tick.
- **The template itself is broken.** It applied but doesn't include the expected child DBs. Open the template in Notion's template editor and check that it has Workflow + Form + Bulk + Feedback + JD + Interview Stages.

### What to do
- For missing template: create it (named exactly `PROCESS TEMPLATE - {Process Type}`), then uncheck `Processed [Do not touch]` on the stuck process to trigger another attempt.
- For wrong template picked: rename one to be unambiguously distinct, then re-attempt.
- For Notion delay: just retry (uncheck `Processed`, wait 5-10 min).

---

## "Bulk import dropped some candidates"

### What you see
- Someone uploaded 25 CVs in one batch. Only 18 became Form entries.

### Where to look first
1. **CloudWatch logs filtered to "Bulk":**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern 'Bulk'
   ```
2. Look for `Splitting N batches in '<process>'` and then a `Bulk: split file '...' -> new Form entry created` line per file. Count them.
3. If the count is lower than expected, look for the *last* "split file" log — that's where it stopped.

### Common cause
- **Lambda hit its 5-minute timeout** mid-batch. The engine sleeps 10 seconds between each file (to let Notion's per-file automations fire sequentially). 25 files × 10 seconds = 250 seconds of sleep + actual upload time → easy timeout.

### What to do
- **Do NOT uncheck `Processed` on the bulk batch page.** That would cause re-splitting of the files that DID succeed, creating duplicates.
- Manually upload the missing files to the Form DB (one by one or in a smaller bulk batch).
- Long-term: split bulk uploads into batches of ≤25 files.

---

## "OpenAI quota exhausted — lots of AI Pending candidates"

### What you see
- The Main DB has many candidates with `AI Pending=true` and no experience data filled in.

### Where to look first
1. **CloudWatch logs filtered to OpenAI errors:**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern '"OpenAI CV parsing error"'
   ```
   You'll likely see `429 Too Many Requests` or quota-exhausted messages.

### What to do
- Check the OpenAI account dashboard — is the quota actually exhausted, or is it the rate-limit-per-minute?
- If quota: increase the limit in OpenAI's billing settings.
- If rate-limit: wait. The engine retries pending candidates automatically — 5 per Harvester run, 6 runs per hour = 30 candidates/hour recovery rate.
- The Observer also retries when a candidate's Notion page is edited (e.g., a recruiter adds a missing CV). That helps clear the queue too.

---

## "A reference was submitted but didn't appear under the candidate"

### What you see
- A referrer filled in the central References DB an hour ago. The candidate's workflow page doesn't show the reference in their "Candidate References" child DB.

### Where to look first
1. **Confirm the candidate's name in the reference is spelled correctly.** The engine uses name+email matching; a typo in either fails.
2. **Check the Reference DB row.** Is `Processed=true`? If yes — the engine processed it (possibly to a different candidate). If no — engine hasn't seen it.
3. **CloudWatch logs:**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h \
     --filter-pattern '"[CENTRAL_REF]"'
   ```
   Look for log lines about resolving the candidate.

### Common causes
- **Name typo.** Log: `Identity not resolved for reference: '<name>'`. Fix the name on the reference page, uncheck Processed, retry.
- **Candidate has no active applications.** Log: `<some message about no applications>` (actually, the engine just sees zero active apps and silently returns). The candidate exists in the Main DB but has no Workflow entry tagged Active. References can only be distributed to active applications.
- **Child DB at deep nesting on one of the candidate's workflows.** Log: `global_success = False`. The engine could find the child DB on some workflows but not others; the whole reference is marked failed (will retry on next sweep, creating duplicates in the workflows where it succeeded).

### What to do
- Fix the typo or candidate-process linkage.
- If the deep-nesting issue: flatten the workflow page's structure (move the child DB out of nested toggles/columns).

---

## "Confidential candidate is visible to too many people"

### What you see
- A candidate in a Confidential process is showing up in views accessed by people who shouldn't see them.

### Where to look first
1. **Open the candidate's Main DB page.** Check the `Governance: Edit & View Access` field. Is it set to the right people / groups?
2. **Check the Notion permission rule on the Main DB.** This is a permission rule configured in Notion's UI — go to the Main DB's settings, then Permissions. There should be a rule keyed on `Governance: Edit & View Access` restricting page visibility.

### Common causes
- **Governance field empty or contains team-wide groups instead of individuals.** This shouldn't happen for confidential candidates (the Harvester restricts governance to the process's people list). If it did, either the process was misconfigured at Factory time (Process Visibility wasn't set to "Confidential") or someone manually edited the candidate's governance field.
- **The Notion permission rule was deleted or modified.** Without the rule, the governance field is just metadata — Notion enforces nothing.
- **Env var `NOTION_ALL_TEAM_GROUP_IDS` includes the wrong groups.** Used only for non-confidential candidates, but if a non-confidential candidate is in a different view (where they shouldn't appear), this might be the cause.

### What to do
- Verify the Notion permission rule exists and is correctly configured.
- If the governance field is wrong, fix it manually. Don't expect the engine to detect/repair this — the engine only writes the field at ingest and on process close.

---

## "The engine doesn't seem to be running at all"

### What you see
- Nothing is happening. New CVs aren't being processed. Stage changes aren't syncing. The whole system feels dead.

### Where to look first
1. **CloudWatch logs — any activity in the last hour?**
   ```bash
   aws logs tail /aws/lambda/nzyme-talent-management --region eu-west-1 --since 1h
   ```
   If completely empty, the Lambda hasn't been invoked.
2. **Are the EventBridge rules enabled?**
   ```bash
   aws events describe-rule --name nzyme-harvester-schedule --region eu-west-1
   ```
   Look for `"State": "ENABLED"`.
3. **Lambda function state:**
   ```bash
   aws lambda get-function-configuration --function-name nzyme-talent-management \
     --region eu-west-1 --query '{State, LastUpdateStatus}'
   ```
   If `State: Failed` — the last deploy is broken; roll back.

### Common causes
- **EventBridge rules disabled.** Someone disabled them for debugging and forgot to re-enable. Re-enable them.
- **Lambda is in `Failed` state.** A recent deploy broke the function (probably a syntax error or import error). Roll back to the previous version.
- **Env vars were wiped.** If `aws lambda update-function-configuration` was run without first reading existing env vars, all of them might be gone. Restore from a known-good config.
- **AWS account-level issue.** Lambda concurrency limit hit, IAM role broken, etc.

### What to do
- Re-enable scheduled rules.
- If env vars are missing, restore from a previous deploy's config (the deploy script doesn't manage env vars, so you'll need a manual backup or to recreate them from `.env.example` + the actual secrets).
- Manually invoke each worker once to confirm it works:
  ```bash
  for task in harvester observer factory; do
    aws lambda invoke --function-name nzyme-talent-management --region eu-west-1 \
      --payload "{\"task\":\"$task\"}" --cli-binary-format raw-in-base64-out /tmp/out.json
    cat /tmp/out.json
  done
  ```

---

## "A specific candidate is duplicated in the system"

### What you see
- Two Main DB pages for the same person — same name, sometimes same email.

### Where to look first
1. **Compare the Notion pages.** Different emails? Different stages of completion? Different Source tags?
2. **Query Supabase:**
   ```sql
   SELECT id, name, email, notion_page_id, created_at FROM "NzymeTalentNetwork"
   WHERE name ILIKE '%<name>%';
   ```

### Common causes
- **Name-only merge collision.** Two different real people share a name; the engine merged them on a name match because one had no email at ingest time. (See [F-9 in the audit](../handover_audit.md#f-9-identity-by-name-merge-can-silently-combine-different-people).)
- **Concurrent ingest race.** Same person was submitted through two processes within seconds; both Harvester invocations created records before either could see the other. (Less common — the UNIQUE constraint on `notion_page_id` usually catches this.)

### What to do
- **If duplicates are the same person:** in Supabase, pick the older record as canonical; merge the experience data manually; delete the newer record (and its applications); update the surviving applications to point to the canonical record. Then in Notion, archive the duplicate Main DB page and reassign workflow Candidate Relations.
- **If duplicates are different people who got wrongly merged:** Worse case. Split them. One needs a brand-new record. Update Supabase first, then create a new Notion page and reassign relations.

This is one of the few operations the system doesn't automate well today. Engineering work needed if it happens often.

---

## Last-resort moves

When nothing else has worked:

1. **Tail every log channel at once for 15 minutes.** See if any line gives a hint.
2. **Disable all webhook feature flags** (`WEBHOOK_*_ENABLED=false`). Fall back to EventBridge-only mode. Slower but simpler.
3. **Pause the EventBridge rules** to stop further state changes while you investigate.
4. **Roll back to the last known-good deploy** (see runbook).
5. **Manually re-do** the work the engine missed — uncheck Processed checkboxes selectively, manually invoke workers, edit pages by hand.

The system is recoverable from any state because Notion holds the truth — even if Supabase gets corrupted, you can always rebuild it by re-running the Harvester. The cost is time, not data.

---

## Who to call

| What broke | Who knows |
|---|---|
| Notion config (automations, permissions, templates, schema) | Whoever has Notion admin access. The CLAUDE.md and `.claude/rules/notion-schema.md` are the technical references. |
| AWS / Lambda / EventBridge / CloudWatch | Engineering. The CLAUDE.md "AWS Operations" section is the reference. |
| Supabase | Engineering. The Supabase project ID is `yphbrpbwpakjduhmoimw`. |
| OpenAI quota | Whoever manages the OpenAI billing account. |
| Notion API key expired | Whoever manages the Notion integration. The key is in the env var `NOTION_KEY` in Lambda configuration. |

For full technical context on any of these, the [technical docs](../technical/) are the next stop — start with the [runbook](../technical/07_runbook.md).
