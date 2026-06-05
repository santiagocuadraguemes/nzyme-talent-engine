# 01 — Who Does What: Notion vs the Engine

This is the single most important document for anyone joining the project. The Nzyme Talent Engine isn't just code — it's a partnership between code and Notion, and most of the time when something looks broken, the cause is on Notion's side, not the code's.

This doc maps every action in the system: what the code is responsible for, what Notion does on its own, and what fails when one of them changes behavior.

---

## The mental model

Imagine the system as a building with two floors:

- **Top floor: Notion.** Forms, dashboards, candidate pages, tables. This is where humans work. It has its own behavior — automations that fire when a form is submitted, permission rules that decide who sees what, templates that get cloned into new pages.

- **Bottom floor: the engine.** Python code running on AWS. It listens for things happening on the top floor (Notion sends a webhook) and it occasionally walks upstairs to read or write things (calling the Notion API).

The engine *doesn't own* the top floor. It doesn't create the automations. It doesn't define the permission rules. It doesn't write the templates. Those live in Notion's settings — configured by humans using Notion's UI, not by code.

When the engine breaks, you check the bottom floor (CloudWatch logs).
When the engine works but the system feels broken, you check the top floor (Notion settings).

---

## The big table: every action, who does it

Read this slowly. It's the cheat sheet for everything.

| When this happens | The code does | Notion does (on its own) | What breaks if Notion misbehaves |
|---|---|---|---|
| A candidate submits a CV through a Form | Receives a webhook, downloads the CV, runs AI, creates the Main DB candidate record + Workflow application row | Fires its own automation: creates a Workflow DB entry, links it to the Form entry by an `ID` field, fires the webhook | If Notion's "Form → Workflow" automation is paused or deleted, the engine never finds the new candidate. Symptom: candidate submitted, nothing appears in the Workflow DB. |
| A new process is set up | Receives webhook, fuzzy-matches the `Process Type` to a Notion template, applies the template to the page, renames the child databases, fills the JD and Interview Stages pages, registers the process in Supabase | Creates the four child databases (Workflow / Form / Bulk / Feedback) and two child pages (JD, Interview Stages) when the template is applied — and does this *asynchronously*, so it might take 8-20 seconds | If Notion's template is missing or named wrong (must start with `"PROCESS TEMPLATE - "`), the engine has nothing to apply. If the async lag is too long, the engine gives up after 4 retries (~40 seconds). |
| Someone uploads multiple CVs at once via the Bulk DB | The engine splits the upload into individual Form entries, one per CV, with 10 seconds between each | Each new Form entry triggers the same Form-submission automation that fires for single CVs | The engine forces "Headhunter" = true on every split file. If the bulk upload was *not* from a headhunter, all candidates get mis-tagged. There's no way to override this today. |
| A recruiter changes a candidate's Stage in the Workflow DB | The engine compares the new stage to what's stored in Supabase. If different, writes an audit log entry and updates the stored stage | The Workflow DB's `Stage` is a select column with predefined options (set by the Factory when the process was created). The options have invisible "zero-width space" characters prepended for ordering — humans don't see them but the strings include them | If a recruiter manually changes the Stage options in Notion's UI (e.g., adding "On Hold"), they should NOT delete options. Deleting an option causes Notion to remap candidates to the closest remaining option, silently moving real people to wrong stages. |
| A recruiter clicks "Assessment Requested" on a workflow page | The engine reads the candidate's CV + all gathered interview feedback, asks AI to produce a scored assessment matrix, creates a new "Feedback Assessment [AI-generated]" page in the workflow's Gathered Feedback DB, unchecks the box | The "Assessment Requested" checkbox is just a Notion property — toggled by a button or by automation. Notion has no concept of "kick off this expensive operation" — that's purely the engine's interpretation. | If the engine fails (OpenAI down, no characteristics defined), the engine logs an error and unchecks the box anyway. The recruiter sees the box unchecked but no assessment appeared. |
| An interviewer uploads a feedback PDF | The engine downloads the PDF, asks AI to convert it to markdown, finds the candidate by name (no email on feedback forms), creates a new page in the candidate's "Gathered Feedback" child DB with the markdown content | Notion accepts the file upload, creates a Feedback DB entry. No automations beyond the standard webhook | If the candidate's name on the feedback form is misspelled, the engine can't find them. Feedback gets dropped, logged as warning. Recovery: fix the name on the feedback form, uncheck Processed, the engine will try again. |
| Someone fills in an Outcome form (Discarded / Disqualified / Lost) | The engine fuzzy-matches the outcome label to a Stage option, moves the candidate to that stage, saves the explanation as the rejection reason in Supabase, creates a Confidential Assessment page that links back to the candidate's Main DB record | The Outcome form is a child DB inside each workflow page (templated by the Factory). Notion auto-populates a back-relation when the engine creates the Confidential Assessment with a relation to the candidate | If Notion changes how back-relations work (e.g., makes them async), the candidate's Main DB page would lose the link to their Confidential Assessment. UI breaks; the data is technically still correct. |
| A submitter adds a reference for a candidate (central References DB) | The engine resolves the candidate by name+email, finds all their active workflow pages, and creates a new reference entry inside each one's "Candidate References" child DB | Notion accepts the new reference page | If one of the candidate's active workflow pages has the child DB at deep nesting (more than 4 levels), the engine can't find it and the whole reference distribution fails. |
| A confidential process is closed | The engine restores the candidates' visibility (people who were excluded can now see them again), adds the process name to the candidate's "Recruiting Processes History" (it was hidden until closure to maintain confidentiality) | The candidate's "Governance: Edit & View Access" people field is a Notion permission gate — Notion's own permission system uses it to enforce who sees what | If the env var `NOTION_ALL_TEAM_GROUP_IDS` is wrong or stale, the engine writes invalid group IDs and candidates effectively become invisible to everyone. |
| Anyone edits anything on the Main DB | The engine receives a webhook (if `WEBHOOK_MAIN_CANDIDATE_ENABLED=true`) or notices on the next scheduled sweep. It either dispatches the candidate to another process, re-runs AI parsing if "AI Pending" is checked and a CV is now available, or just syncs the latest data to Supabase. | Notion's `last_edited_time` updates on every edit, including edits to *child* content (which is how the engine can detect changes via filtering) | Edits to a workflow page's child DB ("Past Experience [AI-generated]") cascade up — they update the parent page's last_edited_time too. This contributed to a March 2026 incident where the engine misinterpreted a benign edit as a stage change. |
| A bulk upload exceeds ~27 files | The engine sleeps 10 seconds between each file (to avoid stampeding Notion automations); but Lambda has a 300-second hard timeout | Each file becomes a Form entry, then a Workflow entry via the standard Form-submission automation chain | If the Lambda times out mid-batch, the remaining files are never split. The bulk DB row gets marked Processed (only because the marker is at the *end* of the loop, but the loop never gets there in the timeout case). |
| A Process Type select option is added to the Process Dashboard | Nothing — the engine doesn't know about the new option | The new option appears in the Dashboard's Process Type select | If no corresponding template is created (named `"PROCESS TEMPLATE - {new option}"`), the engine can't process pages with this new type. New processes silently stall. |
| An automation in Notion is paused (anywhere) | Nothing — the engine only knows about the webhooks it receives | Webhooks for that automation stop being sent | The EventBridge scheduled sweep (every 10 minutes) is the safety net. Things still get processed, just with up to 10-minute lag. |

---

## What lives in Notion (not in code)

These are the things you can't fix by editing the engine — they're in Notion's settings:

- **The automations that fire webhooks** (one per database where the engine listens — Process Dashboard, Form, Workflow, Bulk, Feedback, Main DB, References, and the per-application Outcome Forms). Configured in Notion's automation panel. Each one targets the engine's URL.
- **The Form-submission → Workflow-entry automation** on every Form DB. Without this, candidates submitted to a form never reach the engine.
- **The permission rule on the Main DB** that enforces who can see a candidate based on their `Governance: Edit & View Access` field. Notion enforces this; the engine just sets the field value.
- **The `PROCESS TEMPLATE - {suffix}` templates** in the Process Dashboard. Each Process Type needs a matching template.
- **The guidelines documents** in the Guidelines DB — one Interview Stages document and one Role & Candidate Description document per process type. The Factory reads these when setting up a new process.
- **The Confidential Assessments DB schema** including the `Assessment` select options ("4. Discarded", etc.).
- **The Process Type select options** — adding a new option requires also adding a matching template.
- **The Stage options** on each process's Workflow DB. The Factory creates them, but they can be edited manually. If you edit, *add* options — don't delete.

If any of these is missing, mis-named, or paused, the engine fails in ways that don't show up as code errors. They show up as "nothing is happening."

---

## What the engine owns (and Notion does not see directly)

- **Supabase** — the structured copy of every candidate, application, stage transition. Used for analytics. Notion doesn't talk to Supabase; the engine does.
- **The CV files** in Supabase Storage (`resumes` bucket). The engine uploads them; Notion stores the resulting public URL as a link.
- **The "Stage history" audit log** in Supabase. This doesn't exist in Notion — it's how analytics can answer "how long did Marta spend in stage 0.2?".
- **The "AI Pending" reprocessing loop.** When OpenAI is down at ingest time, the engine flags candidates and quietly retries them later.
- **The identity merge logic.** When two CVs come in for the same person from different processes, the engine decides whether to merge or create a new record — Notion has no concept of this.

---

## The Source field (Notion-managed and engine-managed)

The Main DB candidate page has two related fields that confuse people:

- **`Creator` (multi-select)** — **fully human-managed.** The engine never writes this. If you tag a candidate with "Marta" in Creator, only Marta touched it. The engine reads the field but never modifies it.
- **`Source` (multi-select)** — **engine-managed**, but additively. The engine adds tags like "Headhunter - BAON" or "Applied via LinkedIn" when a candidate is ingested via a Form. Tags accumulate over time as the candidate appears in more processes. The engine never *removes* tags from Source.

Two confusions to avoid:
- **Don't expect the engine to remove Source tags.** Once added, they're there. (To clean up, edit manually in Notion.)
- **Don't add Source tags yourself for candidates the engine handles.** The engine will *append*, not overwrite, so if you manually add "Referred by Pablo" the engine might later add "Headhunter - BAON" — both will coexist. That may or may not be what you want.

---

## The "Processed" checkboxes — what they mean and what not to touch

Several Notion databases have a `Processed` checkbox (or `Processed [Do not touch]` on the Process Dashboard). These are the engine's bookkeeping flags:

- **Workflow DB `Processed`** — set to true after a candidate's CV has been ingested. The engine uses this to avoid re-ingesting the same candidate. **Don't manually check** unless you're trying to skip a candidate. **Manually unchecking** will cause re-ingestion on the next sweep (sometimes useful — see [03_when_things_break.md](03_when_things_break.md)).
- **Form DB `Processed`** — set to true after the corresponding Workflow entry has been processed. Indirect signal — usually doesn't matter to humans.
- **Feedback DB `Processed`** — true after the engine parsed the PDF and synced the markdown to the candidate. Same caveat: unchecking causes re-parse.
- **Outcome Form `Processed`** — true after the engine moved the candidate to the outcome stage. Don't uncheck (will move the candidate again, possibly to a different stage if Stage options shifted).
- **References DB `Processed`** — true after the reference has been distributed to all the candidate's active workflows.
- **Bulk DB `Processed`** — true after all files in a batch have been split into individual Form entries. **Don't uncheck this** — it would cause re-splitting and duplicate Form entries.
- **Process Dashboard `Processed [Do not touch]`** — true after the process has been set up by the Factory. The name is the warning.

---

## When something looks broken, ask: which floor?

| Symptom | Probably on the Notion floor | Probably on the code floor |
|---|---|---|
| Candidate submitted, never appeared anywhere | Form-submission automation paused | (Unlikely — usually Notion) |
| Stage change isn't in analytics | (Possible — wait 10-15 min first) | Observer is failing — check CloudWatch |
| New process won't initialize | Template missing or guidelines doc missing | (Possible — fuzzy match picked wrong template) |
| Wrong Source tag on a candidate | (No — engine added it, but per its rules — see bulk import note) | (Possible — but more likely a configuration question) |
| Confidential candidate visible to too many people | Permission rule on Main DB | env var `NOTION_ALL_TEAM_GROUP_IDS` is wrong |
| Feedback PDF didn't get parsed | (No) | OpenAI down, or candidate name not found in DB |
| "Past Experience" matrix is empty | Process Type's template doesn't have a Past Experience DB | (Unlikely) |
| Bulk import dropped files | Lambda timed out | (Possible if very large batch) |
| Pages can't be edited | Notion permission rule too strict | (Possible if Governance field has bad IDs) |

When in doubt, ask: *would this still be broken if the engine were turned off?* If yes — it's a Notion problem. If no — it's a code problem.
