# 02 — The Three Robots

The engine isn't one program — it's three programs that share a codebase, take turns running, and have different jobs. Each one is a "robot" in the warehouse: they don't talk to each other, but they all know about the same shelves (Notion + Supabase) and they each have a strict set of things they're responsible for.

This doc introduces all three by analogy. The technical names are "workers," but "robots" is closer to how they actually behave.

---

## The warehouse setup

Imagine a warehouse full of folders (Notion databases). Three robots work in the warehouse:

- **The Factory robot** sets up new shelves when a new process starts.
- **The Harvester robot** takes incoming CVs, reads them, and files them on the right shelves.
- **The Observer robot** walks the floor noticing what's changed and updating records.

Each robot wakes up in two ways:

1. **The clock wakes them up.** Every 10 minutes, the Harvester and Observer wake up and do a full sweep of the warehouse. The Factory wakes up every hour for a slower safety check.
2. **A bell rings them.** When something specific happens in the warehouse (a new folder is created, a sticker is moved), Notion rings a bell that wakes up the right robot immediately.

The bells (webhooks) give fast service. The clock guarantees nothing is forgotten if a bell didn't ring.

---

## The Factory robot — sets up new shelves

### What it does
When a new recruiting process is needed, someone creates a page in the **Process Dashboard** in Notion: they give it a name like "PC RB Solutions CFO 2026Q2", pick the Process Type from a dropdown ("PortCo - CFO"), and save.

The Factory wakes up (because Notion rings the bell for new pages on the Dashboard). It:

1. Reads the page's `Process Type`.
2. Looks at the list of templates available on the Process Dashboard. Each template is named like `"PROCESS TEMPLATE - PortCo - CFO"`. The Factory fuzzy-matches the process type against template names and picks the best fit.
3. Tells Notion: "apply this template to this page." Notion clones the template's contents — creating four child databases (Workflow / Form / Bulk / Feedback) and two child pages (Job Description, Interview Stages) inside the new page.
4. Waits a few seconds (Notion does this asynchronously).
5. Renames the four child databases to include the process name (so they look like "Feedback Tool & Workflow - PC RB Solutions CFO 2026Q2").
6. Opens the Interview Stages guidelines document, reads the stages from a table inside it, and writes those stages as options on the Workflow DB's "Stage" column.
7. Opens the Role & Candidate Description guidelines and copies its content into the new process's JD page.
8. Records the process in Supabase so other systems can query it.
9. Ticks `Processed [Do not touch]` on the Dashboard page. Done.

### What can go wrong
- **No matching template.** If the Process Type is "PortCo - Chief Data Officer" but the only template is "PortCo - CDO", the fuzzy match might still pick it correctly — or might pick the wrong template. The Factory logs the match score and the runner-up score; an engineer reviews after each new process type is added.
- **Notion didn't finish creating the child DBs in time.** The Factory waits up to 40 seconds; if Notion's still working, the page gets abandoned. Manually unchecking `Processed [Do not touch]` lets the Factory try again later.
- **Guidelines documents are missing.** No stages get written. The process is half-set-up — the Workflow DB exists but its Stage column has no options.

### When it wakes up
- **Bell rings:** within seconds of someone saving a new Process Dashboard page.
- **Clock alarm:** every hour, as a safety net to catch missed bells.

The Factory is mostly idle. It might run a few times a month — once per new process the team starts. The hourly check is paranoia.

---

## The Harvester robot — ingests new candidates

### What it does
The Harvester does the heavy lifting. When a CV lands in the system, the Harvester picks it up.

A typical run goes like this:

1. Looks at every active process. For each one, queries the Workflow DB for pages that have `Processed=false` and an `ID` linking to a Form entry.
2. For each such candidate:
   - Finds the matching Form entry (which holds the uploaded CV file).
   - Downloads the CV from Notion.
   - Uploads it to permanent storage (so the file stays available even if Notion's link expires).
   - Asks OpenAI to read the CV and extract structured data — name, contact info, education, languages, employment broken down into 17 sector/functional categories, plus a scored assessment against the process's specific criteria.
   - Checks if this person already exists in the Talent Network (Supabase) — by email first, then by name as fallback. If yes, merge. If no, create new.
   - Writes the candidate's data to Notion's Main DB (Talent Network), creating or updating the candidate's page with 30+ properties.
   - Creates an application row in Supabase linking the candidate to the process.
   - Writes the AI's scored assessment to the "Past Experience [AI-generated]" table on the candidate's workflow page.
   - Ticks `Processed=true` on the workflow page.
3. After all standard candidates: handles "direct entry" candidates (people added straight to the Workflow DB without a Form), processes pending bulk imports (CSV-style multi-file uploads), and retries any candidates that failed AI parsing previously ("AI Pending").

### What can go wrong
- **OpenAI is down or rate-limited.** The Harvester marks the candidate "AI Pending" and creates a skeleton record with basic info (name from the form, email if available) but no experience data. The candidate is visible in Notion but flagged for retry.
- **No CV attached to the form.** The Harvester tries to fetch the candidate's LinkedIn profile via Exa.ai instead. If that fails too, creates a skeleton record marked AI Pending.
- **Identity merge picks the wrong person.** Two people with the same name (and neither has an email on file) get merged into one record. The system logs a WARNING but proceeds. Recovery: manually split the records in Supabase + Notion.
- **Lambda timeout on big bulk batches.** Bulk imports sleep 10 seconds between each file split (to give Notion time to fire automations sequentially). With 30+ files in one batch, the run can hit the 5-minute Lambda hard timeout. Remaining files don't get split — the batch is marked Processed but only partially.

### When it wakes up
- **Bell rings:** within seconds of a Form submission or a Bulk DB upload — if the corresponding webhook feature flag is enabled.
- **Clock alarm:** every 10 minutes, for a full sweep across all active processes.

The Harvester is the busiest robot. In a busy week it might process dozens of CVs.

---

## The Observer robot — watches for changes

### What it does
The Observer's job is reactive. Things change in Notion all the time — a recruiter moves a candidate to a new stage, an interviewer uploads feedback, a referrer fills in a reference form. The Observer makes sure those changes are reflected in the right places.

It has six distinct responsibilities:

1. **Stage change detection.** When a candidate's Stage changes in a Workflow DB, the Observer compares it to the stored stage in Supabase. If different, writes an audit log entry and updates Supabase.
2. **Outcome processing.** When someone fills in the Outcome form on a workflow page (Discarded / Disqualified / Lost), the Observer:
   - Moves the candidate to the matching stage.
   - Saves the explanation as the rejection reason in Supabase.
   - Creates a Confidential Assessment page (in a restricted-access database) linked to the candidate.
3. **Feedback PDF ingestion.** When an interviewer uploads a feedback PDF to the Feedback DB, the Observer downloads it, asks AI to convert it to clean markdown, and attaches it to the right candidate's workflow page.
4. **CV enrichment + AI-pending retry.** When a Main DB candidate page is edited — perhaps someone uploaded a CV to an existing candidate who had no CV — the Observer parses it and fills in the experience data. Same for candidates flagged "AI Pending" who now have a CV available.
5. **Reference distribution.** When a reference is submitted to the central References DB, the Observer finds all of the candidate's active workflow applications and creates a reference entry inside each one.
6. **Process status sync.** When someone changes a process from Open to Closed on the Process Dashboard, the Observer updates Supabase. If the process was confidential, it also restores visibility on all its candidates (they were hidden during the process for confidentiality; now that it's closed, everyone can see them).

### How it sees changes
The Observer uses two strategies:

- **Direct query (the "sniper").** For databases whose IDs the engine knows (Main DB, Process Dashboard, References DB, every active process's Workflow + Feedback DBs), the Observer queries them directly with a filter for things edited in the last 11 minutes.
- **Search query (the "radar").** For Outcome Forms (which are per-candidate child databases — there's no master list of their IDs), the Observer asks Notion's search API for all data sources named "Process Outcome Form", then walks each one's parent chain to find the right candidate.

### What can go wrong
- **Feedback PDF for a candidate whose name doesn't exactly match.** Feedback forms ask for the candidate name as free text. If a typo, the Observer can't resolve the identity and the feedback is dropped. The Feedback form is marked Processed anyway. Recovery: edit the name on the form, uncheck Processed, the Observer tries again.
- **Outcome stage value doesn't match any existing Stage option.** The fuzzy match falls through to the raw outcome text, which isn't a valid Stage option. The stage write fails (logged), Outcome is marked Processed anyway, candidate is stuck in their previous stage. Manual fix needed.
- **Confidential process visibility doesn't restore on close.** Usually a wrong/stale `NOTION_ALL_TEAM_GROUP_IDS` env var — the Observer writes invalid permission group IDs and candidates remain invisible.

### When it wakes up
- **Bell rings:** for six different webhook handlers — Main DB edits, Process Dashboard edits, Workflow stage changes, Outcome forms, Feedback uploads, Reference submissions.
- **Clock alarm:** every 10 minutes (offset by 3 minutes from the Harvester so they don't trip over each other), doing a full sweep of all watched databases with an 11-minute lookback window.

The Observer is constantly busy in a way the Harvester isn't — every edit anywhere in the system is a potential thing to look at.

---

## How they share the workspace

The three robots don't talk to each other. They share state through Notion and Supabase. There are a few places where their jobs touch:

- The **Factory** creates a process, then the **Harvester** ingests candidates into it.
- The **Harvester** sometimes can't fully parse a CV (OpenAI was down); the **Observer** retries it later when the candidate's page is edited.
- The **Observer** writes a stage change to Supabase; future Observer runs use that record to detect later changes.
- When a process is closed, the **Observer** restores visibility on all its candidates — re-reading governance data the Harvester wrote at ingest time.

They use *guard checks* to avoid stepping on each other. Before doing expensive work, each robot asks "did someone already do this?" — usually by querying Supabase. If the answer is yes, they back off.

---

## The bell vs the clock — when does which fire?

| Action | Bell (webhook) fires | Clock (EventBridge) fires |
|---|---|---|
| New Form submission | Form webhook → Harvester immediately | If bell missed: next 10-minute Harvester sweep |
| New Process Dashboard page | Process Dashboard webhook → Factory immediately | If bell missed: next hourly Factory sweep |
| Stage change on Workflow | Workflow webhook → Observer immediately | If bell missed: next 10-minute Observer sweep |
| Feedback PDF upload | Feedback webhook → Observer immediately | If bell missed: next 10-minute Observer sweep |
| Outcome form filled | Outcome webhook → Observer immediately | If bell missed: next 10-minute Observer sweep (via radar) |
| Reference added | Reference webhook → Observer immediately | If bell missed: next 10-minute Observer sweep |
| Bulk upload | Bulk webhook → Harvester immediately | If bell missed: next 10-minute Harvester sweep |
| Main DB edit | Main DB webhook → Observer immediately | If bell missed: next 10-minute Observer sweep |

The bells are configurable on a per-handler basis (feature flags). If a bell is disabled, the corresponding clock still fires. In an emergency you can turn off all bells and rely on the clock — there will just be a 10-minute lag on everything.

---

## Watching them work

Every robot's actions are logged in CloudWatch (AWS's log viewer). Each log line is tagged with which robot was working (`[FactoryWorker]`, `[Harvester]`, `[Observer]`) and a unique invocation ID so you can follow one run from start to end.

When something feels broken, the typical first move is:
1. Pick the robot whose job it would be (a CV stuck unprocessed → Harvester; stage change not in analytics → Observer; new process not configured → Factory).
2. Tail the logs filtered to that robot.
3. Look for the candidate or process name in the recent logs.
4. Read the lines around it for hints — "AI failed", "No template resolved", "Application already exists", etc.

The technical runbook in [../technical/07_runbook.md](../technical/07_runbook.md) has the exact commands. For symptom-first troubleshooting see [03_when_things_break.md](03_when_things_break.md).
