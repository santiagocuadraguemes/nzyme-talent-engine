# 04 — Glossary

One sentence per term, in plain language. Where the term has a deeper technical meaning, a "see also" link points to it.

---

## Things that exist in Notion

**Main DB** (also called "Talent Network") — The master list of every candidate Nzyme has ever processed. One page per real person. Has 30+ properties: name, email, experience tags broken down by sector and role type, education, languages, Source tags, processes they've been involved in. *See [03_supabase_schema.md](../technical/03_supabase_schema.md) for the structured mirror in Supabase.*

**Workflow DB** — A per-process database where individual candidate applications live. One row per (candidate, process) pair. Has a `Stage` column showing where they are in the pipeline.

**Form DB** — A per-process database where CV submissions land. One row per submission. Each Form entry triggers a Notion automation that creates a matching Workflow entry.

**Bulk DB** — A per-process database for uploading multiple CVs at once. Each row holds many files; the engine "splits" the row into N separate Form entries.

**Feedback DB** — A per-process database where interviewers upload their feedback PDFs. The engine parses the PDF and attaches the markdown to the candidate's workflow page.

**Outcome Form** — A child database inside each candidate's workflow page. Has a select with three options: "Discarded completely for Nzyme", "Disqualified only for this role", "Lost for this process". Filling it in moves the candidate to a final stage.

**Process Dashboard** — The control panel for active recruiting processes. One row per process. Used to start, configure, and close processes. Has a `Process Type` field that determines which template gets applied.

**Process Type** — A select option on the Process Dashboard like `"Tech - Lead"`, `"PortCo - CEO"`, `"Internship Programme - Rotational Internship"`. Determines which template the Factory applies. *See also: Template.*

**Template** (Process Dashboard template) — A Notion template named `"PROCESS TEMPLATE - {suffix}"` (e.g., `"PROCESS TEMPLATE - Tech - Lead"`). When the Factory creates a process, it clones the matching template. The template defines what child databases and pages the process will have.

**Guidelines DB** — A central Notion database holding documentation per Process Type. Each Process Type has two documents: an "Interview Stages" doc (defines the stages and the assessment matrix) and a "Role & Candidate Description" doc (the JD). The Factory reads both at process setup.

**Confidential Assessments DB** — A restricted-access Notion database where the engine creates entries when a candidate is Discarded / Disqualified / Lost. Each entry links back to the candidate's Main DB page.

**References DB** (central) — A Notion database where referrers submit reference information. The engine distributes each reference to the relevant active workflow pages.

**Past Experience [AI-generated]** — A child database that appears on each candidate's workflow page after ingestion. The AI fills in scores for each characteristic defined in that process's matrix.

**Assessment Characteristics** — A child database inside each process's Interview Stages guideline page. Defines the criteria the AI uses for the more in-depth feedback assessment (separate from Past Experience).

**Process Visibility** — A select on the Process Dashboard with two options: "Standard" or "Confidential". Confidential processes hide their candidates from anyone not in the process's `Governance` list.

**Governance: Edit & View Access** — A people property on both the Process Dashboard (defines who can see this confidential process's candidates) and the Main DB (the per-candidate version, controlling visibility).

---

## Things that exist in Supabase

**`NzymeTalentNetwork`** — The Supabase table mirroring the Main DB. Hybrid schema: identity fields (name, email, etc.) as SQL columns, everything else (experience by sector, education, history) in a JSONB blob called `candidate_data`.

**`NzymeRecruitingProcesses`** — The Supabase table holding active recruiting processes. One row per process. Created by the Factory.

**`NzymeRecruitingApplications`** — The Supabase table linking candidates to processes. One row per (candidate, process) pair. Each row corresponds to one Workflow DB page in Notion.

**`NzymeRecruitingProcessHistory`** — The Supabase audit log of stage transitions. One row per change. Created by the Observer when it detects a stage change.

**`resumes` bucket** — Supabase Storage bucket where CV PDFs are stored. Files are publicly accessible via predictable URLs.

**`current_stage`** — The candidate's current stage on their Application row. Should always match the `Stage` field on the corresponding Notion Workflow page; the Observer reconciles drift.

**`process_id`** / **`candidate_id`** — UUID primary keys linking the three main tables together.

**`notion_page_id`** — Stored on most Supabase tables; the Notion page UUID for the corresponding row. Used to round-trip between Notion and Supabase.

**`matrix_characteristics`** — A JSONB array on a process row. Defines the AI's scoring criteria for Past Experience. Extracted from the process's template at setup time.

**`assessment_characteristics`** — A different JSONB array on a process row. Defines the criteria for the Feedback Assessment (the on-demand AI scoring that combines CV + interview feedback).

**`governance_people`** — A JSONB array of Notion user UUIDs. Populated for confidential processes. Defines who can see candidates in this process.

**`headhunter_name`** — A text field on a process row. The name of the headhunter firm associated with this process (e.g., "BAON"). Used to tag candidates ingested via this process as `"Headhunter - BAON"`.

---

## Concepts about the system

**The Factory** — One of three workers (also called "robots"). Sets up new processes when a new Process Dashboard page is created. *See [02_the_three_robots.md](02_the_three_robots.md).*

**The Harvester** — The worker that ingests new candidates. Reads CVs, runs AI, creates Main DB pages and application rows.

**The Observer** — The worker that watches for changes after ingestion. Detects stage transitions, processes feedback uploads, handles outcomes, runs AI-pending reprocessing.

**Webhook** — A "something happened" notification Notion sends to the engine when a database changes. Lets the engine react in seconds rather than waiting for the next scheduled run.

**EventBridge** — AWS's scheduler. Wakes up the engine every 10 minutes (or hourly, for the Factory) to do safety-net sweeps.

**Feature flag** — A configuration switch (env var) that turns a webhook handler on or off without redeploying. Default is off. Used to roll out webhook handlers incrementally.

**`Processed` checkbox** — A bookkeeping field on most Notion databases. The engine sets it to `true` when it's finished with a row, to avoid re-processing on the next sweep. Manually unchecking it triggers re-processing.

**`AI Pending` checkbox** — A flag on Main DB pages indicating that the engine ingested the candidate but couldn't run AI parsing (usually because OpenAI was down). The engine retries periodically until it succeeds.

**Identity resolution** — The 4-rule decision tree the engine uses to decide whether an incoming CV belongs to an existing candidate (merge) or a new one (create). Rules check email first, then name. *See [03_when_things_break.md](03_when_things_break.md) for related risks.*

**Direct entry** — A candidate added straight to a Workflow DB without going through a Form. The Harvester handles these separately in "Step 2.5". Higher duplicate-risk because there's often no email.

**Bulk import** — A multi-file CV upload, submitted to a process's Bulk DB. The Harvester splits the batch into individual Form entries.

**Strategic assessment** — The per-process AI scoring written into a candidate's "Past Experience [AI-generated]" child DB at ingest time. Uses the process's `matrix_characteristics`.

**Feedback assessment** — A separate, on-demand AI scoring that combines the candidate's CV with all collected interviewer feedback. Triggered by checking "Assessment Requested" on a workflow page.

**Source attribution** — Tags written to the Main DB candidate's `Source` field when they enter the system: `"Applied via LinkedIn"`, `"Headhunter - {firm}"`, or `"Headhunter"` (when no firm is set). Tags accumulate over time across multiple process ingestions.

**ZWSP** — Zero-Width Space (Unicode U+200B). Invisible character the Factory uses to prefix stage names for ordering ("0.1 Identified" gets 0 ZWSPs, "0.2 Engagement" gets 1, etc.). Looks the same to humans but breaks string-equality comparisons. *Source of confusion — see [F-11 in the audit](../handover_audit.md#f-11-zwsp-prefixes-on-stage-names-are-a-hidden-invariant).*

---

## Confusable pairs

These pairs of terms sound similar but mean different things. People mix them up.

**Process vs. Application** — A **process** is a recruiting effort (one row in `NzymeRecruitingProcesses`, one Process Dashboard page). An **application** is one specific candidate's participation in one process (one row in `NzymeRecruitingApplications`, one Workflow DB page). One process has many applications; one candidate can have many applications across different processes.

**Workflow page vs. Main DB page** — A candidate has **one Main DB page** (their canonical profile in the Talent Network) and **one Workflow page per process they're in** (their per-process participation tracker, with the stage). The Workflow page links to the Main DB page via a `Candidate Relation` field.

**Stage vs. Outcome** — A **stage** is any of the predefined steps in the recruiting pipeline ("0.1 Identified", "1.1 Fit Interview", "2.3 Final Interview", etc.). An **outcome** is the *end result* of an application — Discarded, Disqualified, or Lost — which gets translated into a final stage via the Outcome Form.

**Headhunter (checkbox) vs. Headhunter (relation)** — The Form DB has a `Headhunter` *checkbox* (was this candidate forwarded by a headhunter?). The Process Dashboard has a `Headhunter` *relation* (which headhunter firm is this process associated with?). Same name, different field types, different databases.

**Creator vs. Source** — `Creator` is a multi-select on Main DB pages that humans manage (who originally added/tagged this candidate). `Source` is a multi-select on Main DB pages that *the engine* manages (where did the latest application come from — LinkedIn, a specific headhunter, etc.).

**Open vs. Active** — A *process* is either Open or Closed (visible in the Process Dashboard's `Open/Closed` select). An *application* is either Active or some other status (`status` field on `NzymeRecruitingApplications`). Closing a process doesn't automatically mark its applications inactive.

**Talent Network vs. Main DB** — Same thing. "Talent Network" is the human-facing name; "Main DB" is the engineering name and how it's referred to in code. *Use Talent Network in conversations with recruiters; Main DB in code reviews.*

**Form DB vs. Bulk DB** — Both are submission entry points. Form DB takes one CV per submission. Bulk DB takes many CVs per submission and the engine splits them into individual Form entries. Both end up in the same Workflow DB.

**Webhook vs. EventBridge** — Two ways the engine gets woken up. Webhooks are *event-driven* (Notion tells the engine when something changes). EventBridge is *time-driven* (the engine wakes up on a schedule, every 10 minutes). They overlap in coverage — webhooks are faster, EventBridge is the safety net.

**Data source ID vs. Database ID** — Two different IDs for what looks like the same thing in Notion. As of the 2025-09-03 API version, every Notion database has a separate "data source" ID for querying its rows. Mostly an implementation detail; the engine resolves between them automatically.

---

## What to do if you encounter a term that isn't here

The most likely places to find it:
- **In code:** [core/constants.py](../../core/constants.py) holds every Notion property name and handler constant.
- **Schema reference:** [.claude/rules/notion-schema.md](../../.claude/rules/notion-schema.md) catalogues every Notion property used by the engine.
- **Architecture reference:** [.claude/rules/architecture.md](../../.claude/rules/architecture.md) covers the data flow, identity engine, and concurrency guards.
- **Tech docs:** [../technical/](../technical/) has the systematic technical reference.

If after all that it's still unclear, the candid answer is: the original author was the only person who knew, and now you're working it out. The audit at [../handover_audit.md](../handover_audit.md) flags the worst landmines so you can be careful around them while you learn.
