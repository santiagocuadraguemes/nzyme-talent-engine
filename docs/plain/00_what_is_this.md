# 00 — What is this?

The **Nzyme Talent Engine** is the software that turns a candidate's CV into a structured record the Nzyme team can search, score, and follow through the hiring process.

Think of it as the invisible assistant sitting between the recruiting team and the rest of the world. When a candidate applies, when an interviewer writes feedback, when a partner forwards a CV from a headhunter — the engine catches it, reads it, files it, and tells the team about it.

You don't see the engine directly. You see Notion — pages, tables, forms, dashboards. The engine works behind Notion, watching it, writing into it, keeping it in sync with a database the team queries for analytics.

---

## What it does, in one paragraph

A candidate applies through a form. Their CV (a PDF) lands on a Notion page. The engine downloads the PDF, asks an AI to read it and extract the candidate's name, education, languages, employment history, and a tag for every sector they've worked in. The engine then checks whether this person already exists in Nzyme's "Talent Network" (the master list of every candidate the team has ever touched). If yes, it merges. If no, it creates a new record. The candidate now lives in two places: the Talent Network in Notion (where recruiters interact with it) and a database called Supabase (where analytics queries run). From this point, every time the candidate's stage changes — Fit Interview, Cultural Interview, Offer, Hired, Disqualified — the engine notices and writes it down. When an interviewer uploads a feedback PDF, the engine reads it, reformats it, and attaches it to the candidate's record. When someone fills in an Outcome form rejecting the candidate, the engine records why, where, and when.

That's it. The engine doesn't make hiring decisions. It just makes sure nothing falls through the cracks and everything is searchable later.

---

## A story: what happens when Marta sends Pablo's CV

It's 3pm on a Tuesday. Marta — a partner at Nzyme — has been emailing with a headhunter who recommends Pablo for the CFO role at one of Nzyme's portfolio companies.

**Marta opens the right Notion page** (the Form for the "PC RB Solutions CFO 2026Q2" process) and uploads Pablo's CV as a PDF. She ticks "Headhunter" because the candidate came through an external firm. She hits Save.

**Within seconds**, Notion fires a webhook — a tiny "something happened" message — to the engine. The engine looks at the message and knows: this is a new Form submission, route it to the Harvester worker.

**The Harvester gets to work.** It downloads Pablo's CV from Notion. It uploads it to a permanent storage location (so the file persists even if the original Notion link expires). It hands the PDF to OpenAI, which reads it and returns a structured summary: Pablo Rodríguez, two universities (one in Spain, one in the UK), 12 years of experience, mostly in private equity portfolio companies, fluent in three languages.

**Identity check.** The Harvester asks Supabase: "Do we already have someone with email `pablo@example.com`? Or, failing that, someone named Pablo Rodríguez?" If yes, this CV gets merged into the existing record (Pablo has now applied to one more process). If no, a new record is created.

**The candidate appears in Notion.** A new page in the Main DB (the Talent Network) now exists for Pablo, with his name, contact info, every sector he's worked in tagged as multi-select chips ("Private Equity", "PortCo", "5-7 Years"), languages, education. The CV is attached as a file. A "Headhunter - BAON" tag is added to his Source field (because Marta ticked Headhunter and the BAON firm was linked to this process when it was set up).

**A workflow application is created.** A row appears in the process's Workflow DB linking Pablo to the CFO role at the portfolio company. He's at stage "0.1 Identified" — the very first stage of the recruiting pipeline.

**A few weeks later**, Pablo gets a Fit Interview. A recruiter changes his stage in the Workflow DB to "1.1 Fit Interview with Strategy & Growth Director". The engine — the Observer worker, this time — notices the change within a minute or two (or right away, if the webhook is enabled) and writes a row in the Process History table: "from `0.1 Identified` to `1.1 Fit Interview...` at <timestamp>." This audit trail is what powers all the analytics about how long candidates spend at each stage.

**An interviewer uploads a feedback PDF.** The engine reads it, asks the AI to convert it to clean markdown, and creates a new page inside Pablo's workflow record titled with the interviewer's name and the interview stage. Anyone looking at Pablo's profile can read the feedback.

**Eventually**, Pablo is not the right fit. Someone fills in the Outcome form — "Discarded completely for Nzyme" — with an explanation. The engine moves Pablo's stage to "Discarded", saves the explanation as the rejection reason in Supabase, and creates a Confidential Assessment page that records the outcome alongside Pablo's name in a restricted-access database that only specific Nzyme team members can see.

None of this involved Marta or the recruiting team doing anything beyond clicking around in Notion. The engine quietly handled the file management, the AI parsing, the database syncing, the audit logging.

---

## What lives where

| Where | What's in it | Who interacts with it |
|---|---|---|
| **Notion** | Forms, dashboards, candidate pages, workflow tables, interview feedback, the dashboards where recruiters do their daily work | The recruiting team, partners, anyone at Nzyme |
| **The engine (AWS Lambda)** | The Python code that does the work. Runs invisibly. | Nobody, normally — only an engineer when something breaks |
| **Supabase** | The structured copy of every candidate, every application, every stage transition. Used for analytics. | Engineers, occasionally; data dashboards that read from it |
| **Supabase Storage** | The actual CV PDF files | The engine writes them; Notion links to them |
| **OpenAI** | The AI that reads CVs and extracts structured data | The engine, only |
| **Exa.ai** | A service that fetches LinkedIn profiles when there's no CV available | The engine, only |

If Notion goes down, the engine can't see anything new — but the data is safe in Supabase. If Supabase goes down, the engine fails to record new things but Notion keeps working as a UI. If the engine goes down, both Notion and Supabase still work — they just don't talk to each other for a while.

---

## What it doesn't do

- **It doesn't decide whether to hire someone.** The AI scoring is decision *support*, not decision-making. The "scored matrix" in each candidate's profile is the AI's read on them — humans interpret it.
- **It doesn't send emails or schedule interviews.** No outbound communication. The engine reads things and writes them into Notion/Supabase.
- **It doesn't reject candidates automatically.** When the engine moves someone to a "Discarded" stage, it's because a human filled in an Outcome form.
- **It doesn't surface candidates to portfolio companies or external clients.** Everything stays within Nzyme's Notion workspace.

---

## What can go wrong, briefly

Three things break most often:

1. **A candidate's CV doesn't get processed.** Usually because OpenAI was momentarily down or rate-limited. The engine flags the candidate as "AI Pending" and tries again next time it runs (every 10 minutes for the scheduled batch; sooner if the candidate's page is edited).
2. **A stage change doesn't appear in the analytics.** Usually a delay — the Observer worker runs every 10 minutes, so it can take up to 10 minutes for a stage change to make it from Notion into Supabase. If it's been longer than that, something's broken.
3. **A process won't start.** Process setup depends on a Notion template being applied. If the template is missing, broken, or named wrong, the new process page stays in a half-configured state. An engineer has to look at logs.

A more complete list, with what to check first, is in [03_when_things_break.md](03_when_things_break.md).

---

## Who built this

Santiago Cuadra built it from scratch starting in January 2026. The system has been live since early February 2026 and has processed thousands of candidates across dozens of active recruiting processes. It is maintained by whoever inherits this repo — which, right now, is you.

The codebase is small (under 5000 lines of Python). Most of the complexity is in *what it knows about Notion* — which fields exist, when Notion fires webhooks, what happens when you click certain buttons. That knowledge is captured in detail in [01_who_does_what.md](01_who_does_what.md). If you read nothing else, read that.
