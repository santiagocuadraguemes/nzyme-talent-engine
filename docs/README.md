# Nzyme Talent Engine — Documentation

You're inheriting a serverless recruitment automation system that ingests CVs into Notion + Supabase via AI parsing. The original author has handed off; this documentation is the handover package.

This index points to three layers of documentation, ordered by audience.

---

## If you're a non-engineer (operator, future hire, day-one reader)

Read these in order:

1. **[plain/00_what_is_this.md](plain/00_what_is_this.md)** — Two pages. What the system does, in a story.
2. **[plain/01_who_does_what.md](plain/01_who_does_what.md)** — The Notion-vs-engine cheat sheet. Read this twice.
3. **[plain/02_the_three_robots.md](plain/02_the_three_robots.md)** — Factory, Harvester, Observer explained by analogy.
4. **[plain/03_when_things_break.md](plain/03_when_things_break.md)** — Symptom-first troubleshooting. Bookmark this.
5. **[plain/04_glossary.md](plain/04_glossary.md)** — Every term, one sentence.

---

## If you're an engineer inheriting the codebase

Read these in order:

1. **[technical/00_overview.md](technical/00_overview.md)** — Architecture diagram, three workers, two triggers, data flow.
2. **[technical/01_workers.md](technical/01_workers.md)** — One section per worker; entry points, reads, writes, idempotency, failure modes.
3. **[technical/02_notion_integration.md](technical/02_notion_integration.md)** — **The most important file in the set.** A 28-row table of every Notion interaction and its failure mode.
4. **[technical/03_supabase_schema.md](technical/03_supabase_schema.md)** — Every Supabase table, column, JSONB shape, unique constraint.
5. **[technical/04_ai_pipeline.md](technical/04_ai_pipeline.md)** — OpenAI usage, Pydantic models, AI-pending reprocessing, cost shape.
6. **[technical/05_webhook_router.md](technical/05_webhook_router.md)** — Three payload shapes, registry tiers, feature flags, how to add a handler safely.
7. **[technical/06_decision_log.md](technical/06_decision_log.md)** — 12 non-obvious architectural choices with rationale.
8. **[technical/07_runbook.md](technical/07_runbook.md)** — Deploy, rollback, log tailing, feature flag rollout, recovery playbooks.

---

## If you're triaging a production issue

Start at **[handover_audit.md](handover_audit.md)** — the known architectural defects (F-1–F-14, with F-7 unassigned) ranked by severity, each with a file:line citation and a current status. Check whether your symptom matches a known finding; read the "Status" line first, since several are already resolved.

For symptom-first troubleshooting in plain language, see **[plain/03_when_things_break.md](plain/03_when_things_break.md)**.

For AWS/CloudWatch commands, see the runbook playbooks in **[technical/07_runbook.md](technical/07_runbook.md#common-incident-playbooks)**.

---

## What was removed from `docs/` (recoverable from git history)

Two historical files were removed from the working tree during the handover cleanup. Both are still
recoverable with `git show HEAD~1:docs/<file>` if you want the detail, but note they use the
**pre-refactor Spanish method names** (`crear_aplicacion`, `procesar_candidato`, …) that no longer
exist in the code:

- **`audit_march_2026.md`** — the original author's forensic incident report on two March 2026
  production bugs. Its findings live on as [F-3](handover_audit.md#f-3-factorys-stage-options-update-is-still-destructive-march-audit-bug-3-not-fixed)
  (resolved via the Factory's Supabase guard) and [F-4](handover_audit.md#f-4-no-initial-history-entry-when-an-application-is-created-march-audit-bug-2-not-fixed)
  (still open, low priority) in the handover audit. Useful as a reference for what good incident
  analysis looks like.
- **`continuation_prompt.md`** — a debugging-session handoff from March 2026 referencing the same
  incidents. Historical interest only.

The empty `data_models/` and `workers/` directories were placeholders the original author created
but never filled.

---

## What's NOT in `docs/`

- **`CLAUDE.md`** (project root) — Day-to-day operational reference: AWS CLI cheat sheet, deploy command, EventBridge schedules. Read this for hands-on commands.
- **`.claude/rules/*.md`** — The original author's working notes (architecture, webhooks, notion-schema, testing). Treated as authoritative reference by Claude Code; verified against current code in our technical docs above. Read these for the source-of-truth view; read our technical docs for the engineering-context view.
- **`.env.example`** — Required and optional environment variables.
