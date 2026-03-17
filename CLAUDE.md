# CLAUDE.md - Nzyme Talent Engine

## Project Overview

Serverless recruitment automation on AWS Lambda (Python 3.11, Docker). Processes CVs with AI (OpenAI GPT-5-mini + Pydantic structured output), manages candidates across Notion (UI/CRM) and Supabase (PostgreSQL + JSONB + Storage). Uses pdfplumber for PDF extraction, optional Exa.ai for LinkedIn enrichment, and Logfire for observability.

Three workers — **Factory** (process setup), **Harvester** (CV ingestion), **Observer** (change monitoring) — each with dual triggers: scheduled (EventBridge) and event-driven (Notion workspace webhooks).

## Architecture References

Detailed architecture docs are in `.claude/rules/`:
- @.claude/rules/architecture.md — workers, data flow, identity resolution, AI-pending reprocessing, database schemas
- @.claude/rules/webhooks.md — webhook routing, feature flags, adding new handlers
- @.claude/rules/notion-schema.md — path-scoped rules for Notion property changes
- @.claude/rules/testing.md — local testing commands and simulation

---

## Coding Guidelines

### Language Convention
- **Code/variables**: English
- **Logs/comments**: English, keep logs minimal
- **Notion properties**: English (defined in constants.py)

### Architecture Patterns

- **Dependency Injection** — Workers receive initialized clients via constructor
- **Lazy Initialization** — Lambda only instantiates clients needed for the current task
- **run_once() Pattern** — Workers execute a single pass, then exit (EventBridge)
- **Webhook Entry Points** — Workers also have single-page handlers (`run_from_webhook`, `handle_webhook_event`, `process_single_from_webhook`)
- **Feature Flag Pattern** — Webhook handlers gated by `WEBHOOK_*_ENABLED` env vars (default `false`)
- **Static + Dynamic Registry** — `WebhookRouter` resolves DB IDs via env vars first, then Supabase lookup

### Code Style

- **Early returns** on errors — don't nest, exit early
- **Numbered steps** in complex methods (`# 1. Download CV`, `# 2. Parse with AI`, etc.)
- **Constants for property names** — always import from `core/constants.py`, never hardcode strings
- **Handler name constants** — use `HANDLER_*` from `core/constants.py` for webhook routing
- **Logger per module** — `self.logger = get_logger("ModuleName")`

### Error Handling

- Log errors with context but don't crash the batch
- Mark items as processed even on partial failure (prevents infinite loops)
- Use try/except around external API calls (Notion, Supabase, OpenAI, Exa)

### Keeping Docs in Sync

After making code changes, update the relevant `.claude/rules/*.md` file if the change affects documented behavior. Specifically:

- **New/changed worker logic, data flow, DB tables, or identity resolution** → update `architecture.md`
- **New/changed webhook handlers, feature flags, or routing** → update `webhooks.md`
- **New/changed Notion properties** → follow the checklist in `notion-schema.md`
- **New/changed local testing commands or env vars** → update `testing.md`
- **New architectural patterns, code style rules, or conventions** → update this file (`CLAUDE.md`)

If unsure whether a change warrants a docs update, ask: *"Would a future session make a mistake without knowing this?"* If yes, update the relevant file.
