# 04 — AI Pipeline

All AI calls are concentrated in [core/ai_parser.py](../../core/ai_parser.py). The module is small (~485 lines) but holds four distinct AI operations, each backed by a Pydantic response model and the OpenAI structured-output endpoint.

Model: `gpt-5-mini` (hardcoded at [ai_parser.py:132](../../core/ai_parser.py)).

Observability: optional `LOGFIRE_TOKEN` env var enables `logfire.instrument_openai(self.client)` for full prompt/response tracing.

> ⚠️ Line numbers for `harvester.py`/`observer.py` citations below are approximate (those files
> churn) — grep the named method if a citation has drifted. `ai_parser.py` citations are current.

---

## Why structured output

Every AI call uses `client.beta.chat.completions.parse(response_format=<PydanticModel>)`. This is OpenAI's structured-output mode — the model is constrained at decoding time to produce JSON matching the Pydantic schema, and the SDK returns a parsed Python object (`completion.choices[0].message.parsed`).

The alternative — free-form JSON with regex/json.loads parsing — is brittle: any model output that mis-quotes a string or skips a comma breaks the parser. With structured output, the model can fail to follow content instructions but cannot produce malformed schema. Every downstream consumer can treat the returned object as type-checked.

The cost: prompt instructions for *what* fields contain are baked into the Pydantic field descriptions ([ai_parser.py:16-103](../../core/ai_parser.py)). E.g., `SectorExperience.companies` has a 6-rule docstring about company name canonicalization that's part of the schema sent to the model. Changing field semantics requires editing the model class.

---

## The four AI operations

### 1. `process_cv(file_path, matrix_characteristics=None)` — CV parsing

Reads a PDF (via `pdfplumber`) or DOCX (via `python-docx`), truncates to 25 000 chars, calls the model with the `CVData` schema.

`CVData` ([ai_parser.py:92-103](../../core/ai_parser.py)) has 8 top-level fields:
- `name`, `email`, `phone`, `linkedin_url`, `total_years` — primitive identity + summary
- `education` (`Education` model) — bachelors / masters / mba / university lists
- `experience` (`ExperienceBreakdown` model) — 17 categories split into "sector" (with company names) vs "functional" (with role titles)
- `general` (`GeneralData`) — international_locations + industries_specialized
- `languages` — list of language names
- `strategic_assessment` (list of `AssessmentItem`) — scored evaluation against process-specific characteristics

The 17 experience categories are the heart of the prompt complexity. The system prompt at [ai_parser.py:215-238](../../core/ai_parser.py) contains "Golden Rules" the model must apply — most notably:
- `management` includes ONLY real operating companies — never Consulting/Big4/Banks/PE/VC firms (those have their own categories).
- `portco_roles` only if explicitly stated as PE Portfolio Company.
- "International" means lived + worked >6 months.
- Company names in `companies` lists must be canonicalized (`"McKinsey"` not `"McKinsey & Company"`) — see the field-level descriptions on `SectorExperience.companies`.

The `matrix_characteristics` argument is the process-specific assessment matrix (from Supabase). When passed, the prompt includes the definitions and asks for one assessment per characteristic in the same order. When None, the model is told to return an empty `strategic_assessment` list. Same dual-mode logic appears in `process_linkedin`.

**Returns:** Python dict (Pydantic's `.model_dump()`) matching the `CVData` schema, or `None` on any OpenAI error (rate limit, network, malformed response).

**Where it's called from:**
- `harvester.HarvesterRelational._process_with_cv` — primary ingestion path.
- `harvester.HarvesterRelational._reprocess_ai_pending` — retry on previously-failed parses.
- `observer.Observer._logic_reprocess_ai_pending` — when a CV is added to a Main DB page after the fact.
- `observer.Observer._logic_enrich_cv` — when a Main DB candidate gets a CV via the enrichment path (no associated process).

### 2. `process_linkedin(linkedin_text, matrix_characteristics=None)` — LinkedIn parsing

Same `CVData` schema as `process_cv`, but with a different system prompt tailored to LinkedIn's markdown export shape ([ai_parser.py:304-338](../../core/ai_parser.py)). Key differences:
- Tells the model to set `email`, `phone`, `linkedin_url` to `null` — those come from the form data, not the profile text.
- Adds a "Date Calculation" rule because LinkedIn uses formats like `"Jan 2020 - Present"` that need parsing into durations.
- Everything else (capitalization, experience rules, functional classification) is duplicated from `process_cv` — two near-identical prompt blocks that drift over time.

Input is markdown text (typically from Exa.ai's `get_linkedin_profile`), truncated to 25 000 chars.

**Where it's called from:**
- `harvester.HarvesterRelational._process_with_linkedin` — Path B fallback when there's no CV but there is a LinkedIn URL.
- `observer.Observer._logic_reprocess_ai_pending` — when AI-pending candidate has LinkedIn but still no CV.
- `observer.Observer._logic_enrich_linkedin` — LinkedIn enrichment of a Main DB candidate.

### 3. `process_feedback_pdf(file_path)` — Interviewer feedback PDF → markdown

Reads the PDF, truncates to 50 000 chars, asks the model to identify the candidate's name and reformat the entire document into clean Notion-flavored markdown ([ai_parser.py:368-393](../../core/ai_parser.py)).

Output schema (`FeedbackResponse`):
```python
class FeedbackResponse(BaseModel):
    candidate_name: str
    feedback_markdown: str
```

The system prompt has strict formatting rules — use `#`/`##`/`###` for hierarchy (never `####` because Notion's `markdown_to_blocks` doesn't support them; see [core/markdown_to_blocks.py:106-107](../../core/markdown_to_blocks.py)). Tables become markdown tables. The model is told to preserve *all* content, not summarize.

The output markdown then goes through `core.markdown_to_blocks.markdown_to_notion_blocks` to produce a list of Notion block objects, which are appended to a new page in the Gathered Feedback child DB on the candidate's workflow page.

**Where it's called from:**
- `observer.Observer._handle_feedback_form` — only.

### 4. `process_feedback_assessment(cv_text, feedback_texts, assessment_characteristics)` — On-demand AI scoring

The newest AI operation. Combines CV + interview feedback into a scored matrix.

Input:
- `cv_text` (str or None) — full CV text
- `feedback_texts` (list of `{"title": str, "content": str}`) — each Gathered Feedback page's title (interviewer name + stage) and body content
- `assessment_characteristics` (list of `{"characteristic": str, "definition": str}`) — from Supabase `assessment_characteristics`

Output schema (`FeedbackAssessmentResponse`):
```python
class FeedbackAssessmentItem(BaseModel):
    characteristic: str
    score: StrategicScore  # High / Medium / Low / No
    cv_evidence: str  # max 20 words; "No CV available" if absent
    feedback_evidence: str  # max 20 words; "No feedback available" if absent

class FeedbackAssessmentResponse(BaseModel):
    assessment: List[FeedbackAssessmentItem]
    overall_summary: str
```

The output is rendered into a Notion table block (5 columns: Characteristic, Definition, Score, CV Evidence, Feedback Evidence) on a new page titled `"Feedback Assessment [AI-generated]"` in the Gathered Feedback child DB.

**Where it's called from:**
- `observer.Observer._handle_feedback_assessment` — only.

---

## The AI-pending reprocessing loop

When `process_cv` or `process_linkedin` returns None (typically due to OpenAI rate limit), the Harvester doesn't abort — it creates a skeleton candidate record marked AI-pending. Reprocessing happens later through one of two complementary paths.

### Skeleton creation (at ingest time)

In `_process_candidate_inner` ([harvester.py:416-457](../../scripts/harvester.py)):

```python
ai_data, public_url, ai_failed = self._process_with_cv(...)
if ai_failed:
    ai_data = self._create_minimal_candidate_data(form_data)  # all-zeros shape
needs_ai_pending = True  # in Path B / no AI

# later:
main_props[PROP_AI_PENDING] = {"checkbox": True}
```

The `ai_pending` checkbox on the Main DB page is the **source of truth** for "needs reprocessing." Supabase's `candidate_data.ai_pending` JSONB key is debugging metadata only — see the [.claude/rules/architecture.md](../../.claude/rules/architecture.md) explanation for why.

### Reprocessing path A: scheduled (Harvester)

`HarvesterRelational._reprocess_ai_pending` ([harvester.py:801](../../scripts/harvester.py)):
1. Query Notion Main DB for `AI Pending = true`.
2. Limit to 5 pages per run (batch limit).
3. For each: read Supabase candidate row, get the latest application (for `matrix_characteristics`), look for the CV URL on the Main DB page; fall back to the Workflow page's CV if missing.
4. Download, re-run `process_cv`, update Notion (only experience/education/languages) and Supabase JSONB, fill strategic assessments on workflow pages, uncheck AI Pending.

### Reprocessing path B: event-driven (Observer)

`Observer._logic_reprocess_ai_pending` ([observer.py:935](../../scripts/observer.py)):
1. Triggered when a Main DB page is edited (either by the sniper engine's `last_edited_time` filter, or by a webhook with `HANDLER_MAIN_CANDIDATE` + `AI Pending=true`).
2. Check if the page now has a CV or LinkedIn URL.
3. If CV: download, parse with `process_cv`. If only LinkedIn: fetch via Exa, parse with `process_linkedin`.
4. Update Notion + Supabase, fill strategic assessment on every active application's workflow page (the candidate may be in multiple processes).

The two paths cover complementary cases: the Harvester catches everything still pending after the OpenAI outage resolves; the Observer catches the user-driven case where a recruiter uploads a CV later.

### Why both?
The Observer path won't fire if the candidate's Main DB page was never edited after the failed ingest. The Harvester path needs scheduled runs to make progress when nothing is happening in Notion. Together they guarantee progress.

The downside: the same logic is implemented twice in two places, and the two implementations have drifted (different code paths for finding the CV URL, slightly different Notion property write sets). This is one of the audit notes worth keeping in mind on any refactor.

---

## CV text extraction

`_read_pdf` ([ai_parser.py:134-144](../../core/ai_parser.py)) uses `pdfplumber.open(file_path)` and iterates `page.extract_text()`. `_read_docx` ([ai_parser.py:146-156](../../core/ai_parser.py)) uses `python-docx`'s paragraph iteration.

Both functions return None on any exception. The CV-parsing pipeline treats None as "format unsupported / corrupted" and ultimately marks the candidate AI-pending. There's no OCR fallback — image-only PDFs are silently unprocessable.

Truncation is at 25 000 characters for CV/LinkedIn, 50 000 for feedback PDFs. A 30-page resume will lose its later pages. The model is not warned about truncation.

---

## Cost shape (rough)

The system prompt for `process_cv` is large (~5KB including the Pydantic schema definitions and the matrix characteristics). The CV text is up to 25KB. So input per call is ~30KB ≈ 7-8K tokens. Output is structured JSON: 17 experience objects × ~10 fields + identity + education + assessment list = 2-3K tokens.

At `gpt-5-mini` rates that's a few cents per CV. With `MAX_CVS_PER_RUN=15` and 6 Harvester runs per hour, the cap is ~90 CVs/hour ≈ $5-10/hour worst case. Real load is much lower.

Logfire instrumentation (if enabled) gives per-call token counts; the codebase also logs `usage.prompt_tokens` and `usage.completion_tokens` at DEBUG level — so you can grep CloudWatch for cost forensics during incidents.

---

## Known fragilities

1. **OpenAI 429 → AI Pending**. Survivable, but a long outage stacks pending candidates that all get retried in batches of 5 per Harvester run (every 10 min) — recovery from a 100-CV backlog is ~3 hours.
2. **No OCR**. PDFs that are image scans (no embedded text) return empty strings from `pdfplumber`, which leads to the model fabricating data or the parse failing. Currently uncaught.
3. **Hard-coded model name `gpt-5-mini`**. Bumping the model requires a code change + deploy.
4. **No prompt versioning**. The system prompt evolved over commits but there's no `prompt_version` field on outputs. If a future commit changes the prompt and downstream analytics depend on shape stability, there's no signal.
5. **Logfire is opt-in**. Without `LOGFIRE_TOKEN`, only summary token counts are logged. Full prompt + response traces aren't available in CloudWatch — you can't reconstruct what the model saw on a specific bad output.
