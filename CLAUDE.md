# CLAUDE.md - Nzyme Talent Engine

## WHAT: Project Overview

A serverless recruitment automation system deployed on AWS Lambda. Processes CVs with AI, manages candidates across Notion (UI) and Supabase (SQL), and tracks recruiting workflows.

### Tech Stack
- **Runtime**: Python 3.11 on AWS Lambda (Docker container)
- **Databases**: Supabase (PostgreSQL + JSONB + Storage), Notion (low-code CRM)
- **AI**: OpenAI GPT-5-mini with Pydantic structured output
- **PDF**: pdfplumber for text extraction
- **LinkedIn**: Exa.ai for profile scraping (exa-py, optional)
- **Observability**: Logfire for OpenAI tracing
- **Config**: python-dotenv for environment variables

### Project Structure

```
nzyme-talent-engine/
├── main_lambda.py          # Lambda entry point - routes events to workers
├── Dockerfile              # AWS Lambda container image (Python 3.11)
├── requirements.txt        # Python dependencies
├── core/                   # Shared clients and utilities
│   ├── notion_client.py    # Notion API wrapper (CRUD, queries, schema)
│   ├── notion_builder.py   # Constructs Notion page properties (data → Notion)
│   ├── notion_parser.py    # Parses Notion page properties (Notion → data)
│   ├── supabase_client.py  # SQL operations + identity resolution engine
│   ├── storage_client.py   # Supabase Storage for CV files (permanent URLs)
│   ├── ai_parser.py        # CV + LinkedIn parsing with Pydantic models
│   ├── exa_client.py       # Exa.ai wrapper for LinkedIn profile scraping
│   ├── domain_mapper.py    # AI output → database payloads
│   ├── guidelines_parser.py# Fetches interview stage templates
│   ├── constants.py        # All Notion property names (CRITICAL)
│   ├── utils.py            # File download helpers
│   └── logger.py           # CloudWatch-compatible logging
├── scripts/                # Worker implementations
│   ├── factory_worker.py   # Sets up new recruiting processes
│   ├── harvester.py        # Downloads and processes CVs
│   └── observer.py         # Monitors changes, syncs state
├── docs/                   # Architecture documentation
│   ├── architecture/       # System overview docs
│   ├── data_models/        # Data model docs
│   └── workers/            # Worker-specific docs
├── logs/                   # Local log output (app.log)
└── tmp/                    # Temporary file storage
```

### Key Files to Understand First

**Workers (the core business logic):**
- `scripts/harvester.py` - CV ingestion: download → AI parse → identity resolution → save
- `scripts/observer.py` - Change monitoring: stage transitions, feedback, rejections
- `scripts/factory_worker.py` - Process setup: creates Notion databases and templates

**Supporting modules:**
- `main_lambda.py` - Lambda entry point, routes events to workers
- `core/constants.py` - All Notion property names (edit here when schema changes)
- `core/supabase_client.py` - SQL operations + identity resolution engine
- `core/ai_parser.py` - Pydantic models that define AI extraction structure
- `core/notion_builder.py` - Builds Notion page payloads from candidate data (data → Notion)
- `core/notion_parser.py` - Parses Notion properties back into data dicts (Notion → data)
- `core/exa_client.py` - LinkedIn profile fetching via Exa.ai (optional)

---

## WHY: System Purpose

### The Three Workers

| Worker | Trigger | Purpose |
|--------|---------|---------|
| **Factory** | Webhook | Creates new recruiting processes (databases, stages, templates) |
| **Harvester** | Scheduled | Downloads CVs, parses with AI, creates candidate records |
| **Observer** | Scheduled | Monitors stage changes, feedback uploads, rejections |

### Data Flow
```
Notion Form → Harvester → AI Parser → Supabase + Notion Main DB
                                            ↓
                              Observer monitors → syncs changes back
```

### Identity Resolution (4-Rule Engine)
Located in `supabase_client.py::gestion_candidato()`:
1. Email match → Merge with existing candidate
2. Name match + different email → New candidate
3. Name match + no email conflict → Merge
4. No match → Create new record

### Supabase Tables
- `NzymeTalentNetwork` - Candidate profiles (SQL columns + JSONB candidate_data)
- `NzymeRecruitingProcesses` - Active recruiting processes
- `NzymeRecruitingApplications` - Candidate-to-process links
- `NzymeRecruitingProcessHistory` - Stage transition audit log

### Notion Databases
- **Main DB** - Master candidate records with multi-select experience tags
- **Workflow DB** - Per-process application tracking (one per process)
- **Form DB** - New CV submissions queue
- **Bulk DB** - CSV import queue
- **Feedback DB** - Interviewer feedback documents

---

## HOW: Coding Guidelines

### Language Convention
- **Code/variables**: English
- **Logs/comments**: English, try to keep logs minimum.
- **Notion properties**: English (defined in constants.py)

### Architecture Patterns

**Dependency Injection** - Workers receive initialized clients:
```python
bot = HarvesterRelational(notion_client, supa_client, storage_client, ai_analyzer, exa_client=exa)
```

**Lazy Initialization** - Lambda only loads what each task needs:
```python
if task_name == "harvester":
    n_client = NotionClient()  # Only instantiate if needed
```

**run_once() Pattern** - Workers execute a single pass:
```python
def run_once(self):
    # Process all pending items, then exit
```

### Code Style

**Early returns on errors**:
```python
if not notion_url:
    return  # Exit early, don't nest
```

**Numbered steps in complex methods**:
```python
# 1. Download CV
# 2. Parse with AI
# 3. Identity resolution
# 4. Write to databases
# 5. Mark as processed
```

**Constants for property names** (never hardcode):
```python
from core.constants import PROP_CHECKBOX_PROCESSED, PROP_NAME
# NOT: "Processed", "Name"
```

**Logger per module**:
```python
self.logger = get_logger("Harvester")
```

### Pydantic for AI Output
All AI responses use structured output with Pydantic models in `ai_parser.py`:
```python
class CVData(BaseModel):
    name: str
    email: Optional[str]
    experience: ExperienceBreakdown
    # ... fields with Field(description="...")
```

### Error Handling
- Log errors with context but don't crash the batch
- Mark items as processed even on partial failure (prevents infinite loops)
- Use try/except around external API calls

### Testing Locally
```bash
# In main_lambda.py, uncomment the desired test event:
fake_schedule_event = {"task": "harvester"}
python main_lambda.py
```

### When Adding New Notion Properties
1. Add constant to `core/constants.py`
2. Update `notion_builder.py` to write the property (data → Notion)
3. Update `notion_parser.py` to read the property (Notion → data)
4. Update `domain_mapper.py` if it affects Supabase
5. Update Pydantic models in `ai_parser.py` if AI should extract it

### Environment Variables (.env)
```
NOTION_KEY=secret_xxx
NOTION_MAIN_DB_ID=xxx
NOTION_PROCESS_DASHBOARD_DB_ID=xxx
SUPABASE_URL=xxx
SUPABASE_KEY=xxx
OPENAI_API_KEY=xxx
EXA_API_KEY=xxx          # Optional: enables LinkedIn enrichment via Exa.ai
```
