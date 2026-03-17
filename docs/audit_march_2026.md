# Nzyme Talent Engine — System Audit Report

**Date:** March 9, 2026
**Scope:** Root cause analysis for two incidents reported in the NZ Rotational Internship 2026Q1 and PC RB Solutions CEO 2026Q1 recruiting processes.

---

## Executive Summary

Two distinct bugs in the Nzyme Talent Engine caused unintended data mutations in production:

1. **Issue 1 (NZ Rotational):** Five previously-disqualified candidates had their Supabase `current_stage` silently reset to `"0.1 Identified"` — the initial stage — with no audit trail. The Observer subsequently detected the mismatch and logged corrective transitions.

2. **Issue 2 (PC RB Solutions):** The FactoryWorker destructively overwrote Stage options on the Workflow database, removing stages like "0.4 On Hold" and "0.5 Back-up" and adding new ones from updated guidelines.

Three bugs were identified, with recommended fixes for each.

---

## Issue 1: NZ Rotational — Candidates Reset to Initial Stage

### Affected Candidates

| Candidate | Application ID | Last Legitimate Stage | Date of Last Legitimate Transition |
|-----------|---------------|----------------------|-----------------------------------|
| Georgian Tomozei | `3255a320...` | Discarded | Feb 2, 2026 |
| Irene Borges Rodríguez | `561e36f0...` | Discarded | Feb 13, 2026 |
| Marcos Fuentes García | `03390f44...` | Discarded | Feb 20, 2026 |
| Pablo Rosal Ayuso | `c91fa9f7...` | Discarded | Feb 13, 2026 |
| Javier Gordo Martín | `11981640...` | Lost | Feb 16, 2026 |

All five belong to the NZ Rotational Internship 2026Q1 process (`process_id: ec794213-01f6-48e1-b234-aea44471aa02`).

### Timeline of Events (March 5, 2026)

| Time (UTC) | Event | Evidence |
|------------|-------|----------|
| 10:35:26 | `_reprocess_ai_pending()` updates 5 `NzymeTalentNetwork` rows — backfills AI-extracted experience/education fields, clears `ai_pending` flag | `NzymeTalentNetwork.updated_at` = `2026-03-05 10:35:26` for all 5 |
| ~10:35-10:36 | `_rellenar_strategic_assessment()` writes to "Past Experience [AI-generated]" child databases inside each candidate's workflow page | This updates each workflow page's `last_edited_time` in Notion |
| 19:25:30-31 | Observer detects mismatch: Supabase `current_stage` = `"0.1 Identified"` (no ZWSP) ≠ Notion Stage (disqualification stage with ZWSPs). Logs corrective transitions via `registrar_cambio_stage()` | 5 entries in `NzymeRecruitingProcessHistory` with `from_stage = "0.1 Identified"` (no ZWSP), `to_stage` = respective disqualification stages (with ZWSPs) |
| 19:25-19:35 | 2 additional candidates (Álvaro de Castro Miranda, Marcos García Martín) also have Observer-detected transitions, but these are **normal** — their `from_stage` values contain ZWSPs | Different pattern from the anomalous 5 |

### Root Cause: `crear_aplicacion()` UPSERT Bug

**Location:** `core/supabase_client.py`, lines 132-152

The `crear_aplicacion()` method uses a Supabase UPSERT with `on_conflict="candidate_id, process_id"`:

```python
def crear_aplicacion(self, candidate_uuid, notion_wf_id, notion_page_id, stage_inicial):
    proc_res = self.client.table("NzymeRecruitingProcesses") \
        .select("id").eq("notion_workflow_id", notion_wf_id).execute()
    process_uuid = proc_res.data[0]['id']
    app_data = {
        "candidate_id": candidate_uuid,
        "process_id": process_uuid,
        "notion_page_id": notion_page_id,
        "current_stage": stage_inicial,   # ← ALWAYS overwrites
        "status": "Active"
    }
    self.client.table("NzymeRecruitingApplications").upsert(
        app_data, on_conflict="candidate_id, process_id"
    ).execute()
```

**Problem:** When an application already exists for a `(candidate_id, process_id)` pair, the UPSERT silently **overwrites** `current_stage` back to `stage_inicial` (which is `"0.1 Identified"`). It does not check whether the application already exists, does not create a history entry, and does not set `updated_at` — making the reset **completely invisible** in Supabase timestamp forensics.

**Only call site:** `scripts/harvester.py`, line 485, inside `procesar_candidato()`. Confirmed via full codebase grep — no other code path calls this method.

### Trigger Mechanism

The Harvester's `run_once()` executes three steps in order:

1. **Step 1:** `procesar_bulk_imports()`
2. **Step 2:** Standard processing — queries each workflow DB for pages with `Processed=False AND ID is_not_empty`, then calls `procesar_candidato()` → `crear_aplicacion()`
3. **Step 3:** `_reprocess_ai_pending()`

**Critical finding from git history:** The Jan 19 and Jan 29 code versions (deployed when these candidates were originally processed in January 2026) had an early-return on AI failure:

```python
# Jan 19/29 version of procesar_candidato():
datos_ia = self.ai.procesar_cv(local_path)
if not datos_ia: return   # ← Returns BEFORE crear_aplicacion() and BEFORE Processed=True
```

In these versions, if the OpenAI API returned an error (e.g., rate limit 429), the function exited early:
- `Processed` checkbox on workflow page remained `False`
- `crear_aplicacion()` was never called
- The candidate skeleton existed in the Main DB but had no complete application record

The Feb 12 "Major update" commit introduced the `ai_failed`/`ai_pending` pattern, which allows `procesar_candidato()` to continue even on AI failure, calling `crear_aplicacion()` and setting `Processed=True` on the workflow page.

**Most likely sequence:**

1. **Late January:** Original processing with Jan 19 code. AI fails for some candidates → workflow pages left with `Processed=False`. Applications may or may not have been created at this point.
2. **Feb 12+:** New code deployed. A Harvester run's Step 2 finds workflow pages still with `Processed=False` and re-processes them → `crear_aplicacion()` UPSERT silently resets `current_stage` to `"0.1 Identified"` on any pre-existing applications.
3. **Feb–March:** The Supabase `current_stage` sits at `"0.1 Identified"` while the Notion Stage property reflects the disqualification stages (set manually or by earlier Observer runs). No system detects the mismatch because the Observer's 25-minute lookback window doesn't pick up these pages unless their `last_edited_time` changes.
4. **March 5, ~10:35:** `_reprocess_ai_pending()` runs, updating the 5 candidates' Main DB pages and writing strategic assessments to their workflow pages' child databases. This updates the workflow pages' `last_edited_time` in Notion.
5. **March 5, 19:25:** The Observer's lookback window now captures these workflow pages. It reads Notion Stage (disqualification stage with ZWSPs) and compares to Supabase `current_stage` (`"0.1 Identified"`, no ZWSPs). Mismatch detected → corrective `registrar_cambio_stage()` entries logged.

**Key evidence supporting this chain:**
- `"0.1 Identified"` (no ZWSP) is exactly what `determinar_stage_inicial()` returns — index 0 of the Stage options, which has zero ZWSP characters prepended.
- Only `crear_aplicacion()` can produce this value in Supabase without a history entry.
- The `crear_aplicacion()` UPSERT does not set `updated_at`, explaining why the `NzymeRecruitingApplications.updated_at` timestamps for these 5 candidates don't show the reset — they still reflect the original creation time.
- `_reprocess_ai_pending()` was confirmed (by full code review) to NOT call `crear_aplicacion()`.
- The 2 additional candidates with March 5 transitions (Álvaro, Marcos García Martín) had **normal** Observer-detected transitions with ZWSP-prefixed `from_stage` values, confirming they were not affected by this bug.

### Remaining Uncertainty

The exact date of the `crear_aplicacion()` UPSERT re-trigger cannot be determined from Supabase data because:
1. The UPSERT does not set `updated_at`
2. No database trigger auto-updates `updated_at` on the `NzymeRecruitingApplications` table
3. No `NzymeRecruitingProcessHistory` entry was created (by design — `crear_aplicacion()` doesn't log history)

The most likely window is **between Feb 12 and early March 2026**, during a routine Harvester run after the code update.

---

## Issue 2: PC RB Solutions — Stage Options Replaced

### What Happened

The Stage select property on the PC RB Solutions CEO 2026Q1 Workflow database had some options removed (e.g., "0.4 On Hold", "0.5 Back-up") and new ones added. This caused Notion to remap existing candidates to the closest surviving stage.

### Root Cause: FactoryWorker Destructive Stage Update

**Location:** `scripts/factory_worker.py`, lines 191-195

```python
if opciones_stages:
    wf_ds_id = self.notion.get_data_source_id(wf_db_id)
    if wf_ds_id:
        wf_updates = {"Stage": {"select": {"options": opciones_stages}}}
        self.notion.update_data_source(wf_ds_id, properties=wf_updates)
```

**Problem:** The `update_data_source()` call **replaces** the entire set of Stage options with `opciones_stages` (parsed from the guidelines document). When guidelines change — adding, removing, or renaming stages — this call destroys any Stage options not present in the new guidelines.

The FactoryWorker was designed to run once during initial process setup. However, if it re-runs (e.g., due to the dashboard page's `Processed` checkbox being reset, or during debugging), it overwrites the Stage options with whatever the guidelines currently specify.

**Sequence:**
1. Process originally configured with one set of stages
2. Guidelines document updated (stages added/removed/renamed)
3. FactoryWorker re-ran (or ran on a new dashboard entry for the same process)
4. `update_data_source()` replaced all Stage options → Notion remapped existing candidates
5. Observer detected the Stage changes and logged transitions in history

---

## Bugs Identified & Recommended Fixes

### Bug 1: Destructive UPSERT in `crear_aplicacion()`

**Severity:** High
**File:** `core/supabase_client.py`

**Current behavior:** UPSERT blindly overwrites `current_stage` on conflict, resetting applications to their initial stage.

**Fix:** Check if the application exists before inserting. If it exists, skip or raise a warning:

```python
def crear_aplicacion(self, candidate_uuid, notion_wf_id, notion_page_id, stage_inicial):
    proc_res = self.client.table("NzymeRecruitingProcesses") \
        .select("id").eq("notion_workflow_id", notion_wf_id).execute()
    process_uuid = proc_res.data[0]['id']

    # CHECK: Does application already exist?
    existing = self.client.table("NzymeRecruitingApplications") \
        .select("id") \
        .eq("candidate_id", candidate_uuid) \
        .eq("process_id", process_uuid) \
        .execute()

    if existing.data:
        self.logger.info(f"Application already exists for candidate {candidate_uuid} "
                         f"in process {process_uuid}. Skipping creation.")
        return existing.data[0]['id']

    # INSERT (not UPSERT) — only for new applications
    app_data = {
        "candidate_id": candidate_uuid,
        "process_id": process_uuid,
        "notion_page_id": notion_page_id,
        "current_stage": stage_inicial,
        "status": "Active"
    }
    res = self.client.table("NzymeRecruitingApplications") \
        .insert(app_data).execute()
    return res.data[0]['id'] if res.data else None
```

### Bug 2: No History Entry on Application Creation

**Severity:** Medium
**File:** `core/supabase_client.py`

**Current behavior:** `crear_aplicacion()` sets `current_stage` but does not log an initial history entry in `NzymeRecruitingProcessHistory`.

**Fix:** Add an initial history entry after creating a new application:

```python
# After successful INSERT:
app_id = res.data[0]['id']
self.client.table("NzymeRecruitingProcessHistory").insert({
    "application_id": app_id,
    "from_stage": None,
    "to_stage": stage_inicial
}).execute()
```

### Bug 3: Destructive Stage Options Update in FactoryWorker

**Severity:** Medium
**File:** `scripts/factory_worker.py`

**Current behavior:** `configurar_proceso()` replaces all Stage options with those from guidelines, destroying any options that were previously configured.

**Fix:** Merge new options with existing ones instead of replacing:

```python
if opciones_stages:
    wf_ds_id = self.notion.get_data_source_id(wf_db_id)
    if wf_ds_id:
        # Read existing options first
        schema = self.notion.get_database_schema(wf_ds_id)
        existing_options = schema.get("Stage", {}).get("select", {}).get("options", [])
        existing_names = {opt["name"] for opt in existing_options}

        # Only add new options, never remove existing ones
        merged = list(existing_options)
        for opt in opciones_stages:
            if opt["name"] not in existing_names:
                merged.append(opt)

        wf_updates = {"Stage": {"select": {"options": merged}}}
        self.notion.update_data_source(wf_ds_id, properties=wf_updates)
```

### Additional Recommendation: Add `updated_at` Trigger

The `NzymeRecruitingApplications` table lacks an auto-update trigger on `updated_at`, which made this incident harder to diagnose.

```sql
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON "NzymeRecruitingApplications"
    FOR EACH ROW
    EXECUTE FUNCTION update_modified_column();
```

---

## Appendix: Key Database IDs

| Entity | ID |
|--------|----|
| Supabase Project | `yphbrpbwpakjduhmoimw` |
| NZ Rotational Process | `ec794213-01f6-48e1-b234-aea44471aa02` |
| NZ Rotational Workflow DB | `28383e67-e2e7-8296-8865-81181e02d5a7` |
| NZ Rotational Form DB | `d1383e67-e2e7-83fb-a66e-810e31d6c053` |
| PC RB Solutions Process | `3deef129-6cc4-4457-bbab-ffa93129cc91` |
| PC RB Solutions Workflow DB | `2f183e67-e2e7-81a7-a0d5-cbb3f0f2cd49` |

## Appendix: Git Commit History

| Date | Hash | Message |
|------|------|---------|
| Jan 9, 2026 | `4b6e8f2` | First commit |
| Jan 19, 2026 | `911b7aa` | Versión 2; Antes de modificar para AWS |
| Jan 29, 2026 | `4e1daf3` | Estado de antes de Claude |
| Feb 12, 2026 | `24cb15a` | Major update: Exa LinkedIn enrichment, harvester/observer refactor, AI parser expansion |
