# Context Prompt for New Conversation

Copy everything below this line into a new conversation:

---

I'm debugging two issues in my recruitment automation system (nzyme-talent-engine). Read `CLAUDE.md` in the project root for full architecture context. I've done extensive investigation across multiple sessions and need to continue.

## Issue Under Investigation: PC RB Solutions CEO 2026Q1 — Stage Options Replaced

On **March 3, 2026**, the FactoryWorker Lambda ran and re-configured 5 existing processes, overwriting their Stage select options on the Workflow databases. This destroyed existing stage names (e.g., "0.4 On Hold", "0.5 Back-up") and replaced them with stages parsed from updated guidelines.

### What We Know For Sure

1. **AWS logs from March 3** show the FactoryWorker was triggered by a misconfigured webhook (every Notion workspace edit sent a webhook that day). It found **5 pending solicitudes** and ran `configurar_proceso()` on all of them, including PC RB Solutions CEO 2026Q1.

2. **The destructive code** is in `scripts/factory_worker.py` lines 191-195. `configurar_proceso()` overwrites ALL Stage options via `update_data_source()` before the Supabase registration step:
   ```python
   if opciones_stages:
       wf_ds_id = self.notion.get_data_source_id(wf_db_id)
       if wf_ds_id:
           wf_updates = {"Stage": {"select": {"options": opciones_stages}}}
           self.notion.update_data_source(wf_ds_id, properties=wf_updates)
   ```

3. **The Supabase INSERT failed** for 4 of 5 processes with `duplicate key value violates unique constraint "nzymerecruitingprocesses_workflow_unique"` — because these processes already existed. Since `exito_supa` was false, `Processed [Do not touch]` was NOT set to True by this run (line 252 was skipped).

4. **The FactoryWorker filter** (`buscar_solicitudes_pendientes()`, line 97-108) queries the Process Dashboard DB for pages where:
   ```python
   "Ready to be Processed [Do not touch]" = True AND "Processed [Do not touch]" = False
   ```

5. **THE UNSOLVED MYSTERY**: The `Processed [Do not touch]` checkbox on the PC RB Solutions CEO 2026Q1 Dashboard page has been **checked (True) since its creation in January**. The same applies to NZ Rotational Internship. Yet the FactoryWorker query returned these processes as pending. **How is this possible?**
   - There are no duplicate Dashboard pages for these processes
   - The checkbox was never set to False
   - But the Notion API filter `checkbox equals False` returned them anyway

### Hypotheses to Investigate

- **Notion API bug or race condition**: Could the webhook + query timing cause a stale read?
- **Notion filter behavior with specific property types**: Does the `"Processed [Do not touch]"` property name (with brackets) cause any issues?
- **Webhook-triggered state**: Could the webhook event itself temporarily affect the query results?
- **Different data source ID**: Could `buscar_solicitudes_pendientes()` be querying a different data source than expected?
- **Template pages or views**: Could Notion templates or database views appear as query results?
- Check Notion API audit logs if available
- Check if `_init_datasources()` returns the correct `dashboard_ds_id`

### Key IDs

| Entity | ID |
|--------|-----|
| Supabase Project | `yphbrpbwpakjduhmoimw` |
| Process Dashboard DB | env var `NOTION_PROCESS_DASHBOARD_DB_ID` (check `.env`) |
| PC RB Solutions Workflow DB | `2f183e67-e2e7-81a7-a0d5-cbb3f0f2cd49` |
| PC RB Solutions Process (Supabase) | `3deef129-6cc4-4457-bbab-ffa93129cc91` |
| NZ Rotational Workflow DB | `28383e67-e2e7-8296-8865-81181e02d5a7` |
| NZ Rotational Process (Supabase) | `ec794213-01f6-48e1-b234-aea44471aa02` |
| Webhook page entity | `31883e67-e2e7-8097-aef4-f25159c36942` |
| Webhook parent DB | `2f783e67-e2e7-8123-b3f1-dedd84c92732` |
| Webhook parent data source | `2f783e67-e2e7-81f7-8aca-000bb87a5325` |

### AWS Logs (March 3, 2026)

```
16:33:48 [TRIGGER] Detectada Petición HTTP (Webhook).
16:33:48 WEBHOOK BODY: {"entity":{"id":"31883e67-e2e7-8097-aef4-f25159c36942","type":"page"},"type":"page.content_updated","data":{"parent":{"id":"2f783e67-e2e7-8123-b3f1-dedd84c92732","type":"database","data_source_id":"2f783e67-e2e7-81f7-8aca-000bb87a5325"},...}}
16:33:48 Iniciando entorno para Factory...
16:33:48 FactoryWorker iniciando
16:33:49 No requests found, retrying in 10s... (0/90s elapsed)
16:33:59 No requests found, retrying in 10s... (10/90s elapsed)
16:34:10 No requests found, retrying in 10s... (20/90s elapsed)
16:34:21 No requests found, retrying in 10s... (30/90s elapsed)
16:34:31 No requests found, retrying in 10s... (40/90s elapsed)
16:34:41 No requests found, retrying in 10s... (50/90s elapsed)
16:34:53 Procesando 5 solicitudes
16:34:53 Configurando: NZ Germany Fund Partner (Leadership Team - Fund Partner)
16:35:01 [CRITICAL] No se encontraron las DBs hijas principales (Workflow/Form). Revisa el template.
16:35:01 Configurando: PC RB Solutions CEO 2026Q1 (PortCo - CEO)
16:35:17 Extracted 13 matrix characteristics from template
16:35:20 [PARSER] Analizando pagina 2d683e67-e2e7-811a-a7e7-e7cf9453fbc9...
16:35:52 [ERROR] Fallo al registrar proceso: duplicate key "nzymerecruitingprocesses_workflow_unique" Key (notion_workflow_id)=(2f183e67-e2e7-81a7-a0d5-cbb3f0f2cd49) already exists.
16:35:52 [ERROR] Error al registrar en Supabase
16:35:52 Configurando: NZ Rotational Internship 2026Q1 (Internship Programme - Rotational Internship)
16:36:05 No 'Past Experience' database found in template
16:36:07 [PARSER] Analizando pagina 2f483e67-e2e7-805a-a046-c2281a2b72ff...
16:36:20 [ERROR] Fallo al registrar proceso: duplicate key Key (notion_workflow_id)=(28383e67-e2e7-8296-8865-81181e02d5a7) already exists.
16:36:20 [ERROR] Error al registrar en Supabase
16:36:20 Configurando: PC Azenea CPO 2026Q1 (PortCo - CPO (Product))
16:36:30 Extracted 10 matrix characteristics from template
16:36:31 [PARSER] Analizando pagina 30383e67-e2e7-80d4-9506-d4b6727a25a9...
16:36:47 [ERROR] Fallo al registrar proceso: duplicate key Key (notion_workflow_id)=(30383e67-e2e7-81fb-8d52-fd1d6710cf1f) already exists.
16:36:47 [ERROR] Error al registrar en Supabase
16:36:47 Configurando: NZ Tech Lead 2026Q1 (Tech - Lead)
16:36:57 Extracted 9 matrix characteristics from template
16:37:01 [PARSER] Analizando pagina 2f683e67-e2e7-805d-957a-e0d700d8255c...
16:37:26 [ERROR] Fallo al registrar proceso: duplicate key Key (notion_workflow_id)=(30683e67-e2e7-81dc-ba1b-e3484ffa7d57) already exists.
16:37:26 [ERROR] Error al registrar en Supabase
16:37:26 Ejecución completada
```

### Confirmed Bugs (from prior sessions, fixes still needed)

1. **`configurar_proceso()` does destructive operations BEFORE the Supabase guard** — Stage overwrite, content appending, title renaming all happen at steps 6-8, but `Processed=True` only happens at step 10 if Supabase succeeds. Fix: check Supabase existence FIRST and skip if process already registered.

2. **Stage options update is destructive** — replaces all options instead of merging. Fix: read existing options, merge new ones, only add what's missing.

3. **`crear_aplicacion()` UPSERT in `core/supabase_client.py`** silently overwrites `current_stage` on existing applications without creating a history entry (separate issue affecting NZ Rotational candidates).

### Files to Read First

- `scripts/factory_worker.py` — Full FactoryWorker code (290 lines)
- `core/constants.py` — Property name constants
- `core/notion_client.py` — Notion API wrapper (check `query_data_source` implementation)
- `main_lambda.py` — Lambda entry point, webhook routing logic

### What to Do Next

1. **Solve the mystery**: How did `buscar_solicitudes_pendientes()` return 5 processes whose `Processed [Do not touch]` checkboxes were already True? Start by:
   - Reading `main_lambda.py` to understand how webhooks route to FactoryWorker
   - Reading `core/notion_client.py` to check `query_data_source` implementation
   - Querying the Process Dashboard via Notion MCP to see current state of all entries
   - Checking if there could be template pages, archived pages, or linked database views that match the filter

2. **After solving the mystery**, implement fixes for the three confirmed bugs listed above.

## Second Issue (Deferred): NZ Rotational — 5 Candidates Reset to Initial Stage

This is a separate issue. Full audit report is at `docs/audit_march_2026.md`. The confirmed mechanism is the `crear_aplicacion()` UPSERT bug, but the exact trigger timing is still uncertain. Address this after resolving the CEO process issue.
