import sys
import os
import time
import httpx
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import CVAnalyzer
from core.notion_parser import NotionParser
from core.notion_builder import NotionBuilder
from core.domain_mapper import DomainMapper
from core.utils import download_file
from core.logger import get_logger
from core.markdown_to_blocks import markdown_to_notion_blocks
from core.constants import (
    PROP_NAME, PROP_EMAIL, PROP_PHONE, PROP_LINKEDIN, PROP_CV_FILES,
    PROP_NEXT_STEPS, PROP_PROCESS_HISTORY, PROP_TEAM_ROLE,
    PROP_CHECKBOX_PROCESSED, PROP_HEADHUNTER_FEEDBACK, PROP_AI_PENDING,
    PROP_ASSESSMENT_REQUESTED,
    PROP_EXP_TOTAL_YEARS, PROP_EXP_CONSULTING, PROP_EXP_AUDIT,
    PROP_EXP_IB, PROP_EXP_PE, PROP_EXP_VC, PROP_EXP_ENGINEER,
    PROP_EXP_LAWYER, PROP_EXP_FOUNDER, PROP_EXP_CORP_MA, PROP_EXP_PORTCO,
    PROP_EXP_MANAGEMENT, PROP_EXP_FINANCE, PROP_EXP_MARKETING,
    PROP_EXP_OPERATIONS, PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE,
    PROP_EXP_TECHNOLOGY, PROP_EXP_INTERNATIONAL, PROP_EXP_INDUSTRIES,
    PROP_LANGUAGES, PROP_EDU_BACHELORS, PROP_EDU_MASTERS,
    PROP_EDU_UNIVERSITIES, PROP_EDU_MBAS,
)



load_dotenv()



# --- CONFIGURATION ---
LOOKBACK_MINUTES = 25
MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
PROCESS_DASHBOARD_DB_ID = os.getenv("NOTION_PROCESS_DASHBOARD_DB_ID")
CENTRAL_REFS_DB_ID = os.getenv("NOTION_REFERENCES_DB_ID")
INTERNAL_REFS_DB_TITLE = "Candidate References [Input here feedback received]"
TEMP_FOLDER = "/tmp/temp_downloads"



if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)



class Observer:
    def __init__(self, notion_client, supa_client, storage_client, ai_analyzer, exa_client=None):
        self.logger = get_logger("Observer")
        self.notion = notion_client
        self.supa = supa_client
        self.storage = storage_client
        self.ai = ai_analyzer
        self.exa = exa_client



        self.main_ds_id = self.notion.get_data_source_id(MAIN_DB_ID) or MAIN_DB_ID
        self.dashboard_ds_id = self.notion.get_data_source_id(PROCESS_DASHBOARD_DB_ID) or PROCESS_DASHBOARD_DB_ID
        self.refs_ds_id = self.notion.get_data_source_id(CENTRAL_REFS_DB_ID) or CENTRAL_REFS_DB_ID



    # =========================================================================
    # 0. API HELPER (Rate Limit Protection)
    # =========================================================================

    def _api_request(self, method, url, json=None, max_retries=3):
        """
        Wrapper for direct Notion API calls with retry on 429 (rate limit).
        Returns the Response object, or None if all retries are exhausted.
        """
        for attempt in range(max_retries):
            try:
                resp = httpx.request(method, url, headers=self.notion.headers, json=json)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", 1))
                    self.logger.warning(f"[RATE LIMIT] 429 on {method} {url} — retrying in {retry_after}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_after)
                    continue
                return resp
            except httpx.HTTPError as e:
                self.logger.error(f"[API] Request failed: {method} {url} — {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
        self.logger.error(f"[API] All {max_retries} retries exhausted for {method} {url}")
        return None

    # =========================================================================
    # 1. SURVEILLANCE ENGINES
    # =========================================================================



    def _engine_sniper(self, db_id, handler_func, context=None, label="SNIPER"):
        """
        ENGINE 1: Direct access by ID.
        Filters by (Created > Time) OR (Edited > Time).
        """
        if not db_id: return



        now = datetime.now(timezone.utc)
        start_time = (now - timedelta(minutes=LOOKBACK_MINUTES)).isoformat()


        filter_params = {
            "or": [
                {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": start_time}
                },
                {
                    "timestamp": "created_time",
                    "created_time": {"after": start_time}
                }
            ]
        }


        self.logger.debug(f"[{label}] Scanning DB {db_id[:8] if db_id else '?'}... lookback={LOOKBACK_MINUTES}m")
        try:
            pages = self.notion.query_data_source(db_id, filter_params)
            self.logger.debug(f"[{label}] {len(pages) if pages else 0} page(s) found in lookback window")
            if pages:
                self.logger.info(f"[{label}] Processing {len(pages)} changes in DB {db_id}")
                for page in pages:
                    try:
                        handler_func(page, context)
                    except Exception as e:
                        self.logger.error(f"Error in handler {label}: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Sniper engine failed (DB {db_id}): {e}")



    def _engine_radar(self, db_title_query, handler_func, label="RADAR"):
        """
        ENGINE 2 (STRICT MATCH): Data Source Hunter.
        Searches Data Sources by name, applies an EXACT FILTER in Python
        to avoid 400 errors on non-matching databases.
        """
        url = "https://api.notion.com/v1/search"
        payload = {
            "query": db_title_query,
            "filter": {"value": "data_source", "property": "object"},
            "page_size": 50
        }

        self.logger.debug(f"[{label}] Searching data sources with query='{db_title_query}'")
        try:
            resp = self._api_request("POST", url, json=payload)
            if not resp or resp.status_code != 200:
                self.logger.error(f"[{label}] API Search error: {resp.status_code if resp else 'No response'}")
                return


            results = resp.json().get("results", [])
            self.logger.debug(f"[{label}] Search returned {len(results)} result(s)")

            for ds in results:
                ds_id = ds["id"]

                ds_name = "Unnamed"
                if "title" in ds and ds["title"]:
                    ds_name = ds["title"][0]["plain_text"]
                elif "name" in ds:
                    ds_name = ds["name"]

                if ds_name.strip() != db_title_query.strip():
                    self.logger.debug(f"[{label}] Skipping non-exact match: '{ds_name}'")
                    continue
                self.logger.debug(f"[{label}] Exact match found: '{ds_name}' ({ds_id[:8]}...)")


                if ds.get("archived", False): continue


                try:
                    pending_filter = {
                        "property": "Processed",
                        "checkbox": {"equals": False}
                    }

                    rows = self.notion.query_data_source(ds_id, pending_filter)

                except Exception:
                    continue


                if not rows: continue


                parent = ds.get("parent", {})
                p_type = parent.get("type")
                p_id = parent.get(p_type)


                candidate_id = self._find_candidate_ancestor(p_id, p_type)

                if candidate_id:
                    self.logger.info(f"[{label}] Processing {len(rows)} entries in '{ds_name}'")
                    context = {"candidate_id": candidate_id}

                    for page in rows:
                        try:
                            handler_func(page, context)
                        except Exception as e:
                            self.logger.error(f"Error in row handler: {e}")


        except Exception as e:
            self.logger.error(f"[{label}] Critical exception: {e}", exc_info=True)



    # =========================================================================
    # 2. HANDLERS (PURE BUSINESS LOGIC)
    # =========================================================================

    def _handle_main_candidate(self, page, _=None):
        """Handler: Talent Network (CV enrichment + Supabase sync + DISPATCHER)"""

        # 1. Check for assignment to new process (high priority)
        props = page["properties"]
        assign_rel = props.get("Assign to Active Process", {}).get("relation", [])

        if assign_rel:
            process_dashboard_page_id = assign_rel[0]["id"]
            self.logger.debug(f"[MAIN_CANDIDATE] Path: dispatch to process {process_dashboard_page_id[:8]}...")
            self.logger.info(f"[DISPATCH] Move request detected to page: {process_dashboard_page_id}")
            self._logic_dispatch_candidate_to_form(page, process_dashboard_page_id)
            return

        # 2. AI Pending reprocessing (CV or LinkedIn was added to a pending candidate)
        is_ai_pending = props.get(PROP_AI_PENDING, {}).get("checkbox", False)
        if is_ai_pending:
            has_cv = bool(props.get(PROP_CV_FILES, {}).get("files", []))
            has_linkedin = bool(props.get(PROP_LINKEDIN, {}).get("url"))
            if has_cv or has_linkedin:
                self.logger.debug(f"[MAIN_CANDIDATE] Path: AI-pending reprocess (has_cv={has_cv}, has_linkedin={has_linkedin})")
                self._logic_reprocess_ai_pending(page)
                return

        # 3. Enrichment (only if not already processed)
        if not props.get(PROP_CHECKBOX_PROCESSED, {}).get("checkbox", False):
            self.logger.debug("[MAIN_CANDIDATE] Path: enrichment (not yet processed)")
            was_enriched = self._logic_enrich_cv(page)
            if was_enriched:
                return

            was_enriched = self._logic_enrich_linkedin(page)
            if was_enriched:
                return

        # 4. Basic Supabase sync (always runs as fallback)
        self.logger.debug("[MAIN_CANDIDATE] Path: basic Supabase sync (fallback)")
        page_id = page["id"]
        data_update = NotionParser.parse_candidate_properties(props)
        self.supa.manage_candidate(data_update, page_id)



    def _handle_process_dashboard(self, page, _=None):
        """Handler: Dashboard (Update Open/Closed status)"""
        self.sync_process_status(page)



    def _handle_workflow_item(self, page, process_context):
        """Handler: Workflow (Detect stage change or assessment request)"""
        page_id = page["id"]
        props = page["properties"]

        # Check for assessment request (priority over stage change)
        if props.get(PROP_ASSESSMENT_REQUESTED, {}).get("checkbox", False):
            self.logger.info(f"[WORKFLOW] Assessment requested for page {page_id[:8]}...")
            self._handle_feedback_assessment(page, process_context)
            return

        stage_prop = props.get("Stage", {})
        current_stage = None
        if stage_prop.get("select"): current_stage = stage_prop["select"]["name"]
        elif stage_prop.get("status"): current_stage = stage_prop["status"]["name"]

        self.logger.debug(f"[WORKFLOW] Detected stage: '{current_stage}' (page {page_id[:8]}...)")
        if not current_stage: return



        app_record = self.supa.get_application_by_notion_id(page_id)
        if not app_record: return

        self.logger.debug(f"[WORKFLOW] Stored stage: '{app_record['current_stage']}', incoming: '{current_stage}'")

        if app_record["current_stage"] != current_stage:
            self.logger.info(f"Stage change: {app_record['current_stage']} -> {current_stage}")
            self.supa.register_stage_change(app_record["id"], app_record["current_stage"], current_stage)



    def _handle_feedback_form(self, page, process_context):
        """
        Handler: External Feedback (PDF -> AI -> Note on Candidate).
        Uses Identity Engine to find the candidate and cross-reference with the current process.
        """
        if page["properties"].get("Processed", {}).get("checkbox"): return



        props = page["properties"]
        form_id = page["id"]


        raw_name = props.get("Name", {}).get("title", [])
        interviewer_name = raw_name[0]["plain_text"] if raw_name else "External Headhunter"
        files = props.get("File", {}).get("files", [])
        if not files: return



        for file_obj in files:
            try:
                file_url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")
                if not file_url:
                    self.logger.warning(f"Feedback file '{file_obj.get('name', '?')}' has no URL, skipping")
                    continue

                self.logger.debug(f"[FEEDBACK] Downloading file: '{file_obj['name']}'")
                local_path = download_file(file_url, file_obj["name"], TEMP_FOLDER)
                if not local_path:
                    self.logger.warning(f"Failed to download feedback file '{file_obj['name']}', skipping")
                    continue

                feedback_data = self.ai.process_feedback_pdf(local_path)
                try: os.remove(local_path)
                except OSError: pass

                if not feedback_data:
                    self.logger.warning(f"AI returned no data for '{file_obj['name']}', skipping")
                    continue

                self.logger.debug(f"[FEEDBACK] AI parse succeeded for '{file_obj['name']}'")
                cand_name_ai = feedback_data["candidate_name"]
                self.logger.debug(f"[FEEDBACK] Resolving identity for candidate (name_prefix={cand_name_ai[:2]+'***' if cand_name_ai else None})")
                cand_db, _ = self.supa.resolve_candidate_identity(None, cand_name_ai)

                if not cand_db:
                    self.logger.warning(f"Candidate '{cand_name_ai}' not found for feedback")
                    continue

                res_app = self.supa.client.table("NzymeRecruitingApplications")\
                    .select("notion_page_id, current_stage")\
                    .eq("candidate_id", cand_db["id"])\
                    .eq("process_id", process_context["id"])\
                    .execute()

                if not res_app.data:
                    self.logger.warning(f"No application found for '{cand_name_ai}' in process {process_context['id']}")
                    continue

                self.logger.debug(f"[FEEDBACK] Application found (candidate_id={cand_db['id'][:8]}...)")
                app_data = res_app.data[0]
                target_id = app_data["notion_page_id"]
                current_stage = app_data.get("current_stage")

                # Upload raw feedback file to permanent storage and attach to Workflow page
                permanent_url = self.storage.upload_cv_from_url(file_url, file_obj["name"])
                if permanent_url:
                    # Sanitize display name: keep unicode but strip control chars, limit length
                    raw_name = file_obj.get("name", "Feedback")
                    safe_display_name = "".join(c for c in raw_name if c.isprintable())[:200] or "Feedback"

                    res_upload = self.notion.update_page(target_id, {
                        PROP_HEADHUNTER_FEEDBACK: {"files": [{"name": safe_display_name, "external": {"url": permanent_url}}]}
                    })
                    if res_upload.status_code == 200:
                        self.supa.client.table("NzymeRecruitingApplications").update({
                            "headhunter_feedback_url": permanent_url,
                            "updated_at": "now()"
                        }).eq("candidate_id", cand_db["id"]).eq("process_id", process_context["id"]).execute()
                        self.logger.info(f"Raw feedback file uploaded for '{cand_name_ai}'")
                    else:
                        self.logger.warning(f"Workflow page for '{cand_name_ai}' has no '{PROP_HEADHUNTER_FEEDBACK}' property, skipping file attach")

                gathered_db_id = self.notion.find_child_database(target_id, "Gathered Feedback")
                if not gathered_db_id:
                    self.logger.warning(f"No 'Gathered Feedback' DB found for '{cand_name_ai}'")
                    continue

                payload = {
                    "Interviewer": {"title": [{"text": {"content": f"{interviewer_name} - {current_stage}" if current_stage else interviewer_name}}]},
                }

                res_create = self.notion.create_page(gathered_db_id, payload)

                if res_create.status_code == 200:
                    new_page_id = res_create.json()["id"]
                    blocks = markdown_to_notion_blocks(feedback_data["feedback_markdown"])
                    self.logger.info(f"Feedback: {len(blocks)} blocks generated for '{cand_name_ai}'")

                    CHUNK_SIZE = 100
                    for i in range(0, len(blocks), CHUNK_SIZE):
                        chunk = blocks[i:i + CHUNK_SIZE]
                        self.notion.append_block_children(new_page_id, chunk)

                    self.logger.info(f"Feedback synced for '{cand_name_ai}'")
                else:
                    self.logger.error(f"Error creating note in Notion: {res_create.text}")

            except Exception as e:
                self.logger.error(f"Error processing feedback file '{file_obj.get('name', '?')}': {e}", exc_info=True)

        res_fb_proc = self.notion.update_page(form_id, {"Processed": {"checkbox": True}})
        if res_fb_proc.status_code != 200:
            self.logger.error(f"Feedback form mark-processed FAILED — form={form_id[:8]}..., status={res_fb_proc.status_code}")



    def _handle_central_reference(self, page, _=None):
        """Handler: Central References with strict identity resolution"""
        if page["properties"].get("Processed", {}).get("checkbox"): return


        ref_page_id = page["id"]
        props = page["properties"]



        try:
            cand_email = props.get("Candidate Email", {}).get("email")
            c_name_obj = props.get("Candidate Name", {}).get("rich_text", [])
            cand_name = c_name_obj[0]["plain_text"].strip() if c_name_obj else None


            ref_email = props.get("Referrer Email", {}).get("email")
            ref_phone = props.get("Referrer Phone", {}).get("phone_number")
            r_name_obj = props.get("Referrer Name", {}).get("title", [])
            ref_name = r_name_obj[0]["plain_text"] if r_name_obj else "Unknown"
            ctx_obj = props.get("Context", {}).get("rich_text", [])
            context = ctx_obj[0]["plain_text"] if ctx_obj else ""
            raw_rel = props.get("Relationship to Candidate", {}).get("multi_select", [])
            rel_list = [item["name"] for item in raw_rel]
            raw_timing = props.get("Timing of such relationship", {}).get("select")
            timing_val = raw_timing["name"] if raw_timing else None
            raw_outcome = props.get("Reference Outcome", {}).get("select")
            outcome_val = raw_outcome["name"] if raw_outcome else "To contact"


        except Exception: return



        self.logger.debug(f"[CENTRAL_REF] Resolving identity (name_prefix={cand_name[:2]+'***' if cand_name else None})")
        cand_db, _ = self.supa.resolve_candidate_identity(cand_email, cand_name)


        if not cand_db:
            self.logger.warning(f"Identity not resolved for reference: '{cand_name or cand_email}'")
            return

        self.logger.debug(f"[CENTRAL_REF] Identity resolved: candidate_id={cand_db['id'][:8]}...")

        try:
            apps_res = self.supa.client.table("NzymeRecruitingApplications")\
                .select("notion_page_id")\
                .eq("candidate_id", cand_db["id"])\
                .eq("status", "Active")\
                .execute()
            app_page_ids = [item["notion_page_id"] for item in apps_res.data if item.get("notion_page_id")]
        except Exception as e:
            self.logger.error(f"Error searching apps: {e}")
            return

        self.logger.debug(f"[CENTRAL_REF] {len(app_page_ids)} active application(s) found")
        # Backfill email if candidate was matched by name but had no email
        should_backfill = cand_email and not cand_db.get("email")
        self.logger.debug(f"[CENTRAL_REF] Email backfill needed: {should_backfill}")
        if should_backfill:
            self._backfill_candidate_email(cand_db, cand_email, app_page_ids)



        if not app_page_ids: return



        global_success = True
        for app_pid in app_page_ids:
            child_db_id = self.notion.find_child_database(app_pid, INTERNAL_REFS_DB_TITLE)
            if child_db_id:
                rel_payload = [{"name": r, "color": "default"} for r in rel_list]
                payload = {
                    "Referrer Name": {"title": [{"text": {"content": ref_name}}]},
                    "Referrer Email": {"email": ref_email} if ref_email else None,
                    "Referrer Phone": {"phone_number": ref_phone} if ref_phone else None,
                    "Context": {"rich_text": [{"text": {"content": context}}]},
                    "Relationship to Candidate": {"multi_select": rel_payload} if rel_payload else None,
                    "Timing of such relationship": {"select": {"name": timing_val, "color": "default"}} if timing_val else None,
                    "Reference Outcome": {"select": {"name": "To contact", "color": "default"}}
                }
                payload = {k: v for k, v in payload.items() if v is not None}
                res = self.notion.create_page(child_db_id, payload)
                if res.status_code != 200: global_success = False
            else:
                global_success = False



        if global_success:
            res_ref_proc = self.notion.update_page(ref_page_id, {"Processed": {"checkbox": True}})
            if res_ref_proc.status_code != 200:
                self.logger.error(f"Reference mark-processed FAILED — ref={ref_page_id[:8]}..., status={res_ref_proc.status_code}")
            self.logger.info(f"Reference synced for '{cand_name}'")



    def _handle_outcome_entry(self, page, context):
        """Handler: Outcome Form (Fuzzy Match + Sync Reason)"""
        candidate_id = context.get("candidate_id")
        if not candidate_id: return


        props = page["properties"]
        page_id = page["id"]


        outcome_prop = props.get("Discarded/Disqualified/Lost", {}).get("select")
        outcome_val = outcome_prop["name"] if outcome_prop else None

        explanation_obj = props.get("Explanation", {}).get("rich_text", [])
        explanation_val = explanation_obj[0]["plain_text"] if explanation_obj else "No explanation provided"

        self.logger.debug(f"[OUTCOME] outcome_val='{outcome_val}', candidate_id={candidate_id[:8] if candidate_id else None}...")
        if not outcome_val:
            return


        final_stage_name = self._fuzzy_match_stage(candidate_id, outcome_val)

        self.logger.debug(f"[OUTCOME] Fuzzy match result: '{outcome_val}' -> '{final_stage_name}'")
        self.logger.info(f"Outcome match: '{outcome_val}' -> '{final_stage_name}'")


        payload_cand = {
            "Stage": {"select": {"name": final_stage_name}},
            PROP_NEXT_STEPS: {"multi_select": []}  # Clear next steps on disqualification
        }
        res_upd = self.notion.update_page(candidate_id, payload_cand)

        if res_upd.status_code == 200:
            self.supa.update_rejection_reason(candidate_id, explanation_val, final_stage_name)
            self.notion.update_page(page_id, {"Processed": {"checkbox": True}})
            self.logger.info("Outcome processed successfully")
        else:
            self.logger.error(f"Failed to update candidate: {res_upd.text}")



    # =========================================================================
    # 3. SPECIFIC HELPERS (LOGIC SUPPORT)
    # =========================================================================



    # --- FEEDBACK ASSESSMENT LOGIC ---

    def _handle_feedback_assessment(self, page, process_context):
        """Generates an AI-scored assessment matrix using CV + gathered feedback."""
        page_id = page["id"]
        props = page["properties"]

        # 1. Get candidate name for logging
        raw_name = props.get(PROP_NAME, {}).get("title", [])
        candidate_name = raw_name[0]["plain_text"] if raw_name else "Unknown"

        # 2. Get assessment characteristics from process context
        assessment_chars = process_context.get("assessment_characteristics") if process_context else None
        if not assessment_chars:
            self.logger.info(f"[ASSESSMENT] No assessment characteristics for this process. Skipping '{candidate_name}'.")
            self._uncheck_assessment_requested(page_id)
            return

        # 3. Download CV and extract text
        cv_text = self._get_cv_text(page)

        # 4. Read all Gathered Feedback
        feedback_texts = self._read_gathered_feedback(page_id)

        # 5. Validate: at least CV or feedback must exist
        if not cv_text and not feedback_texts:
            self.logger.warning(f"[ASSESSMENT] No CV and no feedback available for '{candidate_name}'. Skipping.")
            self._uncheck_assessment_requested(page_id)
            return

        self.logger.info(f"[ASSESSMENT] Running for '{candidate_name}' — CV={'yes' if cv_text else 'no'}, feedback={len(feedback_texts)} entries")

        # 6. Call AI
        result = self.ai.process_feedback_assessment(cv_text, feedback_texts, assessment_chars)
        if not result:
            self.logger.error(f"[ASSESSMENT] AI failed for '{candidate_name}'.")
            self._uncheck_assessment_requested(page_id)
            return

        # 7. Find "Gathered Feedback" child DB
        gathered_db_id = self.notion.find_child_database(page_id, "Gathered Feedback")
        if not gathered_db_id:
            self.logger.warning(f"[ASSESSMENT] No 'Gathered Feedback' DB found for '{candidate_name}'.")
            self._uncheck_assessment_requested(page_id)
            return

        # 8. Create assessment page with scored matrix blocks
        process_name = process_context.get("process_name", "Unknown Process")
        payload = {
            "Interviewer": {"title": [{"text": {"content": f"Feedback Assessment [AI-generated]"}}]},
        }
        res_create = self.notion.create_page(gathered_db_id, payload)

        if res_create.status_code == 200:
            new_page_id = res_create.json()["id"]
            blocks = self._build_assessment_blocks(result, process_name)
            self.logger.info(f"[ASSESSMENT] {len(blocks)} blocks generated for '{candidate_name}'")

            CHUNK_SIZE = 100
            for i in range(0, len(blocks), CHUNK_SIZE):
                chunk = blocks[i:i + CHUNK_SIZE]
                self.notion.append_block_children(new_page_id, chunk)

            self.logger.info(f"[ASSESSMENT] Completed for '{candidate_name}'")
        else:
            self.logger.error(f"[ASSESSMENT] Error creating page: {res_create.text}")

        # 9. Uncheck "Assessment Requested"
        self._uncheck_assessment_requested(page_id)

    def _get_cv_text(self, page):
        """Extracts CV text from workflow page's CV file, or falls back to candidate's Main DB CV."""
        props = page["properties"]
        page_id = page["id"]

        # Try workflow page CV first
        cv_files = props.get(PROP_CV_FILES, {}).get("files", [])
        if cv_files:
            file_obj = cv_files[0]
            file_url = file_obj.get("external", {}).get("url") or file_obj.get("file", {}).get("url")
            if file_url:
                file_name = file_obj.get("name", "cv.pdf")
                local_path = download_file(file_url, file_name, TEMP_FOLDER)
                if local_path:
                    text = self.ai._read_file(local_path)
                    try: os.remove(local_path)
                    except OSError: pass
                    if text:
                        return text

        # Fallback: look up candidate via Supabase → Main DB → cv_url
        app_record = self.supa.get_application_by_notion_id(page_id)
        if app_record:
            app_full = self.supa.client.table("NzymeRecruitingApplications") \
                .select("candidate_id") \
                .eq("notion_page_id", page_id) \
                .execute()
            if app_full.data:
                candidate_id = app_full.data[0].get("candidate_id")
                if candidate_id:
                    cand = self.supa.client.table("NzymeTalentNetwork") \
                        .select("cv_url") \
                        .eq("id", candidate_id) \
                        .execute()
                    if cand.data and cand.data[0].get("cv_url"):
                        cv_url = cand.data[0]["cv_url"]
                        local_path = download_file(cv_url, "candidate_cv.pdf", TEMP_FOLDER)
                        if local_path:
                            text = self.ai._read_file(local_path)
                            try: os.remove(local_path)
                            except OSError: pass
                            if text:
                                return text

        return None

    def _read_gathered_feedback(self, workflow_page_id):
        """Reads all pages from the 'Gathered Feedback' child DB, excluding AI-generated assessments.
        Returns list of {"title": str, "content": str}."""
        gathered_db_id = self.notion.find_child_database(workflow_page_id, "Gathered Feedback")
        if not gathered_db_id:
            return []

        ds_id = self.notion.get_data_source_id(gathered_db_id) or gathered_db_id
        pages = self.notion.query_data_source(ds_id, filter_params=None)

        results = []
        for page in pages:
            title_list = page.get("properties", {}).get("Interviewer", {}).get("title", [])
            title = title_list[0]["plain_text"] if title_list else "Unknown"

            # Skip previously generated AI assessments
            if "AI-generated" in title:
                continue

            blocks = self.notion.get_page_blocks(page["id"])
            content = self._blocks_to_plain_text(blocks)
            if content.strip():
                results.append({"title": title, "content": content})

        return results

    def _blocks_to_plain_text(self, blocks):
        """Extracts plain text from Notion blocks recursively."""
        lines = []
        for block in blocks:
            block_type = block.get("type", "")
            block_data = block.get(block_type, {})

            # Extract rich_text from common block types
            rich_text = block_data.get("rich_text", [])
            if rich_text:
                text = "".join(rt.get("plain_text", "") for rt in rich_text)
                if text.strip():
                    lines.append(text)

            # Handle nested children
            if block.get("has_children"):
                children = self.notion.get_page_blocks(block["id"])
                child_text = self._blocks_to_plain_text(children)
                if child_text.strip():
                    lines.append(child_text)

        return "\n".join(lines)

    def _build_assessment_blocks(self, result, process_name):
        """Builds Notion blocks for the feedback assessment output page."""
        blocks = []

        # Header
        blocks.append({
            "object": "block", "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "text": {"content": f"Feedback Assessment — {process_name}"}}]}
        })

        # Overall summary
        summary = result.get("overall_summary", "")
        if summary:
            blocks.append({
                "object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "text": {"content": summary}}]}
            })

        blocks.append({"object": "block", "type": "divider", "divider": {}})

        # Per-characteristic sections
        for item in result.get("assessment", []):
            char_name = item.get("characteristic", "Unknown")
            score = item.get("score", "No")

            # Heading with characteristic name and score
            blocks.append({
                "object": "block", "type": "heading_3",
                "heading_3": {"rich_text": [
                    {"type": "text", "text": {"content": f"{char_name} — "},
                     "annotations": {"bold": True}},
                    {"type": "text", "text": {"content": score}}
                ]}
            })

            # CV evidence
            cv_evidence = item.get("cv_evidence", "No CV available")
            blocks.append({
                "object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [
                    {"type": "text", "text": {"content": "CV: "}, "annotations": {"bold": True}},
                    {"type": "text", "text": {"content": cv_evidence}}
                ]}
            })

            # Feedback evidence
            fb_evidence = item.get("feedback_evidence", "No feedback available")
            blocks.append({
                "object": "block", "type": "paragraph",
                "paragraph": {"rich_text": [
                    {"type": "text", "text": {"content": "Feedback: "}, "annotations": {"bold": True}},
                    {"type": "text", "text": {"content": fb_evidence}}
                ]}
            })

        return blocks

    def _uncheck_assessment_requested(self, page_id):
        """Unchecks the 'Assessment Requested' checkbox on a workflow page."""
        self.notion.update_page(page_id, {PROP_ASSESSMENT_REQUESTED: {"checkbox": False}})

    # --- END FEEDBACK ASSESSMENT LOGIC ---

    def _logic_reprocess_ai_pending(self, page):
        """Reprocess AI Pending candidate when CV or LinkedIn is added to Main DB.
        Updates Main DB + Supabase with parsed data, then fills strategic
        assessment matrices on all active workflow pages."""
        page_id = page["id"]
        props = page["properties"]

        candidate_name_raw = props.get(PROP_NAME, {}).get("title", [])
        candidate_name = candidate_name_raw[0]["plain_text"] if candidate_name_raw else "Unknown"

        self.logger.info(f"[AI-PENDING] Reprocessing: {candidate_name}")

        # 1. Get candidate from Supabase
        candidate = self.supa.get_candidate_by_notion_page_id(page_id)
        if not candidate:
            self.logger.warning(f"[AI-PENDING] Candidate not found in Supabase: {candidate_name}")
            return

        cand_json = candidate.get("candidate_data") or {}

        # 2. Get active applications (for matrix + workflow pages)
        applications = self.supa.get_applications_by_candidate_id(candidate["id"])

        # Find first available matrix_characteristics for AI parsing
        matrix_chars = None
        if applications:
            for app in applications:
                mc = app.get("matrix_characteristics")
                if mc:
                    matrix_chars = mc
                    break

        # 3. Parse CV or LinkedIn
        ai_data = None
        public_url = None
        cv_files = props.get(PROP_CV_FILES, {}).get("files", [])
        linkedin_url = props.get(PROP_LINKEDIN, {}).get("url")

        if cv_files:
            first_file = cv_files[0]
            file_url = first_file.get("external", {}).get("url") or first_file.get("file", {}).get("url")
            file_name = first_file.get("name", "cv.pdf")

            if file_url:
                local_path = download_file(file_url, file_name, TEMP_FOLDER)
                if local_path:
                    # Upload to permanent storage if Notion-hosted
                    if first_file.get("type") == "file":
                        public_url = self.storage.upload_cv_from_url(file_url, file_name)
                    else:
                        public_url = file_url

                    ai_data = self.ai.process_cv(local_path, matrix_characteristics=matrix_chars)
                    try: os.remove(local_path)
                    except OSError: pass

        if not ai_data and linkedin_url and self.exa:
            linkedin_text = self.exa.get_linkedin_profile(linkedin_url)
            if linkedin_text:
                ai_data = self.ai.process_linkedin(linkedin_text, matrix_characteristics=matrix_chars)
                if ai_data:
                    ai_data["linkedin_url"] = linkedin_url
                    if not ai_data.get("email"):
                        ai_data["email"] = props.get(PROP_EMAIL, {}).get("email")
                    if not ai_data.get("phone"):
                        ai_data["phone"] = props.get(PROP_PHONE, {}).get("phone_number")

        if not ai_data:
            self.logger.warning(f"[AI-PENDING] Could not parse CV or LinkedIn for {candidate_name}")
            return

        # 4. Update Notion Main DB
        exp = ai_data.get("experience", {})
        edu = ai_data.get("education", {})
        gen = ai_data.get("general", {})

        notion_props = {}

        total_range = DomainMapper.get_years_range_tag(ai_data.get("total_years", 0))
        if total_range:
            notion_props[PROP_EXP_TOTAL_YEARS] = {"select": {"name": total_range, "color": "default"}}

        sector_mapping = {
            PROP_EXP_CONSULTING: exp.get("consulting"),
            PROP_EXP_AUDIT: exp.get("audit"),
            PROP_EXP_IB: exp.get("ib"),
            PROP_EXP_PE: exp.get("pe"),
            PROP_EXP_VC: exp.get("vc"),
            PROP_EXP_ENGINEER: exp.get("engineer_role"),
            PROP_EXP_LAWYER: exp.get("lawyer"),
            PROP_EXP_FOUNDER: exp.get("founder"),
            PROP_EXP_CORP_MA: exp.get("corp_ma"),
            PROP_EXP_PORTCO: exp.get("portco_roles"),
        }
        for prop_name, data in sector_mapping.items():
            notion_props[prop_name] = {"multi_select": NotionBuilder._create_experience_tags(data)}

        functional_mapping = {
            PROP_EXP_MANAGEMENT: exp.get("management"),
            PROP_EXP_FINANCE: exp.get("finance"),
            PROP_EXP_MARKETING: exp.get("marketing"),
            PROP_EXP_OPERATIONS: exp.get("operations"),
            PROP_EXP_PRODUCT: exp.get("product"),
            PROP_EXP_SALES_REVENUE: exp.get("sales_revenue"),
            PROP_EXP_TECHNOLOGY: exp.get("technology"),
        }
        for prop_name, data in functional_mapping.items():
            notion_props[prop_name] = {"multi_select": NotionBuilder._create_functional_tags(data)}

        if gen.get("international_locations"):
            notion_props[PROP_EXP_INTERNATIONAL] = {"multi_select": NotionBuilder._format_multi_select(gen["international_locations"])}
        if gen.get("industries_specialized"):
            notion_props[PROP_EXP_INDUSTRIES] = {"multi_select": NotionBuilder._format_multi_select(gen["industries_specialized"])}

        if ai_data.get("languages"):
            notion_props[PROP_LANGUAGES] = {"multi_select": NotionBuilder._format_multi_select(ai_data["languages"])}

        if edu.get("bachelors"):
            notion_props[PROP_EDU_BACHELORS] = {"multi_select": NotionBuilder._format_multi_select(edu["bachelors"])}
        if edu.get("masters"):
            notion_props[PROP_EDU_MASTERS] = {"multi_select": NotionBuilder._format_multi_select(edu["masters"])}
        if edu.get("university"):
            notion_props[PROP_EDU_UNIVERSITIES] = {"multi_select": NotionBuilder._format_multi_select(edu["university"])}
        mba_val = edu.get("mba")
        if isinstance(mba_val, str) and mba_val != "No":
            notion_props[PROP_EDU_MBAS] = {"multi_select": NotionBuilder._format_multi_select([mba_val])}

        if public_url:
            cv_display_name = f"CV - {ai_data.get('name', candidate_name)}"
            notion_props[PROP_CV_FILES] = {"files": [{"name": cv_display_name, "external": {"url": public_url}}]}

        # Uncheck AI Pending
        notion_props[PROP_AI_PENDING] = {"checkbox": False}

        res = self.notion.update_page(page_id, notion_props)
        if res.status_code != 200:
            self.logger.error(f"[AI-PENDING] Error updating Main DB: {res.status_code}")
            return

        # 5. Update Supabase
        raw_exp = ai_data.get("experience", {})
        updated_json = dict(cand_json)

        updated_json["total_years_range"] = DomainMapper.get_years_range_tag(ai_data.get("total_years", 0))
        updated_json["languages"] = ai_data.get("languages", [])
        updated_json["general"] = {
            "international_locations": gen.get("international_locations", []),
            "industries_specialized": gen.get("industries_specialized", []),
        }
        updated_json["education"] = {
            "bachelors": edu.get("bachelors", []),
            "masters": edu.get("masters", []),
            "university": edu.get("university", []),
            "mba": [edu.get("mba")] if edu.get("mba") and edu.get("mba") != "No" else [],
        }
        updated_json["experience"] = {
            "consulting": DomainMapper._format_experience(raw_exp.get("consulting")),
            "audit": DomainMapper._format_experience(raw_exp.get("audit")),
            "ib": DomainMapper._format_experience(raw_exp.get("ib")),
            "pe": DomainMapper._format_experience(raw_exp.get("pe")),
            "vc": DomainMapper._format_experience(raw_exp.get("vc")),
            "engineer_role": DomainMapper._format_experience(raw_exp.get("engineer_role")),
            "lawyer": DomainMapper._format_experience(raw_exp.get("lawyer")),
            "founder": DomainMapper._format_experience(raw_exp.get("founder")),
            "management": DomainMapper._format_experience(raw_exp.get("management")),
            "corp_ma": DomainMapper._format_experience(raw_exp.get("corp_ma")),
            "portco_roles": DomainMapper._format_experience(raw_exp.get("portco_roles")),
            "finance": DomainMapper._format_experience(raw_exp.get("finance")),
            "marketing": DomainMapper._format_experience(raw_exp.get("marketing")),
            "operations": DomainMapper._format_experience(raw_exp.get("operations")),
            "product": DomainMapper._format_experience(raw_exp.get("product")),
            "sales_revenue": DomainMapper._format_experience(raw_exp.get("sales_revenue")),
            "technology": DomainMapper._format_experience(raw_exp.get("technology")),
        }

        updated_json.pop("ai_pending", None)
        updated_json.pop("ai_pending_cv_url", None)
        updated_json.pop("ai_pending_process_name", None)

        supa_update = {"candidate_data": updated_json, "updated_at": "now()"}
        if public_url:
            supa_update["cv_url"] = public_url

        try:
            self.supa.client.table("NzymeTalentNetwork").update(
                supa_update
            ).eq("id", candidate["id"]).execute()
        except Exception as e:
            self.logger.error(f"[AI-PENDING] Error updating Supabase: {e}")

        # 6. Fill strategic assessment for all active workflow pages
        if ai_data.get("strategic_assessment") and applications:
            for app in applications:
                workflow_page_id = app.get("notion_page_id")
                if workflow_page_id:
                    self._fill_strategic_assessment(workflow_page_id, ai_data["strategic_assessment"])

        self.logger.info(f"[AI-PENDING] Successfully reprocessed: {candidate_name}")

    def _fill_strategic_assessment(self, workflow_page_id, assessment_list):
        """Finds 'Past Experience [AI-generated]' table in a workflow page and fills AI scores."""
        if not assessment_list:
            return

        time.sleep(4)

        db_title = "Past Experience [AI-generated]"
        child_db_id = self.notion.find_child_database(workflow_page_id, db_title)
        if not child_db_id:
            self.logger.warning(f"DB '{db_title}' not found in workflow page {workflow_page_id[:8]}...")
            return

        ds_child = self.notion.get_data_source_id(child_db_id) or child_db_id
        rows = self.notion.query_data_source(ds_child, filter_params=None)

        row_map = {}
        for r in rows:
            title_list = r["properties"].get("Characteristic", {}).get("title", [])
            if title_list:
                name = title_list[0]["plain_text"].strip()
                row_map[name] = r["id"]

        updates_count = 0
        for item in assessment_list:
            char_name = item.get("characteristic", "").strip()
            score_val = item.get("score")
            comment_val = item.get("comment", "")

            target_id = row_map.get(char_name)
            if target_id:
                payload = {
                    "AI Score": {"select": {"name": score_val}},
                    "AI Comments": {"rich_text": [{"text": {"content": comment_val}}]}
                }
                res = self.notion.update_page(target_id, payload)
                if res.status_code == 200:
                    updates_count += 1
                else:
                    self.logger.error(f"Error writing '{char_name}': {res.status_code}")

        self.logger.info(f"Assessment completed: {updates_count}/{len(assessment_list)} rows")

    def _logic_enrich_cv(self, page):
        """CV enrichment logic"""
        props = page["properties"]
        cv_files = props.get("CV", {}).get("files", [])
        if not cv_files: return False



        file_obj = cv_files[0]
        if file_obj.get("type") != "file": return False



        name = file_obj.get("name", "cv.pdf")
        url = file_obj.get("file", {}).get("url")


        self.logger.info(f"New CV detected: {name}")
        local_path = download_file(url, name, TEMP_FOLDER)
        if not local_path: return False



        public_url = self.storage.upload_cv_from_url(url, name)
        if not public_url: return False



        ai_data = self.ai.process_cv(local_path)
        try: os.remove(local_path)
        except OSError: pass


        if not ai_data: return False



        curr_hist = [t["name"] for t in props.get(PROP_PROCESS_HISTORY, {}).get("multi_select", [])]
        curr_role = [t["name"] for t in props.get(PROP_TEAM_ROLE, {}).get("multi_select", [])]
        curr_proc_obj = props.get("Last Process Involved in", {}).get("select")
        curr_proc = curr_proc_obj["name"] if curr_proc_obj else "Referral/General"



        payload = NotionBuilder.build_candidate_payload(
            ai_data, public_url, curr_proc,
            existing_history=curr_hist, existing_team_role=curr_role
        )

        payload[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
        self.notion.update_page(page["id"], payload)
        self.logger.info("Enrichment completed")
        return True

    def _logic_enrich_linkedin(self, page):
        """Enrich candidate from LinkedIn profile when no CV is available."""
        props = page["properties"]
        linkedin_url = props.get(PROP_LINKEDIN, {}).get("url")
        if not linkedin_url:
            return False

        if not self.exa:
            return False

        self.logger.info(f"Trying LinkedIn enrichment for: {linkedin_url}")

        try:
            linkedin_text = self.exa.get_linkedin_profile(linkedin_url)
            if not linkedin_text:
                return False

            ai_data = self.ai.process_linkedin(linkedin_text)
            if not ai_data:
                return False

            # Preserve existing page values the AI won't have
            ai_data["linkedin_url"] = linkedin_url
            if not ai_data.get("email"):
                ai_data["email"] = props.get(PROP_EMAIL, {}).get("email")
            if not ai_data.get("phone"):
                ai_data["phone"] = props.get(PROP_PHONE, {}).get("phone_number")

            # Read existing process history and team role
            curr_hist = [t["name"] for t in props.get(PROP_PROCESS_HISTORY, {}).get("multi_select", [])]
            curr_role = [t["name"] for t in props.get(PROP_TEAM_ROLE, {}).get("multi_select", [])]
            curr_proc_obj = props.get("Last Process Involved in", {}).get("select")
            curr_proc = curr_proc_obj["name"] if curr_proc_obj else "Referral/General"

            # Build and apply Notion payload (no CV file URL)
            payload = NotionBuilder.build_candidate_payload(
                ai_data, None, curr_proc,
                existing_history=curr_hist, existing_team_role=curr_role
            )
            # Don't overwrite these — referral form already sets them
            payload.pop(PROP_PROCESS_HISTORY, None)
            payload.pop(PROP_TEAM_ROLE, None)
            payload[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
            self.notion.update_page(page["id"], payload)

            # Sync to Supabase
            supa_data = DomainMapper.map_to_supabase_candidate(ai_data, None)
            self.supa.manage_candidate(supa_data, page["id"])

            self.logger.info("LinkedIn enrichment completed")
            return True

        except Exception as e:
            self.logger.error(f"LinkedIn enrichment failed: {e}", exc_info=True)
            return False

    def _logic_dispatch_candidate_to_form(self, candidate_page, process_dashboard_page_id):
        """
        Moves a candidate from Main DB to the destination Form.
        Uses resolved ID for Schema, but raw ID for Creation.
        """
        cand_id = candidate_page["id"]
        props = candidate_page["properties"]

        self.logger.info(f"[DISPATCH] --- START DISPATCH (Candidate ID: {cand_id}) ---")

        # --- A. EXTRACTION (Standard) ---
        raw_name = props.get(PROP_NAME, {}).get("title", [])
        val_name = raw_name[0]["plain_text"] if raw_name else "Unknown Candidate"

        val_email = props.get(PROP_EMAIL, {}).get("email")
        val_phone = props.get(PROP_PHONE, {}).get("phone_number")
        val_linkedin = props.get(PROP_LINKEDIN, {}).get("url")

        val_cv_url = None
        val_cv_name = "attached_cv.pdf"
        cv_files = props.get(PROP_CV_FILES, {}).get("files", [])

        if cv_files:
            file_data = cv_files[0]
            val_cv_name = file_data.get("name", "attached_cv.pdf")
            val_cv_url = file_data.get("external", {}).get("url") or file_data.get("file", {}).get("url")

        if not val_cv_url:
            self.logger.info(f"[DISPATCH] Candidate '{val_name}' has no CV. Sending without CV.")

        # --- B. DESTINATION RESOLUTION ---
        try:
            # 1. Get process name
            url_proc = f"https://api.notion.com/v1/pages/{process_dashboard_page_id}"
            resp_proc = self._api_request("GET", url_proc)

            if not resp_proc or resp_proc.status_code != 200:
                self.logger.error(f"[DISPATCH] Dashboard error: {resp_proc.status_code if resp_proc else 'No response'}")
                return

            proc_title_obj = resp_proc.json()["properties"].get("Name", {}).get("title", [])
            process_name = proc_title_obj[0]["plain_text"] if proc_title_obj else None

            if not process_name:
                self.logger.error("[DISPATCH] Process has no name.")
                return

            self.logger.info(f"[DISPATCH] Destination: '{process_name}'")

            # 2. Get raw ID from Supabase
            proc_record = self.supa.get_process_by_name(process_name)
            if not proc_record:
                self.logger.error(f"[DISPATCH] Process not in Supabase.")
                self.notion.update_page(cand_id, {"Assign to Active Process": {"relation": []}})
                return

            raw_form_id = proc_record.get("notion_form_id")
            if not raw_form_id:
                self.logger.error(f"[DISPATCH] notion_form_id is NULL.")
                return

            # --- C. DYNAMIC SCHEMA (Use resolved ID for reading) ---

            target_ds_id_for_schema = self.notion.get_data_source_id(raw_form_id) or raw_form_id

            schema = self.notion.get_database_schema(target_ds_id_for_schema)
            valid_cols = set(schema.keys()) if schema else set()

            self.logger.info(f"[DISPATCH] Schema check OK. Columns: {list(valid_cols)}")

            # --- D. PAYLOAD CONSTRUCTION ---
            payload = {}

            # Title
            target_title_col = PROP_NAME
            if PROP_NAME not in valid_cols:
                # Fallback: find title-type column
                target_title_col = next((k for k, v in schema.items() if v["type"] == "title"), None)

            if target_title_col:
                payload[target_title_col] = {"title": [{"text": {"content": val_name}}]}
            else:
                self.logger.error("[DISPATCH] Destination has no Title column.")
                return

            # Conditional fields (checking valid_cols)
            if val_email and PROP_EMAIL in valid_cols:
                payload[PROP_EMAIL] = {"email": val_email}

            if val_phone and PROP_PHONE in valid_cols:
                payload[PROP_PHONE] = {"phone_number": val_phone}

            if val_linkedin and PROP_LINKEDIN in valid_cols:
                payload[PROP_LINKEDIN] = {"url": val_linkedin}

            if val_cv_url and PROP_CV_FILES in valid_cols:
                payload[PROP_CV_FILES] = {
                    "files": [{
                        "name": val_cv_name,
                        "type": "external",
                        "external": {"url": val_cv_url}
                    }]
                }

            # --- E. INSERTION (Use raw ID) ---

            res = self.notion.create_page(raw_form_id, payload)

            if res.status_code == 200:
                self.logger.info(f"[DISPATCH] Success. Candidate sent.")
                self.notion.update_page(cand_id, {"Assign to Active Process": {"relation": []}})
            else:
                self.logger.error(f"[DISPATCH] create_page failed: {res.status_code} - {res.text}")

        except Exception as e:
            self.logger.error(f"[DISPATCH] Exception: {e}", exc_info=True)

    def sync_process_status(self, page):
        """Helper for dashboard status sync"""
        props = page["properties"]
        try:
            name_prop = props.get("Name", {}).get("title", [])
            if not name_prop: return
            name = name_prop[0]["plain_text"]

            status_prop = props.get("Open/Closed") or props.get("Status")
            new_status = None
            if status_prop:
                if status_prop.get("select"): new_status = status_prop["select"]["name"]
                elif status_prop.get("status"): new_status = status_prop["status"]["name"]

            if new_status:
                self.supa.update_process_status_by_name(name, new_status)
        except Exception: pass

    def _find_candidate_ancestor(self, starting_id, starting_type):
        """Helper to walk up the Notion hierarchy to find the candidate page (has 'Stage' property)."""
        curr_id, curr_type = starting_id, starting_type

        for i in range(8):
            if not curr_id: return None
            self.logger.debug(f"[ANCESTOR] Walk iteration {i}: type={curr_type}, id={curr_id[:8] if curr_id else None}...")

            if curr_type in ["database_id", "block_id"]:
                endpoint = "databases" if curr_type == "database_id" else "blocks"
                url = f"https://api.notion.com/v1/{endpoint}/{curr_id}"
                resp = self._api_request("GET", url)
                if not resp or resp.status_code != 200:
                    self.logger.warning(f"[ANCESTOR] Failed to fetch {endpoint}/{curr_id} (status={resp.status_code if resp else 'None'})")
                    return None
                parent = resp.json().get("parent")
                if not parent: return None
                curr_type = parent["type"]
                curr_id = parent.get(curr_type)

            elif curr_type == "page_id":
                url = f"https://api.notion.com/v1/pages/{curr_id}"
                resp = self._api_request("GET", url)
                if not resp or resp.status_code != 200:
                    self.logger.warning(f"[ANCESTOR] Failed to fetch page/{curr_id} (status={resp.status_code if resp else 'None'})")
                    return None
                props = resp.json().get("properties", {})
                if "Stage" in props: return curr_id

                parent = resp.json().get("parent")
                if not parent: return None
                curr_type = parent["type"]
                curr_id = parent.get(curr_type)
        return None

    def _fuzzy_match_stage(self, candidate_page_id, partial_text):
        """Helper for Outcome: Finds the real stage in the parent schema"""
        try:
            url_cand = f"https://api.notion.com/v1/pages/{candidate_page_id}"
            resp_cand = self._api_request("GET", url_cand)
            if not resp_cand or resp_cand.status_code != 200:
                return partial_text
            parent_db_id = resp_cand.json()["parent"]["database_id"]

            ds_id = self.notion.get_data_source_id(parent_db_id) or parent_db_id
            schema = self.notion.get_database_schema(ds_id)

            options = []
            if "select" in schema.get("Stage", {}): options = schema["Stage"]["select"]["options"]
            elif "status" in schema.get("Stage", {}): options = schema["Stage"]["status"]["options"]

            for opt in options:
                if partial_text in opt["name"]: return opt["name"]
        except Exception: pass
        return partial_text

    def _backfill_candidate_email(self, cand_db: dict, new_email: str, app_page_ids: list):
        """
        Backfills email when candidate was matched by name but had no email on file.
        Updates: Supabase, Notion Main DB, all active workflow pages.
        """
        candidate_id = cand_db["id"]
        main_notion_id = cand_db.get("notion_page_id")

        # 1. Update Supabase
        self.supa.update_candidate_email(candidate_id, new_email)

        # 2. Update Notion Main DB
        if main_notion_id:
            try:
                self.notion.update_page(main_notion_id, {
                    PROP_EMAIL: {"email": new_email}
                })
            except Exception as e:
                self.logger.error(f"Error updating Main DB email: {e}")

        # 3. Update all active workflow pages
        for app_pid in app_page_ids:
            try:
                self.notion.update_page(app_pid, {
                    PROP_EMAIL: {"email": new_email}
                })
            except Exception as e:
                self.logger.error(f"Error updating workflow page email: {e}")

        self.logger.info(f"Email backfilled for '{cand_db.get('name')}': {new_email}")
    # =========================================================================
    # 4. WEBHOOK ENTRY POINT
    # =========================================================================
    def handle_webhook_event(self, handler_name, page_id, process_context=None):
        """
        Entry point for webhook-triggered events.
        Fetches the full page and dispatches to the appropriate handler.
        """
        from core.constants import (
            HANDLER_MAIN_CANDIDATE, HANDLER_PROCESS_DASHBOARD,
            HANDLER_CENTRAL_REFERENCE, HANDLER_WORKFLOW_ITEM,
            HANDLER_FEEDBACK_FORM, HANDLER_OUTCOME_FORM,
        )

        self.logger.debug(f"[WEBHOOK] Dispatching handler='{handler_name}', page={page_id[:8]}...")
        page = self.notion.get_page(page_id)
        if not page:
            self.logger.error(f"[WEBHOOK] Could not fetch page {page_id}")
            return

        handler_map = {
            HANDLER_MAIN_CANDIDATE: self._handle_main_candidate,
            HANDLER_PROCESS_DASHBOARD: self._handle_process_dashboard,
            HANDLER_CENTRAL_REFERENCE: self._handle_central_reference,
            HANDLER_WORKFLOW_ITEM: self._handle_workflow_item,
            HANDLER_FEEDBACK_FORM: self._handle_feedback_form,
            HANDLER_OUTCOME_FORM: self._handle_outcome_entry,
        }

        handler = handler_map.get(handler_name)
        if handler:
            self.logger.debug(f"[WEBHOOK] Handler resolved: '{handler_name}', executing")
            try:
                handler(page, process_context)
            except Exception as e:
                self.logger.error(f"[WEBHOOK] Error in {handler_name}: {e}", exc_info=True)
        else:
            self.logger.warning(f"[WEBHOOK] Unknown handler: {handler_name}")

    # =========================================================================
    # 5. MAIN EXECUTION (ORCHESTRATOR)
    # =========================================================================
    def run_once(self):
        self.logger.info("Observer starting")
        active_processes = self.supa.get_active_processes() or []
        self.logger.info(f"Context: {len(active_processes)} active processes")
        self.logger.debug(f"run_once: {len(active_processes)} active process(es); main_ds={self.main_ds_id[:8] if self.main_ds_id else None}...")
        self._engine_sniper(self.main_ds_id, self._handle_main_candidate, label="MAIN DB")
        self._engine_sniper(self.dashboard_ds_id, self._handle_process_dashboard, label="DASHBOARD")
        self._engine_sniper(self.refs_ds_id, self._handle_central_reference, label="CENTRAL REFS")
        for proc in active_processes:
            wf_raw_id = proc["notion_workflow_id"]
            if wf_raw_id:
                wf_final_id = self.notion.get_data_source_id(wf_raw_id) or wf_raw_id
                self.logger.debug(f"run_once: process '{proc['process_name']}' wf_ds={wf_final_id[:8]}...")
                self._engine_sniper(
                    wf_final_id,
                    self._handle_workflow_item,
                    context=proc,
                    label=f"WF: {proc['process_name']}"
                )
            fb_raw_id = proc.get("notion_feedback_id")
            if fb_raw_id:
                fb_final_id = self.notion.get_data_source_id(fb_raw_id) or fb_raw_id
                self._engine_sniper(
                    fb_final_id,
                    self._handle_feedback_form,
                    context=proc,
                    label="FEEDBACK FORM"
                )
        self._engine_radar("Process Outcome Form", self._handle_outcome_entry, label="OUTCOME")
        self.logger.info("Execution completed")

if __name__ == "__main__":
    n_client = NotionClient()
    s_client = SupabaseManager()
    st_client = StorageClient()
    ai_agent = CVAnalyzer()

    exa = None
    try:
        from core.exa_client import ExaClient
        exa = ExaClient()
    except (ValueError, ImportError) as e:
        print(f"ExaClient not available: {e}. LinkedIn enrichment disabled.")

    obs = Observer(n_client, s_client, st_client, ai_agent, exa_client=exa)
    obs.run_once()
