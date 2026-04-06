# factory_worker.py
import sys
import os
import time
import httpx
from dotenv import load_dotenv

# Path adjustment for local/Lambda execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.guidelines_parser import GuidelinesParser
from core.constants import PROP_READY_TO_PROCESS, PROP_PROCESSED_DASHBOARD, PROP_NAME, PROP_PROCESS_TYPE
from core.logger import get_logger

load_dotenv()

# --- CONFIGURATION ---
PROCESS_DASHBOARD_DB_ID = os.getenv("NOTION_PROCESS_DASHBOARD_DB_ID")
MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
POLLING_INTERVAL = 10

class FactoryWorkerV2:
    def __init__(self, notion_client: NotionClient, supa_client: SupabaseManager, parser: GuidelinesParser):
        self.logger = get_logger("FactoryWorker")
        self.notion = notion_client
        self.supa = supa_client
        self.parser = parser
        self.dashboard_ds_id = None

    def _init_datasources(self):
        if not self.dashboard_ds_id and PROCESS_DASHBOARD_DB_ID:
            self.dashboard_ds_id = self.notion.get_data_source_id(PROCESS_DASHBOARD_DB_ID) or PROCESS_DASHBOARD_DB_ID

    def _get_default_template_id(self, workflow_db_id):
        """Get the default template ID for a workflow database."""
        ds_id = self.notion.get_data_source_id(workflow_db_id)
        if not ds_id:
            return None

        url = f"{self.notion.base_url}/data_sources/{ds_id}/templates"
        response = self.notion.client.get(url)

        if response.status_code != 200:
            self.logger.warning(f"Templates endpoint returned {response.status_code}")
            return None

        templates = response.json().get("templates", [])

        # Find default template, or use first one
        for t in templates:
            if t.get("is_default"):
                return t.get("id")

        return templates[0].get("id") if templates else None

    def _extract_matrix_from_template(self, workflow_db_id, max_retries=3):
        """Extract matrix characteristics from the workflow's default template.
        Retries to handle Notion async template creation delays."""
        for attempt in range(max_retries):
            self.logger.debug(f"Matrix extraction attempt {attempt + 1}/{max_retries} for workflow {workflow_db_id[:8]}...")
            template_id = self._get_default_template_id(workflow_db_id)
            if not template_id:
                if attempt < max_retries - 1:
                    self.logger.info(f"No template found yet, retrying in 10s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(10)
                    continue
                self.logger.info("No template found for matrix extraction after retries")
                return None

            self.logger.debug(f"Template found: {template_id[:8]}...")
            # Find Past Experience database inside the template
            matrix_db_id = self.notion.find_child_database(template_id, "Past Experience")
            if not matrix_db_id:
                if attempt < max_retries - 1:
                    self.logger.info(f"No 'Past Experience' DB found yet, retrying in 10s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(10)
                    continue
                self.logger.info("No 'Past Experience' database found in template after retries")
                return None

            self.logger.debug(f"'Past Experience' DB found: {matrix_db_id[:8]}...")

            # Extract characteristics AND definitions
            ds_id = self.notion.get_data_source_id(matrix_db_id) or matrix_db_id
            rows = self.notion.query_data_source(ds_id, filter_params=None)

            matrix_data = []
            for row in rows:
                props = row.get("properties", {})

                # Get Characteristic (title field)
                char_title = props.get("Characteristic", {}).get("title", [])
                characteristic = char_title[0]["plain_text"].strip() if char_title else ""

                # Get Definition (rich_text field)
                def_text = props.get("Definition", {}).get("rich_text", [])
                definition = def_text[0]["plain_text"].strip() if def_text else ""

                if characteristic:
                    matrix_data.append({
                        "characteristic": characteristic,
                        "definition": definition
                    })

            if matrix_data:
                self.logger.debug(f"Matrix: {len(matrix_data)} item(s) extracted")
                self.logger.info(f"Extracted {len(matrix_data)} matrix characteristics from template")
                return matrix_data

            # Rows empty — template exists but content not populated yet
            if attempt < max_retries - 1:
                self.logger.info(f"Template found but no rows yet, retrying in 10s (attempt {attempt + 1}/{max_retries})")
                time.sleep(10)

        return None

    def find_pending_requests(self):
        """Searches for pages created by the button (Ready=True, Processed=False)."""
        if not self.dashboard_ds_id:
            return []

        filter_params = {
            "and": [
                {"property": PROP_READY_TO_PROCESS, "checkbox": {"equals": True}},
                {"property": PROP_PROCESSED_DASHBOARD, "checkbox": {"equals": False}}
            ]
        }
        return self.notion.query_data_source(self.dashboard_ds_id, filter_params)

    def configure_process(self, parent_page):
        page_id = parent_page["id"]
        props = parent_page["properties"]

        # 1. Extract Data from Dashboard using CONSTANTS
        try:
            raw_title = props.get(PROP_NAME, {}).get("title", [])
            if not raw_title:
                return
            process_name = raw_title[0]["plain_text"]

            raw_select = props.get(PROP_PROCESS_TYPE, {}).get("select")
            if not raw_select:
                return
            process_type = raw_select["name"]

            is_portco = "PortCo" in process_type

            self.logger.debug(f"configure_process: name='{process_name}', type='{process_type}', is_portco={is_portco}")
            self.logger.info(f"Configuring: {process_name} ({process_type})")
        except Exception as e:
            self.logger.error(f"Error extracting data: {e}", exc_info=True)
            return

        # 1b. Concurrency guard: skip if process already registered
        existing = self.supa.get_process_by_name(process_name)
        if existing:
            self.logger.info(f"Process '{process_name}' already in Supabase. Marking processed.")
            self.notion.update_page(page_id, {PROP_PROCESSED_DASHBOARD: {"checkbox": True}})
            return

        # 2. Safety wait + 3. Identify child DBs (with retry for template propagation)
        wf_db_id = None
        form_db_id = None
        bulk_db_id = None
        feedback_db_id = None
        jd_page_id = None
        interview_stages_page_id = None

        for attempt in range(4):
            if attempt == 0:
                time.sleep(8)
            else:
                self.logger.info(f"Child DBs not found yet, retrying in 10s (attempt {attempt + 1}/4)")
                time.sleep(10)

            blocks = self.notion.get_page_blocks(page_id)

            for b in blocks:
                if b["type"] == "child_database":
                    title = b.get("child_database", {}).get("title", "").lower()
                    bid = b["id"]

                    if "workflow" in title:
                        wf_db_id = bid

                    elif "feedback" in title:
                        feedback_db_id = bid

                    elif "form" in title:
                        form_db_id = bid

                    elif "bulk" in title or "import" in title:
                        bulk_db_id = bid

                elif b["type"] == "child_page":
                    title = b.get("child_page", {}).get("title", "").lower()
                    bid = b["id"]
                    if "job" in title or "role" in title:
                        jd_page_id = bid
                    elif "interview stages" in title:
                        interview_stages_page_id = bid

            if wf_db_id and form_db_id:
                break

        self.logger.debug(f"configure_process: child DBs — wf={wf_db_id[:8] if wf_db_id else None}..., form={form_db_id[:8] if form_db_id else None}..., bulk={bulk_db_id[:8] if bulk_db_id else None}..., feedback={feedback_db_id[:8] if feedback_db_id else None}...")
        if not wf_db_id or not form_db_id:
            self.logger.critical("Main child DBs not found (Workflow/Form). Check the template.")
            return

        # 4. Extract matrix characteristics from template
        matrix_chars = self._extract_matrix_from_template(wf_db_id)
        self.logger.debug(f"configure_process: matrix_chars={'present' if matrix_chars else 'none'} ({len(matrix_chars) if matrix_chars else 0} items)")

        # 5. Get Stages from Guidelines
        doc_guidelines = self.parser.find_guidelines_document(process_type)
        self.logger.debug(f"configure_process: guidelines doc {'found' if doc_guidelines else 'not found'} for type='{process_type}'")

        # 5b. Extract assessment characteristics from Guidelines
        assessment_chars = None
        if doc_guidelines:
            assessment_chars = self.parser.extract_assessment_characteristics(doc_guidelines["id"])
            self.logger.debug(f"configure_process: assessment_chars={'present' if assessment_chars else 'none'} ({len(assessment_chars) if assessment_chars else 0} items)")
        stage_options = []
        if doc_guidelines:
            stage_options = self.parser.parse_stages_from_page(doc_guidelines["id"])

            ZWSP = chr(0x200B)
            for i, stage in enumerate(stage_options):
                stage["name"] = ZWSP * i + stage["name"]

        # --- 6. CONFIGURE WORKFLOW ---
        res_wf_title = self.notion.update_database(wf_db_id, title=f"Feedback Tool & Workflow - {process_name}")
        if res_wf_title.status_code != 200:
            self.logger.error(f"Workflow title rename FAILED — db={wf_db_id[:8]}..., status={res_wf_title.status_code}")

        if stage_options:
            wf_ds_id = self.notion.get_data_source_id(wf_db_id)
            if wf_ds_id:
                wf_updates = {"Stage": {"select": {"options": stage_options}}}
                res_stages = self.notion.update_data_source(wf_ds_id, properties=wf_updates)
                if res_stages.status_code != 200:
                    self.logger.error(f"Stage options update FAILED — ds={wf_ds_id[:8]}..., stages={len(stage_options)}, status={res_stages.status_code}")

        # --- 7. CONFIGURE FORM DB ---
        res_form_title = self.notion.update_database(form_db_id, title=f"Single Candidate Application Upload Form - {process_name}")
        if res_form_title.status_code != 200:
            self.logger.error(f"Form DB title rename FAILED — db={form_db_id[:8]}..., status={res_form_title.status_code}")

        # --- CONFIGURE BULK QUEUE ---
        if bulk_db_id:
            res_bulk_title = self.notion.update_database(bulk_db_id, title=f"Bulk Candidate Application Upload Form - {process_name}")
            if res_bulk_title.status_code != 200:
                self.logger.error(f"Bulk DB title rename FAILED — db={bulk_db_id[:8]}..., status={res_bulk_title.status_code}")

        # --- CONFIGURE FEEDBACK FORM ---
        if feedback_db_id:
            res_fb_title = self.notion.update_database(feedback_db_id, title=f"Bulk & Single Feedback Upload Form - {process_name}")
            if res_fb_title.status_code != 200:
                self.logger.error(f"Feedback DB title rename FAILED — db={feedback_db_id[:8]}..., status={res_fb_title.status_code}")

        # --- 8. CONFIGURE JOB DESCRIPTION ---
        if jd_page_id:
            doc_jd = self.parser.find_job_description_document(process_type)
            if doc_jd:
                content_blocks = self.parser.extract_page_content(doc_jd["id"])
                if content_blocks:
                    existing_blocks = self.notion.get_page_blocks(jd_page_id)
                    anchor_id = existing_blocks[0]["id"] if existing_blocks else None

                    self.notion.append_block_children(jd_page_id, content_blocks[:100], after=anchor_id)

            new_jd_title = f"Role & Candidate Description - {process_name}" if is_portco else f"Job Description - {process_name}"
            res_jd = self.notion.update_page(jd_page_id, properties={"title": [{"text": {"content": new_jd_title}}]})
            if res_jd.status_code != 200:
                self.logger.error(f"JD page title FAILED — page={jd_page_id[:8]}..., status={res_jd.status_code}")

        # --- 8.B CONFIGURE INTERVIEW STAGES PAGE ---
        if interview_stages_page_id:
            if doc_guidelines:
                content_blocks_is = self.parser.extract_page_content(doc_guidelines["id"])
                if content_blocks_is:
                    existing_blocks_is = self.notion.get_page_blocks(interview_stages_page_id)
                    anchor_id_is = existing_blocks_is[0]["id"] if existing_blocks_is else None

                    CHUNK_SIZE = 100
                    for i in range(0, len(content_blocks_is), CHUNK_SIZE):
                        chunk = content_blocks_is[i:i + CHUNK_SIZE]
                        anchor = anchor_id_is if i == 0 else None
                        self.notion.append_block_children(interview_stages_page_id, chunk, after=anchor)

            new_is_title = f"Interview Stages - {process_name}"
            res_is = self.notion.update_page(interview_stages_page_id, properties={"title": [{"text": {"content": new_is_title}}]})
            if res_is.status_code != 200:
                self.logger.error(f"Interview Stages title FAILED — page={interview_stages_page_id[:8]}..., status={res_is.status_code}")

        # --- 9. REGISTER IN SUPABASE (BACKEND) ---
        self.logger.debug(f"configure_process: registering in Supabase — process='{process_name}'")
        supa_success = self.supa.register_process(
            wf_db_id,
            form_db_id,
            bulk_db_id,
            feedback_db_id,
            process_name,
            process_type,
            matrix_characteristics=matrix_chars,
            assessment_characteristics=assessment_chars
        )

        # 10. CLOSE
        self.logger.debug(f"configure_process: Supabase registration {'succeeded' if supa_success else 'failed'}")
        if supa_success:
            self.notion.update_page(page_id, properties={PROP_PROCESSED_DASHBOARD: {"checkbox": True}})
            self.logger.info("Process completed")
        else:
            self.logger.error("Error registering in Supabase")

    def run_from_webhook(self, page_id):
        """Processes a single dashboard page triggered by webhook (no polling)."""
        self._init_datasources()
        page = self.notion.get_page(page_id)
        if not page:
            self.logger.error(f"Could not fetch page {page_id}")
            return

        props = page.get("properties", {})
        ready = props.get(PROP_READY_TO_PROCESS, {}).get("checkbox", False)
        processed = props.get(PROP_PROCESSED_DASHBOARD, {}).get("checkbox", False)

        self.logger.debug(f"run_from_webhook: page={page_id[:8]}..., ready={ready}, processed={processed}")
        if not ready or processed:
            self.logger.info(f"Skipping: ready={ready}, processed={processed}")
            return

        self.configure_process(page)

    def run_once(self):
        self.logger.info("FactoryWorker starting")
        self._init_datasources()

        MAX_WAIT = 90
        POLL_INTERVAL = 10

        requests = self.find_pending_requests()
        self.logger.debug(f"run_once: initial poll found {len(requests)} request(s)")
        elapsed = 0
        while not requests and elapsed < MAX_WAIT:
            self.logger.info(f"No requests found, retrying in {POLL_INTERVAL}s... ({elapsed}/{MAX_WAIT}s elapsed)")
            time.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL
            requests = self.find_pending_requests()
            self.logger.debug(f"run_once: retry poll at {elapsed}s found {len(requests)} request(s)")

        if not requests:
            self.logger.info(f"No pending requests found after waiting {MAX_WAIT}s")
            return

        self.logger.debug(f"run_once: processing {len(requests)} request(s)")
        self.logger.info(f"Processing {len(requests)} requests")
        for req in requests:
            self.configure_process(req)

        self.logger.info("Execution completed")


if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()
    parser_guidelines = GuidelinesParser(client_notion)

    worker = FactoryWorkerV2(client_notion, client_supa, parser_guidelines)
    worker.run_once()
