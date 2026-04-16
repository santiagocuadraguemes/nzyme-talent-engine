import sys
import os
import re
import json
import time
from dotenv import load_dotenv


# Path adjustment for local/Lambda execution
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from core.notion_client import NotionClient, get_all_team_group_ids
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import CVAnalyzer
from core.notion_builder import NotionBuilder
from core.notion_parser import NotionParser
from core.domain_mapper import DomainMapper
from core.utils import download_file
from core.constants import (
    PROP_CHECKBOX_PROCESSED, PROP_ID, PROP_NAME, PROP_CV_FILES, PROP_STAGE,
    PROP_HEADHUNTER, PROP_AI_PENDING, PROP_EXP_TOTAL_YEARS,
    PROP_EXP_CONSULTING, PROP_EXP_AUDIT, PROP_EXP_IB, PROP_EXP_PE,
    PROP_EXP_VC, PROP_EXP_ENGINEER, PROP_EXP_LAWYER, PROP_EXP_FOUNDER,
    PROP_EXP_CORP_MA, PROP_EXP_PORTCO, PROP_EXP_MANAGEMENT, PROP_EXP_FINANCE,
    PROP_EXP_MARKETING, PROP_EXP_OPERATIONS, PROP_EXP_PRODUCT,
    PROP_EXP_SALES_REVENUE, PROP_EXP_TECHNOLOGY, PROP_EXP_INTERNATIONAL,
    PROP_EXP_INDUSTRIES, PROP_LANGUAGES, PROP_EDU_BACHELORS, PROP_EDU_MASTERS,
    PROP_EDU_UNIVERSITIES, PROP_EDU_MBAS,
    SOURCE_DIRECT_ENTRY_PREFIX,
)
from core.logger import get_logger


load_dotenv()


MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
TEMP_FOLDER = "/tmp/temp_downloads"
MAX_CVS_PER_RUN = 15


if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)


class HarvesterRelational:
    def __init__(self, notion_client, supa_client, storage_client, ai_analyzer, exa_client=None):
        self.logger = get_logger("Harvester")
        self.notion = notion_client
        self.supa_manager = supa_client
        self.storage = storage_client
        self.ai = ai_analyzer
        self.exa = exa_client

        self.main_ds_id = self.notion.get_data_source_id(MAIN_DB_ID) or MAIN_DB_ID


    def smart_candidate_search(self, email, name):
        """
        Matching logic for Notion.
        Rule: Email > Name.
        If the name matches, we treat it as the same candidate (Merge),
        even if the email is different or doesn't exist yet.
        """
        # A. Search by Email
        if email:
            res = self.notion.query_data_source(self.main_ds_id, {"property": "Email", "email": {"equals": email}})
            if res: return res[0]

        # B. Search by Name (Fallback)
        if name:
            res = self.notion.query_data_source(self.main_ds_id, {"property": "Name", "title": {"equals": name}})
            if res:
                self.logger.info(f"Match by Name: '{name}'. Assuming same candidate (Merge).")
                return res[0]

        return None


    def determine_initial_stage(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        options = schema.get("Stage", {}).get("select", {}).get("options", [])
        if not options: options = schema.get("Stage", {}).get("status", {}).get("options", [])
        if not options: return None
        return options[0]["name"] if options else None


    def find_relation_property(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        for name, details in schema.items():
            if details["type"] == "relation": return name
        return "Candidate Relation"


    def find_unique_id_property(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        for name, details in schema.items():
            if details["type"] == "unique_id": return name
        return "ID"


    def find_cv_in_auxiliary(self, aux_db_id, id_text):
        """
        Finds the form page (auxiliary) using the unique ID.
        Returns: (url, file_name, is_headhunter, form_data)

        form_data contains basic info from the form (name, email, linkedin) for cases without CV.
        """
        numbers = re.findall(r'\d+', str(id_text))
        if not numbers: return None, None, False, None
        numeric_id = int(numbers[-1])

        ds_aux = self.notion.get_data_source_id(aux_db_id)
        if not ds_aux: return None, None, False, None

        col_id_name = self.find_unique_id_property(ds_aux)
        filter_params = {"property": col_id_name, "unique_id": {"equals": numeric_id}}

        res = self.notion.query_data_source(ds_aux, filter_params)
        if not res: return None, None, False, None

        props = res[0]["properties"]

        # Extract form data for candidates without CV
        form_data = {
            "name": self._extract_title(props.get("Name", {})),
            "email": props.get("Email", {}).get("email"),
            "linkedin_url": props.get("LinkedIn", {}).get("url"),
        }

        # Extract headhunter flag
        is_headhunter = props.get(PROP_HEADHUNTER, {}).get("checkbox", False)

        files = props.get("CV", {}).get("files", [])
        if not files:
            # No CV attached - return form_data for minimal processing
            return None, None, is_headhunter, form_data

        file_obj = files[0]
        url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")

        if not url:
            return None, None, is_headhunter, form_data

        return url, file_obj["name"], is_headhunter, form_data

    def _extract_title(self, title_prop):
        """Helper to extract plain text from Notion title property."""
        title_list = title_prop.get("title", [])
        if title_list:
            return title_list[0].get("plain_text", "")
        return ""

    def _create_minimal_candidate_data(self, form_data):
        """
        Creates a CVData-compatible structure with empty defaults for candidates without CV.
        Uses form data (name, email, linkedin) as the foundation.
        """
        empty_sector = {
            "has_experience": False,
            "years": 0,
            "companies": []
        }
        empty_functional = {
            "has_experience": False,
            "years": 0,
            "roles": []
        }

        return {
            "name": form_data.get("name") or "Unnamed",
            "email": form_data.get("email"),
            "phone": None,
            "linkedin_url": form_data.get("linkedin_url"),
            "total_years": 0,
            "education": {
                "bachelors": [],
                "masters": [],
                "mba": "No",
                "university": []
            },
            "experience": {
                "consulting": empty_sector,
                "audit": empty_sector,
                "ib": empty_sector,
                "pe": empty_sector,
                "vc": empty_sector,
                "engineer_role": empty_sector,
                "lawyer": empty_sector,
                "founder": empty_sector,
                "management": empty_functional,
                "corp_ma": empty_sector,
                "portco_roles": empty_sector,
                "finance": empty_functional,
                "marketing": empty_functional,
                "operations": empty_functional,
                "product": empty_functional,
                "sales_revenue": empty_functional,
                "technology": empty_functional,
            },
            "general": {
                "international_locations": [],
                "industries_specialized": []
            },
            "languages": [],
            "strategic_assessment": []
        }

    def _process_with_cv(self, notion_url, file_name, process_entry):
        """
        Downloads CV, uploads to storage, and runs AI parsing.
        Returns: (ai_data, public_url, ai_failed)
          - Download/upload failure -> (None, None, False)
          - AI failure only -> (None, public_url, True) — CV stored, just couldn't parse
          - Success -> (ai_data, public_url, False)
        """
        local_path = download_file(notion_url, file_name, TEMP_FOLDER)
        if not local_path:
            return None, None, False

        public_url = self.storage.upload_cv_from_url(notion_url, file_name)
        if not public_url:
            try: os.remove(local_path)
            except OSError: pass
            return None, None, False

        matrix_chars = process_entry.get("matrix_characteristics")
        self.logger.info(f"Analyzing CV: {file_name}")
        ai_data = self.ai.process_cv(local_path, matrix_characteristics=matrix_chars)

        try: os.remove(local_path)
        except OSError: pass

        if not ai_data:
            return None, public_url, True

        return ai_data, public_url, False

    def _process_with_linkedin(self, linkedin_url, process_entry):
        """
        Fetches LinkedIn profile via Exa and parses with AI.
        Returns: ai_data dict or None on failure.
        """
        if not self.exa:
            return None

        text = self.exa.get_linkedin_profile(linkedin_url)
        if not text:
            self.logger.warning("Could not fetch LinkedIn profile via Exa")
            return None

        matrix_chars = process_entry.get("matrix_characteristics")
        self.logger.info("Parsing LinkedIn profile with AI")
        ai_data = self.ai.process_linkedin(text, matrix_characteristics=matrix_chars)

        if not ai_data:
            self.logger.warning("AI failed to parse LinkedIn profile")
            return None

        return ai_data

    # --- BATCH SPLITTER ---
    def process_bulk_imports(self, processes):
        """
        Checks the 'Bulk Queue' queues.
        Splits multi-file entries into individual Candidate Form entries.
        """
        for proc in processes:
            bulk_db_id = proc.get("notion_bulk_id")
            form_db_id = proc.get("notion_form_id")

            if not bulk_db_id or not form_db_id: continue


            ds_bulk = self.notion.get_data_source_id(bulk_db_id)
            if not ds_bulk: continue


            filter_params = {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}}
            batches = self.notion.query_data_source(ds_bulk, filter_params)

            if batches:
                self.logger.info(f"Splitting {len(batches)} batches in '{proc['process_name']}'")
                self.logger.debug(f"Bulk: {len(batches)} batches found for process '{proc['process_name']}'")

            for batch in batches:
                batch_id = batch["id"]
                props = batch["properties"]

                files = props.get("CVs", {}).get("files", [])
                self.logger.debug(f"Bulk batch {batch_id[:8]}...: {len(files)} file(s)")
                if not files:
                    res_empty = self.notion.update_page(batch_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
                    if res_empty.status_code != 200:
                        self.logger.error(f"Bulk mark-processed FAILED — batch={batch_id[:8]}..., status={res_empty.status_code}")
                    continue


                errors_in_batch = False


                for file_obj in files:
                    file_name = file_obj["name"]
                    notion_url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")

                    if not notion_url: continue


                    try:
                        public_url = self.storage.upload_cv_from_url(notion_url, file_name)

                        if not public_url:
                            self.logger.error(f"Error uploading {file_name} to storage")
                            errors_in_batch = True
                            continue
                    except Exception as e:
                        self.logger.error(f"Exception uploading {file_name}: {e}")
                        errors_in_batch = True
                        continue


                    payload = {
                        "Name": {"title": [{"text": {"content": f"Import: {file_name}"}}]},
                        "CV": {
                            "files": [
                                {
                                    "type": "external",
                                    "name": file_name,
                                    "external": {"url": public_url}
                                }
                            ]
                        },
                        PROP_HEADHUNTER: {"checkbox": True}
                    }

                    res = self.notion.create_page(form_db_id, payload)

                    if res.status_code != 200:
                        errors_in_batch = True
                        self.logger.error(f"Error creating '{file_name}'. Status: {res.status_code} | Body: {res.text[:500]}")
                    else:
                        self.logger.debug(f"Bulk: split file '{file_name}' -> new Form entry created")
                        # Delay so Notion automation fires separately for each entry
                        time.sleep(10)


                res_done = self.notion.update_page(batch_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
                if res_done.status_code != 200:
                    self.logger.error(f"Bulk mark-processed FAILED — batch={batch_id[:8]}..., status={res_done.status_code}")


    # --- STANDARD PROCESSING ---
    def process_candidate(self, cand, process_entry, relation_col_name, initial_stage):
        page_id = cand["id"]
        props = cand["properties"]

        # 0. Concurrency guard: skip if already processed by another invocation
        existing_app = self.supa_manager.get_application_by_notion_id(page_id)
        if existing_app:
            self.logger.info(f"Already processed by another invocation (workflow page {page_id[:8]}...). Skipping.")
            self.notion.update_page(page_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
            return

        try:
            self._process_candidate_inner(cand, process_entry, relation_col_name, initial_stage)
        except Exception as e:
            self.logger.error(f"Unexpected error processing {page_id[:8]}...: {e}", exc_info=True)
        finally:
            # Always mark workflow page as processed to prevent infinite reprocessing loops
            try:
                self.notion.update_page(page_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
            except Exception:
                self.logger.error(f"CRITICAL: Could not mark {page_id[:8]}... as processed")

    def _process_candidate_inner(self, cand, process_entry, relation_col_name, initial_stage):
        page_id = cand["id"]
        props = cand["properties"]
        current_process_name = process_entry["process_name"]
        current_process_type = process_entry.get("process_type")

        # 1. Get CV and form data (retry to handle Notion async indexing delay)
        id_text = props.get(PROP_ID, {}).get("rich_text", [])[0]["plain_text"] if props.get(PROP_ID, {}).get("rich_text", []) else ""
        notion_url, file_name, is_headhunter, form_data = self.find_cv_in_auxiliary(process_entry["notion_form_id"], id_text)

        if form_data is None and id_text:
            for attempt in range(3):
                self.logger.info(f"Form entry not found yet, retrying in 5s (attempt {attempt + 1}/3)")
                time.sleep(5)
                notion_url, file_name, is_headhunter, form_data = self.find_cv_in_auxiliary(process_entry["notion_form_id"], id_text)
                if form_data is not None:
                    break

        # Re-check concurrency guard before expensive AI processing
        if self.supa_manager.get_application_by_notion_id(page_id):
            self.logger.info(f"Application appeared during setup (race). Skipping AI for {page_id[:8]}...")
            return  # finally block marks Processed

        # Two-path processing: CV vs No-CV
        ai_failed = False
        needs_ai_pending = False
        if notion_url:
            # PATH A: Full CV processing
            self.logger.debug(f"Path A: processing with CV ({file_name})")
            ai_data, public_url, ai_failed = self._process_with_cv(notion_url, file_name, process_entry)
            if not ai_data and not ai_failed:
                return  # download/upload truly failed
            if ai_failed:
                self.logger.warning(f"AI unavailable for {file_name}. Creating skeleton record.")
                if not form_data:
                    form_data = {"name": file_name, "email": None, "linkedin_url": None}
                ai_data = self._create_minimal_candidate_data(form_data)
        else:
            # PATH B: No CV
            _fd_name = form_data.get('name') if form_data else None
            self.logger.debug(f"Path B: no CV, form_data name_prefix={(_fd_name[:2]+'***') if _fd_name else None}")
            if not form_data or not form_data.get("name"):
                self.logger.warning("No CV and no form data - skipping")
                return

            linkedin_url = form_data.get("linkedin_url")
            ai_data = None

            # Try LinkedIn enrichment if URL exists
            if linkedin_url:
                self.logger.info(f"Trying LinkedIn enrichment for: {form_data.get('name')}")
                ai_data = self._process_with_linkedin(linkedin_url, process_entry)

                if ai_data:
                    # Preserve form_data values the AI won't have
                    ai_data["linkedin_url"] = linkedin_url
                    if form_data.get("email") and not ai_data.get("email"):
                        ai_data["email"] = form_data["email"]
                    if not ai_data.get("phone"):
                        ai_data["phone"] = None

            # Fallback to minimal record if no LinkedIn or enrichment failed
            if not ai_data:
                self.logger.info(f"Processing candidate without CV/LinkedIn: {form_data.get('name')}. Marking AI Pending.")
                ai_data = self._create_minimal_candidate_data(form_data)
                needs_ai_pending = True

            public_url = None

        # --- 2. IDENTITY MANAGEMENT AND MERGE ---

        self.logger.debug(f"Resolving identity for: {ai_data['name']}")
        cand_db, main_notion_id = self.supa_manager.resolve_candidate_identity(ai_data.get("email"), ai_data["name"])

        previous_history = []
        previous_team_role = []

        # Determine if we should set source
        is_new_candidate = (cand_db is None)
        existing_source = cand_db.get("source") if cand_db else None
        should_set_source = is_new_candidate or (not existing_source)
        source_value = "Headhunter" if is_headhunter else "LinkedIn"

        if cand_db:
            self.logger.debug(f"Identity resolved: merge (existing notion_id={main_notion_id[:8] if main_notion_id else None}...)")
            self.logger.info(f"Existing candidate (ID: {main_notion_id}). Merging data")

            cand_json = cand_db.get("candidate_data") or {}

            previous_history = cand_json.get("recruiting_processes_history", [])
            previous_team_role = cand_json.get("proposed_teams_roles", [])

        else:
            self.logger.debug("Identity resolved: new candidate")
            self.logger.info(f"New candidate (Source: {source_value})")


        # Determine source to pass (new candidates OR existing with empty source)
        source_to_pass = source_value if should_set_source else None
        self.logger.info(f"Source tracking: is_new={is_new_candidate}, existing_source={existing_source}, should_set={should_set_source}, passing={source_to_pass}")

        # --- 3a. GOVERNANCE: Determine access control for Main DB page ---
        is_confidential = process_entry.get("is_confidential", False)
        process_governance = process_entry.get("governance_people")

        if is_confidential:
            governance_ids = set(process_governance or [])
            if cand_db:
                for people_list in self.supa_manager.get_active_confidential_processes_for_candidate(cand_db["id"]):
                    governance_ids.update(people_list)
            governance_entries = [{"object": "user", "id": uid} for uid in governance_ids]
            self.logger.info(f"Confidential process — restricting governance to {len(governance_entries)} users")
        else:
            if cand_db:
                other_conf = self.supa_manager.get_active_confidential_processes_for_candidate(cand_db["id"])
                if other_conf:
                    governance_entries = None  # don't touch — candidate is in a confidential process
                    self.logger.info("Non-confidential process but candidate is in a confidential process — preserving governance")
                else:
                    governance_entries = [{"object": "group", "id": gid} for gid in get_all_team_group_ids()]
            else:
                governance_entries = [{"object": "group", "id": gid} for gid in get_all_team_group_ids()]

        main_props = NotionBuilder.build_candidate_payload(
            ai_data,
            public_url,
            current_process_name,
            existing_history=previous_history,
            process_type=current_process_type,
            existing_team_role=previous_team_role,
            source=source_to_pass,
            governance_entries=governance_entries,
            skip_process_history=is_confidential
        )

        # --- 3b. SKELETON GUARD: Don't overwrite existing experience data ---
        if (ai_failed or needs_ai_pending) and main_notion_id:
            existing_page = self.notion.get_page(main_notion_id)
            if existing_page:
                existing_props = existing_page.get("properties", {})
                exp_prop_names = [
                    PROP_EXP_CONSULTING, PROP_EXP_AUDIT, PROP_EXP_IB, PROP_EXP_PE,
                    PROP_EXP_VC, PROP_EXP_ENGINEER, PROP_EXP_LAWYER, PROP_EXP_FOUNDER,
                    PROP_EXP_MANAGEMENT, PROP_EXP_CORP_MA, PROP_EXP_PORTCO,
                    PROP_EXP_FINANCE, PROP_EXP_MARKETING, PROP_EXP_OPERATIONS,
                    PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE, PROP_EXP_TECHNOLOGY,
                ]
                has_existing_exp = False
                for prop_name in exp_prop_names:
                    tags = NotionParser._extract_tags(existing_props.get(prop_name))
                    if tags and tags != ["No"]:
                        has_existing_exp = True
                        break

                if has_existing_exp:
                    self.logger.info(f"Skeleton guard: preserving existing experience data on {main_notion_id[:8]}...")
                    for prop_name in exp_prop_names:
                        main_props.pop(prop_name, None)
                    main_props.pop(PROP_EXP_TOTAL_YEARS, None)

        # --- 4. WRITE ---
        main_error = False
        main_props[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
        if ai_failed or needs_ai_pending:
            main_props[PROP_AI_PENDING] = {"checkbox": True}

        if main_notion_id:
            self.logger.debug(f"Notion write: update existing page {main_notion_id[:8]}...")
            res_op = self.notion.update_page(main_notion_id, main_props)
        else:
            self.logger.debug("Notion write: create new Main DB page")
            res_op = self.notion.create_page(MAIN_DB_ID, main_props)
            if res_op.status_code == 200:
                main_notion_id = res_op.json()["id"]


        if res_op.status_code != 200:
            op_type = "update" if main_notion_id else "create"
            self.logger.error(
                f"Notion {op_type} FAILED — candidate='{ai_data.get('name', '?')}', "
                f"process='{current_process_name}', page={main_notion_id or 'new'}, "
                f"status={res_op.status_code}, body={res_op.text[:300]}"
            )
            main_error = True
            return


        # 5. SUPABASE SYNC
        if not main_error:
            candidate_sql_data = DomainMapper.map_to_supabase_candidate(
                ai_data,
                public_url,
                source=source_to_pass
            )

            json_payload = candidate_sql_data["candidate_data"]

            if ai_failed or needs_ai_pending:
                json_payload["ai_pending"] = True
                json_payload["ai_pending_cv_url"] = public_url
                json_payload["ai_pending_process_name"] = current_process_name

            full_history = list(previous_history)
            if current_process_name not in full_history:
                full_history.append(current_process_name)
            json_payload["recruiting_processes_history"] = full_history


            full_roles = list(previous_team_role)
            if current_process_type and current_process_type not in full_roles: full_roles.append(current_process_type)
            json_payload["proposed_teams_roles"] = full_roles


            candidate_uuid = self.supa_manager.manage_candidate(candidate_sql_data, main_notion_id)
            self.logger.debug(f"Supabase sync result: candidate_uuid={candidate_uuid[:8] if candidate_uuid else None}...")

            if candidate_uuid:
                app_id = self.supa_manager.create_application(
                    candidate_uuid,
                    process_entry["notion_workflow_id"],
                    page_id,
                    initial_stage
                )

                # 5b. Discover and store Outcome Form DB ID
                if app_id:
                    outcome_db_id = self.notion.find_child_database(page_id, "Process Outcome Form")
                    if outcome_db_id:
                        self.supa_manager.update_application_outcome_id(app_id, outcome_db_id)
                        self.logger.debug(f"Outcome Form DB stored: {outcome_db_id[:8]}... for app {app_id[:8]}...")
                    else:
                        self.logger.debug(f"No Outcome Form DB found on workflow page {page_id[:8]}...")


        # 6. Strategic Assessment
        if not ai_failed and not needs_ai_pending and ai_data.get("strategic_assessment"):
            self._fill_strategic_assessment(page_id, ai_data["strategic_assessment"])


        # 7. CLOSE
        update_props = {
            PROP_NAME: {"title": [{"text": {"content": ai_data["name"]}}]},
        }
        # Only set CV file if we have one
        if public_url:
            candidate_name = ai_data.get("name", "Unknown")
            cv_display_name = f"CV - {candidate_name}"
            update_props[PROP_CV_FILES] = {"files": [{"name": cv_display_name, "external": {"url": public_url}}]}
        if main_notion_id:
            update_props[relation_col_name] = {"relation": [{"id": main_notion_id}]}
        if initial_stage:
            update_props[PROP_STAGE] = {"select": {"name": initial_stage}}

        res_close = self.notion.update_page(page_id, update_props)
        if res_close.status_code != 200:
            self.logger.error(
                f"Workflow close FAILED — candidate='{ai_data.get('name', '?')}', "
                f"page={page_id[:8]}..., status={res_close.status_code}"
            )
        self.logger.info("Candidate processed successfully")


    def _fill_strategic_assessment(self, candidate_page_id, assessment_list):
        """
        Finds the 'Past Experience [AI-generated]' table, maps existing rows
        and fills them with AI data.
        """
        if not assessment_list:
            self.logger.info("No strategic assessment data to fill (skipping)")
            return

        time.sleep(4)


        db_title = "Past Experience [AI-generated]"
        child_db_id = self.notion.find_child_database(candidate_page_id, db_title)

        if not child_db_id:
            self.logger.warning(f"DB '{db_title}' not found")
            return


        ds_child = self.notion.get_data_source_id(child_db_id) or child_db_id


        rows = self.notion.query_data_source(ds_child, filter_params=None)

        row_map = {}
        for r in rows:
            props = r["properties"]
            title_list = props.get("Characteristic", {}).get("title", [])
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
                    self.logger.error(f"Error writing '{char_name}': {res.status_code} | Body: {res.text[:500]}")


        self.logger.info(f"Assessment completed: {updates_count}/{len(assessment_list)} rows")


    def _reprocess_ai_pending(self):
        """
        Second pass: finds candidates with AI Pending checkbox=true in Notion Main DB,
        re-runs AI parsing, and backfills experience/education data.

        Uses Notion as source of truth (not Supabase JSONB) to avoid race condition
        where Observer overwrites ai_pending keys before reprocessor runs.
        """
        # 1. Query Notion Main DB for AI Pending = true
        try:
            filter_params = {"property": PROP_AI_PENDING, "checkbox": {"equals": True}}
            pending_pages = self.notion.query_data_source(self.main_ds_id, filter_params)
        except Exception as e:
            self.logger.error(f"Error querying AI-pending candidates from Notion: {e}")
            return

        if not pending_pages:
            return

        # Batch limit
        pending_pages = pending_pages[:5]
        self.logger.info(f"Reprocessing {len(pending_pages)} AI-pending candidates")

        for page in pending_pages:
            notion_page_id = page["id"]
            props = page.get("properties", {})

            candidate_name = self._extract_title(props.get(PROP_NAME, {})) or "Unknown"

            # 2. Look up candidate in Supabase to get UUID, JSONB, and application info
            candidate = self.supa_manager.get_candidate_by_notion_page_id(notion_page_id)
            if not candidate:
                self.logger.warning(f"Skipping AI-pending candidate {candidate_name}: not found in Supabase")
                continue

            cand_json = candidate.get("candidate_data") or {}

            # 3. Get matrix_characteristics and workflow page ID from applications
            matrix_chars = None
            workflow_page_id = None
            applications = self.supa_manager.get_applications_by_candidate_id(candidate["id"])
            if applications:
                latest_app = applications[0]  # Already ordered by created_at desc
                matrix_chars = latest_app.get("matrix_characteristics")
                workflow_page_id = latest_app.get("notion_page_id")

            # 4. Extract CV URL from Main DB page's CV file property
            cv_files = props.get(PROP_CV_FILES, {}).get("files", [])
            cv_url = None
            if cv_files:
                first_file = cv_files[0]
                cv_url = first_file.get("external", {}).get("url") or first_file.get("file", {}).get("url")

            # Fallback: check Workflow page for CV (handles direct-entry cases where CV was added after processing)
            if not cv_url and workflow_page_id:
                try:
                    wf_page = self.notion.get_page(workflow_page_id)
                    if wf_page:
                        wf_cv_files = wf_page.get("properties", {}).get(PROP_CV_FILES, {}).get("files", [])
                        if wf_cv_files:
                            wf_file = wf_cv_files[0]
                            wf_cv_url = wf_file.get("file", {}).get("url") or wf_file.get("external", {}).get("url")
                            if wf_cv_url:
                                self.logger.info(f"AI-pending {candidate_name}: CV found on Workflow page (fallback)")
                                # Upload to permanent storage and set on Main DB page
                                wf_file_name = wf_file.get("name", "cv.pdf")
                                public_url = self.storage.upload_cv_from_url(wf_cv_url, wf_file_name)
                                if public_url:
                                    cv_url = public_url
                                    cv_display = f"CV - {candidate_name}"
                                    self.notion.update_page(notion_page_id, {
                                        PROP_CV_FILES: {"files": [{"name": cv_display, "external": {"url": public_url}}]}
                                    })
                except Exception as e:
                    self.logger.warning(f"Error checking Workflow page CV for {candidate_name}: {e}")

            if not cv_url:
                self.logger.warning(f"Skipping AI-pending candidate {candidate_name}: no CV URL in Notion or Workflow")
                continue
            self.logger.debug(f"AI-pending {candidate_name}: CV URL found ({cv_url[:40]}...)")

            # 5. Download CV from permanent storage URL
            safe_name = cv_url.split("/")[-1] if "/" in cv_url else "temp_cv"
            local_path = download_file(cv_url, safe_name, TEMP_FOLDER)
            if not local_path:
                self.logger.warning(f"Could not download CV for reprocessing: {candidate_name}")
                continue

            # 6. Run AI parsing
            ai_data = self.ai.process_cv(local_path, matrix_characteristics=matrix_chars)
            try: os.remove(local_path)
            except OSError: pass

            if not ai_data:
                self.logger.warning(f"AI still unavailable for {candidate_name}. Will retry next run.")
                continue
            self.logger.debug(f"AI-pending {candidate_name}: parse succeeded")

            # 7. Update Notion — ONLY experience/education/languages fields
            exp = ai_data.get("experience", {})
            edu = ai_data.get("education", {})
            gen = ai_data.get("general", {})

            notion_props = {}

            # Total years
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

            # General lists
            if gen.get("international_locations"):
                notion_props[PROP_EXP_INTERNATIONAL] = {"multi_select": NotionBuilder._format_multi_select(gen["international_locations"])}
            if gen.get("industries_specialized"):
                notion_props[PROP_EXP_INDUSTRIES] = {"multi_select": NotionBuilder._format_multi_select(gen["industries_specialized"])}

            # Languages
            if ai_data.get("languages"):
                notion_props[PROP_LANGUAGES] = {"multi_select": NotionBuilder._format_multi_select(ai_data["languages"])}

            # Education
            if edu.get("bachelors"):
                notion_props[PROP_EDU_BACHELORS] = {"multi_select": NotionBuilder._format_multi_select(edu["bachelors"])}
            if edu.get("masters"):
                notion_props[PROP_EDU_MASTERS] = {"multi_select": NotionBuilder._format_multi_select(edu["masters"])}
            if edu.get("university"):
                notion_props[PROP_EDU_UNIVERSITIES] = {"multi_select": NotionBuilder._format_multi_select(edu["university"])}
            mba_val = edu.get("mba")
            if isinstance(mba_val, str) and mba_val != "No":
                notion_props[PROP_EDU_MBAS] = {"multi_select": NotionBuilder._format_multi_select([mba_val])}

            # Uncheck AI Pending
            notion_props[PROP_AI_PENDING] = {"checkbox": False}

            res_notion = self.notion.update_page(notion_page_id, notion_props)
            if res_notion.status_code != 200:
                self.logger.error(f"Error updating Notion for reprocessed candidate: {res_notion.status_code}")
                continue
            self.logger.debug(f"AI-pending {candidate_name}: Notion updated (page {notion_page_id[:8]}...)")

            # 8. Update Supabase — merge AI data into existing candidate_data JSONB
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

            # Remove ai_pending keys
            updated_json.pop("ai_pending", None)
            updated_json.pop("ai_pending_cv_url", None)
            updated_json.pop("ai_pending_process_name", None)

            try:
                self.supa_manager.client.table("NzymeTalentNetwork").update({
                    "candidate_data": updated_json,
                    "updated_at": "now()"
                }).eq("id", candidate["id"]).execute()
                self.logger.debug(f"AI-pending {candidate_name}: Supabase merged (id={candidate['id'][:8]}...)")
            except Exception as e:
                self.logger.error(f"Error updating Supabase for reprocessed candidate: {e}")
                continue

            # 9. Fill strategic assessment using WORKFLOW page ID (not Main DB page ID)
            if ai_data.get("strategic_assessment") and workflow_page_id:
                self._fill_strategic_assessment(workflow_page_id, ai_data["strategic_assessment"])
            elif ai_data.get("strategic_assessment"):
                self.logger.warning(f"No workflow page found for {candidate_name}, skipping strategic assessment")

            self.logger.info(f"Successfully reprocessed: {candidate_name}")

    def process_single_from_webhook(self, page_id, process_context):
        """
        Processes unprocessed candidates for a specific process, triggered by webhook.
        Queries the Workflow DB for candidates with Processed=false and ID populated.

        Note: page_id is the Form DB page that triggered the webhook, but processing
        uses Workflow DB pages (created by Notion automation). Retries up to 3 times
        (5s apart) to wait for the automation. EventBridge is the final fallback.
        """
        wf_db_id = process_context["notion_workflow_id"]
        self.logger.debug(f"[WEBHOOK] Resolving data source for workflow {wf_db_id[:8]}...")
        ds_wf = self.notion.get_data_source_id(wf_db_id)
        if not ds_wf:
            self.logger.error(f"Could not resolve data source for workflow {wf_db_id}")
            return
        self.logger.debug(f"[WEBHOOK] Data source resolved: {ds_wf[:8]}...")

        filter_params = {
            "and": [
                {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}},
                {"property": PROP_ID, "rich_text": {"is_not_empty": True}}
            ]
        }
        candidates = self.notion.query_data_source(ds_wf, filter_params)

        # Retry: Notion automation may not have created the Workflow entry yet
        if not candidates:
            for attempt in range(3):
                self.logger.info(f"[WEBHOOK] No unprocessed candidates yet, waiting for Notion automation (attempt {attempt + 1}/3)")
                time.sleep(5)
                candidates = self.notion.query_data_source(ds_wf, filter_params)
                if candidates:
                    break

        if not candidates:
            self.logger.info("[WEBHOOK] No unprocessed candidates after retries. EventBridge will catch them.")
            return

        self.logger.debug(f"[WEBHOOK] {len(candidates)} unprocessed candidate(s) found in Workflow DB")
        rel_col = self.find_relation_property(ds_wf)
        stage_init = self.determine_initial_stage(ds_wf)

        self.logger.info(f"[WEBHOOK] Processing {len(candidates)} candidates")
        for cand in candidates[:MAX_CVS_PER_RUN]:
            self.process_candidate(cand, process_context, rel_col, stage_init)


    # --- DIRECT ENTRY PROCESSING (Step 2.5) ---

    def _process_direct_candidates(self, processes, cvs_processed):
        """
        Step 2.5: Process candidates added directly to Workflow DBs (no Form entry).
        These pages have Processed=false, empty ID field, and a non-empty Name.
        """
        for proc in processes:
            if cvs_processed >= MAX_CVS_PER_RUN:
                break

            wf_db_id = proc["notion_workflow_id"]
            ds_wf = self.notion.get_data_source_id(wf_db_id)
            if not ds_wf:
                continue

            # Complementary filter to standard processing (ID is_empty vs is_not_empty)
            filter_params = {
                "and": [
                    {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}},
                    {"property": PROP_ID, "rich_text": {"is_empty": True}},
                    {"property": PROP_NAME, "title": {"is_not_empty": True}}
                ]
            }
            candidates = self.notion.query_data_source(ds_wf, filter_params)

            if not candidates:
                continue

            self.logger.info(f"[DIRECT] {len(candidates)} direct-entry candidate(s) in '{proc['process_name']}'")

            rel_col = self.find_relation_property(ds_wf)
            stage_init = self.determine_initial_stage(ds_wf)

            for cand in candidates:
                if cvs_processed >= MAX_CVS_PER_RUN:
                    break
                self._process_direct_candidate(cand, proc, rel_col, stage_init)
                cvs_processed += 1

        return cvs_processed


    def _process_direct_candidate(self, cand, process_entry, relation_col_name, initial_stage):
        """Wrapper with try/finally for direct-entry candidates (mirrors process_candidate pattern)."""
        page_id = cand["id"]

        # Concurrency guard
        existing_app = self.supa_manager.get_application_by_notion_id(page_id)
        if existing_app:
            self.logger.info(f"[DIRECT] Already processed (workflow page {page_id[:8]}...). Skipping.")
            self.notion.update_page(page_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
            return

        try:
            self._process_direct_candidate_inner(cand, process_entry, relation_col_name, initial_stage)
        except Exception as e:
            self.logger.error(f"[DIRECT] Error processing {page_id[:8]}...: {e}", exc_info=True)
        finally:
            try:
                self.notion.update_page(page_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
            except Exception:
                self.logger.error(f"[DIRECT] CRITICAL: Could not mark {page_id[:8]}... as processed")


    def _process_direct_candidate_inner(self, cand, process_entry, relation_col_name, initial_stage):
        """Inner processing logic for candidates added directly to Workflow DB (bypassing Form)."""
        page_id = cand["id"]
        props = cand["properties"]
        current_process_name = process_entry["process_name"]
        current_process_type = process_entry.get("process_type")

        # 1. Extract data from Workflow page (NOT from Form)
        name = self._extract_title(props.get(PROP_NAME, {}))
        if not name:
            self.logger.warning(f"[DIRECT] Empty name on page {page_id[:8]}..., skipping")
            return

        creator_name = cand.get("created_by", {}).get("name", "Unknown")
        source_value = f"{SOURCE_DIRECT_ENTRY_PREFIX} - {creator_name}"

        # Preserve existing stage if already set (don't override manual stage placement)
        stage_prop = props.get(PROP_STAGE, {})
        existing_stage = None
        if stage_prop.get("select"):
            existing_stage = stage_prop["select"]["name"]
        elif stage_prop.get("status"):
            existing_stage = stage_prop["status"]["name"]
        effective_stage = existing_stage or initial_stage

        cv_files = props.get(PROP_CV_FILES, {}).get("files", [])
        notion_url = None
        file_name = None
        if cv_files:
            file_obj = cv_files[0]
            notion_url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")
            file_name = file_obj.get("name", "cv.pdf")

        # Re-check concurrency guard before expensive AI processing
        if self.supa_manager.get_application_by_notion_id(page_id):
            self.logger.info(f"[DIRECT] Race detected for {page_id[:8]}...")
            return

        # 2. Process CV or create minimal record
        ai_failed = False
        needs_ai_pending = False
        public_url = None

        if notion_url:
            self.logger.debug(f"[DIRECT] Processing with CV ({file_name})")
            ai_data, public_url, ai_failed = self._process_with_cv(notion_url, file_name, process_entry)
            if not ai_data and not ai_failed:
                return  # download/upload failed
            if ai_failed:
                self.logger.warning(f"[DIRECT] AI failed for {file_name}. Creating skeleton.")
                ai_data = self._create_minimal_candidate_data({"name": name, "email": None, "linkedin_url": None})
        else:
            self.logger.info(f"[DIRECT] No CV for '{name}'. Creating minimal record, marking AI Pending.")
            ai_data = self._create_minimal_candidate_data({"name": name, "email": None, "linkedin_url": None})
            needs_ai_pending = True

        # 3. Identity resolution
        self.logger.debug(f"[DIRECT] Resolving identity for: {ai_data['name']}")
        cand_db, main_notion_id = self.supa_manager.resolve_candidate_identity(ai_data.get("email"), ai_data["name"])

        if cand_db and not ai_data.get("email"):
            self.logger.warning(f"[DIRECT] Name-only merge for '{ai_data['name']}' — verify candidate identity")

        previous_history = []
        previous_team_role = []

        is_new_candidate = (cand_db is None)
        existing_source = cand_db.get("source") if cand_db else None
        should_set_source = is_new_candidate or (not existing_source)

        if cand_db:
            self.logger.info(f"[DIRECT] Existing candidate (ID: {main_notion_id}). Merging data")
            cand_json = cand_db.get("candidate_data") or {}
            previous_history = cand_json.get("recruiting_processes_history", [])
            previous_team_role = cand_json.get("proposed_teams_roles", [])
        else:
            self.logger.info(f"[DIRECT] New candidate (Source: {source_value})")

        source_to_pass = source_value if should_set_source else None

        # 3b. GOVERNANCE: Determine access control for Main DB page
        is_confidential = process_entry.get("is_confidential", False)
        process_governance = process_entry.get("governance_people")

        if is_confidential:
            governance_ids = set(process_governance or [])
            if cand_db:
                for people_list in self.supa_manager.get_active_confidential_processes_for_candidate(cand_db["id"]):
                    governance_ids.update(people_list)
            governance_entries = [{"object": "user", "id": uid} for uid in governance_ids]
            self.logger.info(f"[DIRECT] Confidential process — restricting governance to {len(governance_entries)} users")
        else:
            if cand_db:
                other_conf = self.supa_manager.get_active_confidential_processes_for_candidate(cand_db["id"])
                if other_conf:
                    governance_entries = None
                    self.logger.info("[DIRECT] Non-confidential but candidate is in confidential process — preserving governance")
                else:
                    governance_entries = [{"object": "group", "id": gid} for gid in get_all_team_group_ids()]
            else:
                governance_entries = [{"object": "group", "id": gid} for gid in get_all_team_group_ids()]

        # 4. Build Main DB payload
        main_props = NotionBuilder.build_candidate_payload(
            ai_data,
            public_url,
            current_process_name,
            existing_history=previous_history,
            process_type=current_process_type,
            existing_team_role=previous_team_role,
            source=source_to_pass,
            governance_entries=governance_entries,
            skip_process_history=is_confidential
        )

        # Skeleton guard: don't overwrite existing experience data
        if (ai_failed or needs_ai_pending) and main_notion_id:
            existing_page = self.notion.get_page(main_notion_id)
            if existing_page:
                existing_props = existing_page.get("properties", {})
                exp_prop_names = [
                    PROP_EXP_CONSULTING, PROP_EXP_AUDIT, PROP_EXP_IB, PROP_EXP_PE,
                    PROP_EXP_VC, PROP_EXP_ENGINEER, PROP_EXP_LAWYER, PROP_EXP_FOUNDER,
                    PROP_EXP_MANAGEMENT, PROP_EXP_CORP_MA, PROP_EXP_PORTCO,
                    PROP_EXP_FINANCE, PROP_EXP_MARKETING, PROP_EXP_OPERATIONS,
                    PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE, PROP_EXP_TECHNOLOGY,
                ]
                has_existing_exp = False
                for prop_name in exp_prop_names:
                    tags = NotionParser._extract_tags(existing_props.get(prop_name))
                    if tags and tags != ["No"]:
                        has_existing_exp = True
                        break

                if has_existing_exp:
                    self.logger.info(f"[DIRECT] Skeleton guard: preserving existing experience on {main_notion_id[:8]}...")
                    for prop_name in exp_prop_names:
                        main_props.pop(prop_name, None)
                    main_props.pop(PROP_EXP_TOTAL_YEARS, None)

        # 5. Write Main DB page
        main_props[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
        if ai_failed or needs_ai_pending:
            main_props[PROP_AI_PENDING] = {"checkbox": True}

        if main_notion_id:
            res_op = self.notion.update_page(main_notion_id, main_props)
        else:
            res_op = self.notion.create_page(MAIN_DB_ID, main_props)
            if res_op.status_code == 200:
                main_notion_id = res_op.json()["id"]

        if res_op.status_code != 200:
            op_type = "update" if main_notion_id else "create"
            self.logger.error(
                f"[DIRECT] Notion {op_type} FAILED — candidate='{ai_data.get('name', '?')}', "
                f"process='{current_process_name}', status={res_op.status_code}, body={res_op.text[:300]}"
            )
            return

        # 6. Supabase sync
        candidate_sql_data = DomainMapper.map_to_supabase_candidate(
            ai_data,
            public_url,
            source=source_to_pass
        )

        json_payload = candidate_sql_data["candidate_data"]

        if ai_failed or needs_ai_pending:
            json_payload["ai_pending"] = True
            json_payload["ai_pending_cv_url"] = public_url
            json_payload["ai_pending_process_name"] = current_process_name

        full_history = list(previous_history)
        if current_process_name not in full_history:
            full_history.append(current_process_name)
        json_payload["recruiting_processes_history"] = full_history

        full_roles = list(previous_team_role)
        if current_process_type and current_process_type not in full_roles:
            full_roles.append(current_process_type)
        json_payload["proposed_teams_roles"] = full_roles

        candidate_uuid = self.supa_manager.manage_candidate(candidate_sql_data, main_notion_id)

        if candidate_uuid:
            app_id = self.supa_manager.create_application(
                candidate_uuid,
                process_entry["notion_workflow_id"],
                page_id,
                effective_stage
            )

            # Discover and store Outcome Form DB ID
            if app_id:
                outcome_db_id = self.notion.find_child_database(page_id, "Process Outcome Form")
                if outcome_db_id:
                    self.supa_manager.update_application_outcome_id(app_id, outcome_db_id)

        # 7. Strategic Assessment
        if not ai_failed and not needs_ai_pending and ai_data.get("strategic_assessment"):
            self._fill_strategic_assessment(page_id, ai_data["strategic_assessment"])

        # 8. Close Workflow page
        update_props = {
            PROP_NAME: {"title": [{"text": {"content": ai_data["name"]}}]},
        }
        if public_url:
            cv_display_name = f"CV - {ai_data.get('name', 'Unknown')}"
            update_props[PROP_CV_FILES] = {"files": [{"name": cv_display_name, "external": {"url": public_url}}]}
        if main_notion_id:
            update_props[relation_col_name] = {"relation": [{"id": main_notion_id}]}
        if initial_stage and not existing_stage:
            update_props[PROP_STAGE] = {"select": {"name": initial_stage}}

        res_close = self.notion.update_page(page_id, update_props)
        if res_close.status_code != 200:
            self.logger.error(
                f"[DIRECT] Workflow close FAILED — candidate='{ai_data.get('name', '?')}', "
                f"page={page_id[:8]}..., status={res_close.status_code}"
            )
        self.logger.info(f"[DIRECT] Candidate processed successfully: {ai_data['name']}")


    def run_once(self):
        """Executes a full pass through all active processes."""
        self.logger.info("Harvester starting")

        processes = self.supa_manager.get_active_processes()
        if not processes:
            self.logger.info("No active processes")
            return
        self.logger.debug(f"run_once: {len(processes)} active process(es)")


        # --- STEP 1: BATCH SPLITTER ---
        self.process_bulk_imports(processes)


        # --- STEP 2: STANDARD PROCESSING ---
        cvs_processed_today = 0
        for proc in processes:

            if cvs_processed_today >= MAX_CVS_PER_RUN:
                self.logger.warning(f"Safety limit reached ({MAX_CVS_PER_RUN} CVs)")
                break


            wf_db_id = proc["notion_workflow_id"]
            ds_wf = self.notion.get_data_source_id(wf_db_id)
            if not ds_wf: continue


            rel_col = self.find_relation_property(ds_wf)

            filter_params = {
                "and": [
                    {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}},
                    {"property": PROP_ID, "rich_text": {"is_not_empty": True}}
                ]
            }
            candidates = self.notion.query_data_source(ds_wf, filter_params)

            if candidates:
                self.logger.info(f"Processing {len(candidates)} candidates in '{proc['process_name']}'")
                self.logger.debug(f"run_once: {len(candidates)} unprocessed candidates in '{proc['process_name']}'")
                stage_init = self.determine_initial_stage(ds_wf)
                for cand in candidates:


                    if cvs_processed_today >= MAX_CVS_PER_RUN:
                        self.logger.debug(f"run_once: safety limit reached ({MAX_CVS_PER_RUN}), stopping early")
                        break
                    self.process_candidate(cand, proc, rel_col, stage_init)
                    cvs_processed_today += 1

        # --- STEP 2.5: DIRECT ENTRY PROCESSING ---
        cvs_processed_today = self._process_direct_candidates(processes, cvs_processed_today)

        # --- STEP 3: REPROCESS AI PENDING ---
        self._reprocess_ai_pending()

        self.logger.info("Execution completed")


if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()
    client_storage = StorageClient()
    analyzer_ai = CVAnalyzer()

    exa = None
    try:
        from core.exa_client import ExaClient
        exa = ExaClient()
    except (ValueError, ImportError) as e:
        print(f"[WARNING] ExaClient not available: {e}. LinkedIn enrichment disabled.")

    bot = HarvesterRelational(client_notion, client_supa, client_storage, analyzer_ai, exa_client=exa)
    bot.run_once()
