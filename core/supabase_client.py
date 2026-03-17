# core/supabase_client.py

import os
import unicodedata
from supabase import create_client, Client
from dotenv import load_dotenv
from core.logger import get_logger


load_dotenv()


class SupabaseManager:
    def __init__(self):
        self.logger = get_logger("SupabaseManager")
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("Missing Supabase credentials in .env")
        self.client: Client = create_client(url, key)


    # --- PROCESS MANAGEMENT ---
    def register_process(self, notion_wf_id, notion_form_id, bulk_id, feedback_id, name, process_type, matrix_characteristics=None, assessment_characteristics=None):
        data = {
            "notion_workflow_id": notion_wf_id,
            "notion_form_id": notion_form_id,
            "notion_bulk_id": bulk_id,
            "notion_feedback_id": feedback_id,
            "process_name": name,
            "process_type": process_type,
            "status": "Open",
            "matrix_characteristics": matrix_characteristics,
            "assessment_characteristics": assessment_characteristics
        }
        try:
            self.client.table("NzymeRecruitingProcesses").insert(data).execute()
            self.logger.info(f"Process registered: {name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to register process: {e}")
            return False


    def get_active_processes(self):
        try:
            response = self.client.table("NzymeRecruitingProcesses").select("*").eq("status", "Open").execute()
            return response.data
        except Exception as e:
            self.logger.error(f"Error reading active processes: {e}")
            return []


    def update_process_status_by_name(self, name, new_status):
        """Updates the status (Open/Closed) based on the process name."""
        try:
            self.client.table("NzymeRecruitingProcesses").update({
                "status": new_status,
                "updated_at": "now()"
            }).eq("process_name", name).execute()
            return True
        except Exception as e:
            self.logger.error(f"Failed to update process: {e}")
            return False


    # --- CANDIDATE MANAGEMENT (IDENTITY ENGINE) ---
    def manage_candidate(self, candidate_data, notion_page_id):
        """
        Bulletproof Identity Logic:
        1. Maps explicit SQL columns (creator, source, assessment).
        2. Saves the rest in JSONB (candidate_data).
        3. Performs search by Notion ID or Email to decide Update/Insert.
        """
        try:
            # A. Prepare explicit SQL payload
            payload = {
                "name": candidate_data.get("name"),
                "email": candidate_data.get("email"),
                "phone": candidate_data.get("phone"),
                "linkedin_url": candidate_data.get("linkedin_url"),
                "cv_url": candidate_data.get("cv_url"),

                # --- NEW SQL FIELDS ---
                "creator": candidate_data.get("creator"),
                "source": candidate_data.get("source"),
                "assessment": candidate_data.get("assessment"),
                # -------------------------

                "candidate_data": candidate_data.get("candidate_data"),
                "notion_page_id": notion_page_id,
                "updated_at": "now()"
            }

            # B. Search for existing (OR Logic)
            email = candidate_data.get("email")

            query = self.client.table("NzymeTalentNetwork").select("id")

            or_filter = f"notion_page_id.eq.{notion_page_id}"

            if email:
                email_clean = email.strip()
                or_filter += f",email.eq.{email_clean}"

            self.logger.debug(f"manage_candidate: OR filter = {or_filter}")
            existing = query.or_(or_filter).execute()


            if existing.data:
                # C. UPDATE (Already exists)
                cid = existing.data[0]['id']
                self.logger.debug(f"manage_candidate: existing candidate found — UPDATE {cid[:8]}...")
                # Don't overwrite source with None for existing candidates
                if payload.get("source") is None:
                    del payload["source"]
                self.client.table("NzymeTalentNetwork").update(payload).eq("id", cid).execute()
                self.logger.debug(f"manage_candidate: UPDATE complete for {cid[:8]}...")
                return cid
            else:
                # D. INSERT (New)
                self.logger.debug("manage_candidate: no existing candidate — INSERT")
                response = self.client.table("NzymeTalentNetwork").insert(payload).execute()
                if response.data:
                    new_id = response.data[0]['id']
                    self.logger.debug(f"manage_candidate: INSERT complete — new id {new_id[:8]}...")
                    return new_id
                return None


        except Exception as e:
            self.logger.error(f"Failed to manage candidate: {e}", exc_info=True)
            return None


    # --- APPLICATION MANAGEMENT ---
    def create_application(self, candidate_uuid, notion_wf_id, notion_page_id, initial_stage):
        """Creates or upserts an application. Returns the application UUID on success, None on failure."""
        try:
            proc_res = self.client.table("NzymeRecruitingProcesses").select("id").eq("notion_workflow_id", notion_wf_id).execute()
            if not proc_res.data:
                self.logger.debug(f"create_application: no process found for workflow_id {notion_wf_id[:8]}...")
                return None
            process_uuid = proc_res.data[0]['id']
            self.logger.debug(f"create_application: resolved process_uuid {process_uuid[:8]}...")


            app_data = {
                "candidate_id": candidate_uuid,
                "process_id": process_uuid,
                "notion_page_id": notion_page_id,
                "current_stage": initial_stage,
                "status": "Active"
            }
            res = self.client.table("NzymeRecruitingApplications").upsert(
                app_data, on_conflict="candidate_id, process_id"
            ).execute()
            if res.data:
                app_id = res.data[0]["id"]
                self.logger.debug(f"create_application: success — app_id {app_id[:8]}...")
                return app_id
            return None
        except Exception as e:
            self.logger.error(f"Failed to create application: {e}")
            return None


    # --- OBSERVER METHODS ---
    def get_application_by_notion_id(self, notion_page_id):
        try:
            res = self.client.table("NzymeRecruitingApplications").select("id, current_stage").eq("notion_page_id", notion_page_id).execute()
            if res.data: return res.data[0]
            return None
        except Exception: return None


    def register_stage_change(self, app_id, old_stage, new_stage):
        try:
            self.logger.debug(f"register_stage_change: app {app_id[:8]}... — '{old_stage}' -> '{new_stage}'")
            self.client.table("NzymeRecruitingApplications").update({
                "current_stage": new_stage,
                "updated_at": "now()"
            }).eq("id", app_id).execute()


            self.client.table("NzymeRecruitingProcessHistory").insert({
                "application_id": app_id,
                "from_stage": old_stage,
                "to_stage": new_stage
            }).execute()
            self.logger.info(f"Stage change recorded: {old_stage} -> {new_stage}")
            return True
        except Exception as e:
            self.logger.error(f"Error recording stage change: {e}")
            return False


    # --- REFACTORED METHODS ---


    def update_rejection_reason(self, notion_page_id, reason, outcome_type):
        """
        Updates the rejection/closure reason in the corresponding application.
        Searches by notion_page_id (which is the candidate's page ID in the Workflow).
        """
        try:
            # 1. Search for the application by its Notion ID
            res = self.client.table("NzymeRecruitingApplications").select("id").eq("notion_page_id", notion_page_id).execute()

            if not res.data:
                self.logger.warning(f"No SQL application found for Notion ID: {notion_page_id}")
                return False

            app_id = res.data[0]['id']


            # 2. Update the row with the reason and type
            update_data = {
                "rejection_reason": f"[{outcome_type}] {reason}",
                "updated_at": "now()"
            }

            self.client.table("NzymeRecruitingApplications").update(update_data).eq("id", app_id).execute()
            self.logger.info(f"Rejection reason saved for App ID {app_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving rejection reason: {e}")
            return False

    def resolve_candidate_identity(self, input_email, input_name):
        """
        Implements the 4 identity rules.
        Returns: (candidate_data_dict, notion_page_id) or (None, None)
        """
        email_clean = input_email.strip().lower() if input_email else None
        name_clean = input_name.strip() if input_name else None

        candidate = None


        # 1. SEARCH BY EMAIL (The absolute truth)
        self.logger.debug(f"resolve_candidate_identity: Rule 1 — email lookup (has_email={bool(email_clean)}, name_len={len(name_clean) if name_clean else 0})")
        if email_clean:
            res = self.client.table("NzymeTalentNetwork").select("*").ilike("email", email_clean).execute()
            if res.data:
                self.logger.debug(f"resolve_candidate_identity: Rule 1 matched — email hit, id={res.data[0].get('id', '')[:8]}...")
                return res.data[0], res.data[0].get("notion_page_id")

        # 2. SEARCH BY NAME (If no match by email)
        self.logger.debug(f"resolve_candidate_identity: Rule 2 — name lookup (name_prefix={name_clean[:2]+'***' if name_clean else None})")
        if name_clean:
            # 2a. Try exact (case-insensitive) match first
            res = self.client.table("NzymeTalentNetwork").select("*").ilike("name", name_clean).execute()

            # 2b. If no exact match, try accent-insensitive fuzzy search
            if not res.data:
                self.logger.debug("resolve_candidate_identity: no exact name match, falling back to fuzzy search")
                res.data = self._fuzzy_name_search(name_clean)

            if res.data:
                potential_match = res.data[0]
                db_email = potential_match.get("email")

                # RULE 1: If I bring email, and DB has a DIFFERENT email -> THEY ARE NOT THE SAME.
                if email_clean and db_email and email_clean != db_email.lower():
                    self.logger.debug("resolve_candidate_identity: Rule 3 — same name, different emails → treating as NEW")
                    self.logger.info("Identity conflict: Same name, different emails. Treating as NEW.")
                    return None, None

                # RULE 2: I don't bring email, DB has email -> THEY ARE THE SAME (Merge).
                # RULE 3: I bring email, DB doesn't have email -> THEY ARE THE SAME (Merge).
                # RULE 4: Neither has email -> THEY ARE THE SAME (Merge).
                self.logger.debug(f"resolve_candidate_identity: Rules 2/3/4 — name match, merging with id={potential_match.get('id', '')[:8]}...")

                return potential_match, potential_match.get("notion_page_id")

        self.logger.debug("resolve_candidate_identity: no match found → new candidate")
        return None, None

    @staticmethod
    def _normalize_name(name):
        """Strip accents and lowercase: 'Avelló' -> 'avello', 'Peña' -> 'pena'."""
        nfkd = unicodedata.normalize("NFKD", name)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

    def _fuzzy_name_search(self, input_name):
        """
        Accent-insensitive name search. Searches by first name token in DB,
        then compares normalized (unaccented) full names in Python.
        """
        tokens = input_name.split()
        if not tokens:
            return []

        first_token = tokens[0]
        self.logger.debug(f"_fuzzy_name_search: token_count={len(tokens)}, first_token_prefix={first_token[:2]+'***'}")
        res = self.client.table("NzymeTalentNetwork").select("*").ilike("name", f"{first_token}%").execute()
        if not res.data:
            self.logger.debug("_fuzzy_name_search: no candidates returned from DB")
            return []

        input_normalized = self._normalize_name(input_name)
        self.logger.debug(f"_fuzzy_name_search: input_normalized_len={len(input_normalized)}, evaluating {len(res.data)} candidate(s)")

        matches = []
        for row in res.data:
            db_normalized = self._normalize_name(row.get("name", ""))
            if db_normalized == input_normalized:
                self.logger.debug(f"_fuzzy_name_search: exact normalized match — id={row.get('id', '')[:8]}...")
                return [row]
            if db_normalized.startswith(input_normalized) or input_normalized.startswith(db_normalized):
                self.logger.debug(f"_fuzzy_name_search: partial match — id={row.get('id', '')[:8]}...")
                matches.append(row)
            else:
                self.logger.debug(f"_fuzzy_name_search: no match (db_normalized_len={len(db_normalized)})")

        return matches[:1]

    def get_process_by_name(self, process_name):
        """
        Searches for an active or archived process by its exact name.
        Useful for resolving destination when we only have the name from Notion.
        """
        try:
            response = self.client.table("NzymeRecruitingProcesses")\
                .select("*")\
                .eq("process_name", process_name)\
                .execute()


            if response.data and len(response.data) > 0:
                return response.data[0]

            return None


        except Exception as e:
            print(f"[Supabase] Error finding process by name '{process_name}': {e}")
            return None

    def get_candidate_by_notion_page_id(self, notion_page_id):
        """Queries NzymeTalentNetwork by notion_page_id. Returns full candidate row or None."""
        try:
            res = self.client.table("NzymeTalentNetwork").select("*").eq("notion_page_id", notion_page_id).execute()
            if res.data:
                self.logger.debug(f"get_candidate_by_notion_page_id: found candidate {res.data[0].get('id', '')[:8]}...")
                return res.data[0]
            self.logger.debug(f"get_candidate_by_notion_page_id: no candidate found for page {notion_page_id[:8]}...")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching candidate by notion_page_id: {e}")
            return None

    def get_applications_by_candidate_id(self, candidate_id):
        """
        Queries NzymeRecruitingApplications by candidate_id, ordered by created_at desc.
        Joins with NzymeRecruitingProcesses to get process_name and matrix_characteristics.
        Returns list of enriched application dicts (includes workflow notion_page_id).
        """
        try:
            apps_res = self.client.table("NzymeRecruitingApplications") \
                .select("*") \
                .eq("candidate_id", candidate_id) \
                .order("created_at", desc=True) \
                .execute()
            if not apps_res.data:
                self.logger.debug(f"get_applications_by_candidate_id: no applications for candidate {candidate_id[:8]}...")
                return []

            self.logger.debug(f"get_applications_by_candidate_id: {len(apps_res.data)} application(s) found for candidate {candidate_id[:8]}...")
            enriched = []
            for app in apps_res.data:
                process_id = app.get("process_id")
                if not process_id:
                    enriched.append(app)
                    continue
                proc_res = self.client.table("NzymeRecruitingProcesses") \
                    .select("process_name, matrix_characteristics") \
                    .eq("id", process_id) \
                    .execute()
                if proc_res.data:
                    app["process_name"] = proc_res.data[0].get("process_name")
                    app["matrix_characteristics"] = proc_res.data[0].get("matrix_characteristics")
                enriched.append(app)
            return enriched
        except Exception as e:
            self.logger.error(f"Error fetching applications for candidate {candidate_id}: {e}")
            return []

    def resolve_process_by_notion_db_id(self, database_id):
        """
        Finds an active process by any of its Notion DB ID columns.
        Checks: notion_workflow_id, notion_feedback_id, notion_form_id, notion_bulk_id.
        Only returns Open processes — closed process webhooks are intentionally ignored.
        """
        try:
            response = self.client.table("NzymeRecruitingProcesses").select("*").or_(
                f"notion_workflow_id.eq.{database_id},"
                f"notion_feedback_id.eq.{database_id},"
                f"notion_form_id.eq.{database_id},"
                f"notion_bulk_id.eq.{database_id}"
            ).eq("status", "Open").execute()
            if response.data:
                self.logger.debug(f"resolve_process_by_notion_db_id: matched process '{response.data[0].get('process_name')}' for db_id {database_id[:8]}...")
                return response.data[0]
            self.logger.debug(f"resolve_process_by_notion_db_id: no active process found for db_id {database_id[:8]}...")
            return None
        except Exception as e:
            self.logger.error(f"Error resolving process by DB ID: {e}")
            return None

    def resolve_application_by_outcome_db_id(self, database_id):
        """Finds an application by its Outcome Form DB ID."""
        try:
            res = self.client.table("NzymeRecruitingApplications") \
                .select("*") \
                .eq("notion_outcome_id", database_id) \
                .execute()
            if res.data:
                return res.data[0]
            return None
        except Exception as e:
            self.logger.error(f"Error resolving application by outcome DB ID: {e}")
            return None

    def update_application_outcome_id(self, application_id, outcome_db_id):
        """Stores the Outcome Form DB ID on an application."""
        try:
            self.client.table("NzymeRecruitingApplications") \
                .update({"notion_outcome_id": outcome_db_id}) \
                .eq("id", application_id).execute()
            return True
        except Exception as e:
            self.logger.error(f"Error updating outcome ID: {e}")
            return False

    def update_candidate_email(self, candidate_id: str, new_email: str) -> bool:
        """
        Updates candidate email in SQL column.
        Used for backfilling email when candidate was matched by name.
        Returns True on success.
        """
        try:
            self.client.table("NzymeTalentNetwork")\
                .update({"email": new_email, "updated_at": "now()"})\
                .eq("id", candidate_id)\
                .execute()
            self.logger.info(f"Email updated for candidate {candidate_id}: {new_email}")
            return True
        except Exception as e:
            self.logger.error(f"Error updating candidate email: {e}")
            return False
