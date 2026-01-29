# core/supabase_client.py

import os
from supabase import create_client, Client
from dotenv import load_dotenv
from core.logger import get_logger # <--- Logger integration


load_dotenv()


class SupabaseManager:
    def __init__(self):
        self.logger = get_logger("SupabaseManager") # <--- Logger
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("Faltan credenciales de Supabase en .env")
        self.client: Client = create_client(url, key)


    # --- PROCESS MANAGEMENT ---
    def registrar_proceso(self, notion_wf_id, notion_form_id, bulk_id, feedback_id, nombre, tipo):
        data = {
            "notion_workflow_id": notion_wf_id,
            "notion_form_id": notion_form_id,
            "notion_bulk_id": bulk_id,
            "notion_feedback_id": feedback_id,
            "process_name": nombre,
            "process_type": tipo,
            "status": "Open"
        }
        try:
            self.client.table("NzymeRecruitingProcesses").insert(data).execute()
            self.logger.info(f"Proceso registrado: {nombre}")
            return True
        except Exception as e:
            self.logger.error(f"Fallo al registrar proceso: {e}")
            return False


    def obtener_procesos_activos(self):
        try:
            response = self.client.table("NzymeRecruitingProcesses").select("*").eq("status", "Open").execute()
            return response.data
        except Exception as e:
            self.logger.error(f"Error leyendo procesos activos: {e}")
            return []


    def actualizar_estado_proceso_por_nombre(self, nombre, nuevo_estado):
        """Updates the status (Open/Closed) based on the process name."""
        try:
            self.client.table("NzymeRecruitingProcesses").update({
                "status": nuevo_estado,
                "updated_at": "now()"
            }).eq("process_name", nombre).execute()
            return True
        except Exception as e:
            self.logger.error(f"Fallo actualizando proceso: {e}")
            return False


    # --- CANDIDATE MANAGEMENT (IDENTITY ENGINE) ---
    def gestion_candidato(self, candidate_data, notion_page_id):
        """
        Bulletproof Identity Logic:
        1. Maps explicit SQL columns (creator, source, assessment).
        2. Saves the rest in JSONB (candidate_data).
        3. Performs search by Notion ID or Email to decide Update/Insert.
        """
        try:
            # A. Prepare explicit SQL payload
            # Build the exact dictionary that the Supabase table expects
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
                
                "candidate_data": candidate_data.get("candidate_data"), # The JSON with experience, education, etc.
                "notion_page_id": notion_page_id,
                "updated_at": "now()"
            }
            
            # B. Search for existing (OR Logic)
            email = candidate_data.get("email")
            
            query = self.client.table("NzymeTalentNetwork").select("id")
            
            # Build filter: notion_page_id = X ...
            or_filter = f"notion_page_id.eq.{notion_page_id}"
            
            # ... OR email = Y (Only if there's valid email, cleaning spaces)
            if email:
                email_clean = email.strip()
                or_filter += f",email.eq.{email_clean}"
            
            existing = query.or_(or_filter).execute()


            if existing.data:
                # C. UPDATE (Already exists)
                cid = existing.data[0]['id']
                self.client.table("NzymeTalentNetwork").update(payload).eq("id", cid).execute()
                return cid
            else:
                # D. INSERT (New)
                response = self.client.table("NzymeTalentNetwork").insert(payload).execute()
                if response.data: return response.data[0]['id']
                return None


        except Exception as e:
            self.logger.error(f"Fallo gestión candidato: {e}", exc_info=True)
            return None


    # --- APPLICATION MANAGEMENT ---
    def crear_aplicacion(self, candidate_uuid, notion_wf_id, notion_page_id, stage_inicial):
        try:
            proc_res = self.client.table("NzymeRecruitingProcesses").select("id").eq("notion_workflow_id", notion_wf_id).execute()
            if not proc_res.data: return False
            process_uuid = proc_res.data[0]['id']


            app_data = {
                "candidate_id": candidate_uuid,
                "process_id": process_uuid,
                "notion_page_id": notion_page_id,
                "current_stage": stage_inicial,
                "status": "Active"
            }
            self.client.table("NzymeRecruitingApplications").upsert(
                app_data, on_conflict="candidate_id, process_id"
            ).execute()
            return True
        except Exception as e:
            self.logger.error(f"Fallo creando aplicación: {e}")
            return False


    # --- OBSERVER METHODS ---
    def obtener_aplicacion_por_notion_id(self, notion_page_id):
        try:
            res = self.client.table("NzymeRecruitingApplications").select("id, current_stage").eq("notion_page_id", notion_page_id).execute()
            if res.data: return res.data[0]
            return None
        except: return None


    def registrar_cambio_stage(self, app_id, old_stage, new_stage):
        try:
            self.client.table("NzymeRecruitingApplications").update({
                "current_stage": new_stage, 
                "updated_at": "now()"
            }).eq("id", app_id).execute()


            self.client.table("NzymeRecruitingProcessHistory").insert({
                "application_id": app_id,
                "from_stage": old_stage,
                "to_stage": new_stage
            }).execute()
            self.logger.info(f"Movimiento registrado: {old_stage} -> {new_stage}")
            return True
        except Exception as e:
            self.logger.error(f"Error registrando cambio de stage: {e}")
            return False


    # --- REFACTORED METHODS ---


    def get_candidate_id_by_email(self, email):
        """
        Searches for candidate UUID by email.
        Includes space cleaning (strip) for safety.
        """
        if not email: return None
        try:
            email_clean = email.strip()
            res = self.client.table("NzymeTalentNetwork").select("id").eq("email", email_clean).execute()
            if res.data: return res.data[0]['id']
            return None
        except Exception: 
            return None


    # The 'get_candidate_id_smart' method has been removed due to redundancy.


    def upsert_reference(self, ref_data):
        """Creates or updates a reference in SQL."""
        try:
            # 1. Try to find if it already exists (to do update)
            existing = None
            
            # A. Search by Master ID (Original input)
            if ref_data.get("master_notion_id"):
                existing = self.client.table("NzymeRecruitingCandidateReferences").select("id").eq("master_notion_id", ref_data["master_notion_id"]).execute()
            
            # B. If not, search by Child ID (Recruiter's update)
            if not (existing and existing.data) and ref_data.get("child_notion_id"):
                existing = self.client.table("NzymeRecruitingCandidateReferences").select("id").eq("child_notion_id", ref_data["child_notion_id"]).execute()


            if existing and existing.data:
                # UPDATE
                rid = existing.data[0]['id']
                self.client.table("NzymeRecruitingCandidateReferences").update(ref_data).eq("id", rid).execute()
                self.logger.info(f"Referencia actualizada (ID: {rid})")
            else:
                # INSERT
                self.client.table("NzymeRecruitingCandidateReferences").insert(ref_data).execute()
                self.logger.info("Referencia nueva creada.")
        except Exception as e:
            self.logger.error(f"Error gestionando referencia: {e}")


    def obtener_paginas_activas_candidato_smart(self, email, nombre):
        """
        Searches for candidate by Email (Priority 1) or Name (Priority 2).
        Returns list of notion_page_id from their applications.
        """
        candidate_data = None
        
        # 1. Try to search by EMAIL (Cleaning spaces and lowercase)
        if email:
            email_clean = email.strip()
            try:
                # We use ilike or eq depending on Supabase config, eq is usually case-sensitive,
                # so better to search exact if we trust the data, or filter in python.
                res = self.client.table("NzymeTalentNetwork").select("*").eq("email", email_clean).execute()
                if res.data:
                    candidate_data = res.data[0]
            except Exception as e:
                self.logger.error(f"[Supabase] Error buscando por email: {e}")


        # 2. FALLBACK: Search by NAME (If no match by email)
        if not candidate_data and nombre:
            nombre_clean = nombre.strip()
            try:
                # Here's the key: We assume that if the name matches, it's the same person
                res = self.client.table("NzymeTalentNetwork").select("*").eq("name", nombre_clean).execute()
                if res.data:
                    candidate_data = res.data[0] # Take the first match
            except Exception as e:
                self.logger.error(f"[Supabase] Error buscando por nombre: {e}")


        if not candidate_data: return []


        # 3. Search for applications
        cid = candidate_data["id"]
        try:
            apps_res = self.client.table("NzymeRecruitingApplications").select("notion_page_id").eq("candidate_id", cid).execute()
            return [item["notion_page_id"] for item in apps_res.data if item.get("notion_page_id")]
        except: return []
        
    def actualizar_motivo_rechazo(self, notion_page_id, motivo, outcome_type):
        """
        Updates the rejection/closure reason in the corresponding application.
        Searches by notion_page_id (which is the candidate's page ID in the Workflow).
        """
        try:
            # 1. Search for the application by its Notion ID
            res = self.client.table("NzymeRecruitingApplications").select("id").eq("notion_page_id", notion_page_id).execute()
            
            if not res.data:
                self.logger.warning(f"No se encontró aplicación SQL para Notion ID: {notion_page_id}")
                return False
            
            app_id = res.data[0]['id']


            # 2. Update the row with the reason and type
            # We assume you created the 'rejection_reason' column in Supabase.
            # Optional: We could also save 'outcome_type' if you wanted a separate column,
            # but for simplicity we concatenate or save only the explanatory text.
            
            update_data = {
                "rejection_reason": f"[{outcome_type}] {motivo}", # Useful prefix for analysis
                "updated_at": "now()"
                # "status": "Closed" # We could close here, but Observer already syncs status if it changes in Notion
            }
            
            self.client.table("NzymeRecruitingApplications").update(update_data).eq("id", app_id).execute()
            self.logger.info(f"Motivo de rechazo guardado para App ID {app_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error guardando motivo rechazo: {e}")
            return False
        
    def resolver_identidad_candidato(self, input_email, input_name):
        """
        Implements the 4 identity rules.
        Returns: (candidate_data_dict, notion_page_id) or (None, None)
        """
        email_clean = input_email.strip().lower() if input_email else None
        name_clean = input_name.strip() if input_name else None
        
        candidate = None


        # 1. SEARCH BY EMAIL (The absolute truth)
        if email_clean:
            # Search exact by email
            res = self.client.table("NzymeTalentNetwork").select("*").ilike("email", email_clean).execute()
            if res.data:
                return res.data[0], res.data[0].get("notion_page_id")

        # 2. SEARCH BY NAME (If no match by email)
        if name_clean:
            res = self.client.table("NzymeTalentNetwork").select("*").ilike("name", name_clean).execute()
            
            if res.data:
                potential_match = res.data[0]
                db_email = potential_match.get("email")
                
                # RULE 1: If I bring email, and DB has a DIFFERENT email -> THEY ARE NOT THE SAME.
                if email_clean and db_email and email_clean != db_email.lower():
                    self.logger.info("Conflicto de identidad: Mismo nombre, distintos emails. Se trata como NUEVO.")
                    return None, None
                
                # RULE 2: I don't bring email, DB has email -> THEY ARE THE SAME (Merge).
                # RULE 3: I bring email, DB doesn't have email -> THEY ARE THE SAME (Merge).
                # RULE 4: Neither has email -> THEY ARE THE SAME (Merge).
                
                return potential_match, potential_match.get("notion_page_id")

        return None, None
    
    def obtener_proceso_por_nombre(self, process_name):
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
            # In production you could use self.logger.error if you have injected logger
            print(f"[Supabase] Error buscando proceso por nombre '{process_name}': {e}")
            return None
