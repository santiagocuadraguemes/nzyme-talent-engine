import os
from supabase import create_client, Client
from dotenv import load_dotenv
from core.logger import get_logger # <--- Integración de Logger

load_dotenv()

class SupabaseManager:
    def __init__(self):
        self.logger = get_logger("SupabaseManager") # <--- Logger
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("Faltan credenciales de Supabase en .env")
        self.client: Client = create_client(url, key)

    # --- GESTIÓN DE PROCESOS ---
    def registrar_proceso(self, notion_wf_id, notion_form_id, nombre, tipo):
        data = {
            "notion_workflow_id": notion_wf_id,
            "notion_form_id": notion_form_id,
            "process_name": nombre,
            "process_type": tipo,
            "status": "Open"
        }
        try:
            self.client.table("processes").insert(data).execute()
            self.logger.info(f"Proceso registrado: {nombre}")
            return True
        except Exception as e:
            self.logger.error(f"Fallo al registrar proceso: {e}")
            return False

    def obtener_procesos_activos(self):
        try:
            response = self.client.table("processes").select("*").eq("status", "Open").execute()
            return response.data
        except Exception as e:
            self.logger.error(f"Error leyendo procesos activos: {e}")
            return []

    def actualizar_estado_proceso_por_nombre(self, nombre, nuevo_estado):
        """Actualiza el status (Open/Closed) basado en el nombre del proceso."""
        try:
            self.client.table("processes").update({
                "status": nuevo_estado,
                "updated_at": "now()"
            }).eq("process_name", nombre).execute()
            return True
        except Exception as e:
            self.logger.error(f"Fallo actualizando proceso: {e}")
            return False

    # --- GESTIÓN DE CANDIDATOS (IDENTITY ENGINE) ---
    def gestion_candidato(self, candidate_data, notion_page_id):
        """
        Lógica Blindada de Identidad:
        1. Busca si ya existe alguien con este 'notion_page_id'.
        2. Si no, busca si existe alguien con este 'email' (Legacy Match).
        3. Si encuentra -> Actualiza (y guarda el notion_id si no lo tenía).
        4. Si no -> Crea uno nuevo.
        """
        try:
            # A. Preparar datos
            candidate_data["notion_page_id"] = notion_page_id 
            email = candidate_data.get("email")
            
            # B. Buscar existente (OR Logic)
            query = self.client.table("Nzyme_Talent_Network").select("id")
            
            # Construimos filtro: notion_page_id = X ...
            or_filter = f"notion_page_id.eq.{notion_page_id}"
            # ... OR email = Y (Solo si hay email)
            if email:
                or_filter += f",email.eq.{email}"
            
            existing = query.or_(or_filter).execute()

            if existing.data:
                # C. UPDATE (Ya existe)
                cid = existing.data[0]['id']
                # Actualizamos todo
                self.client.table("Nzyme_Talent_Network").update(candidate_data).eq("id", cid).execute()
                return cid
            else:
                # D. INSERT (Nuevo)
                response = self.client.table("Nzyme_Talent_Network").insert(candidate_data).execute()
                if response.data: return response.data[0]['id']
                return None

        except Exception as e:
            self.logger.error(f"Fallo gestión candidato: {e}", exc_info=True)
            return None

    # --- GESTIÓN DE APLICACIONES ---
    def crear_aplicacion(self, candidate_uuid, notion_wf_id, notion_page_id, stage_inicial):
        try:
            proc_res = self.client.table("processes").select("id").eq("notion_workflow_id", notion_wf_id).execute()
            if not proc_res.data: return False
            process_uuid = proc_res.data[0]['id']

            app_data = {
                "candidate_id": candidate_uuid,
                "process_id": process_uuid,
                "notion_page_id": notion_page_id,
                "current_stage": stage_inicial,
                "status": "Active"
            }
            self.client.table("applications").upsert(
                app_data, on_conflict="candidate_id, process_id"
            ).execute()
            return True
        except Exception as e:
            self.logger.error(f"Fallo creando aplicación: {e}")
            return False

    # --- OBSERVER METHODS ---
    def obtener_aplicacion_por_notion_id(self, notion_page_id):
        try:
            res = self.client.table("applications").select("id, current_stage").eq("notion_page_id", notion_page_id).execute()
            if res.data: return res.data[0]
            return None
        except: return None

    def registrar_cambio_stage(self, app_id, old_stage, new_stage):
        try:
            self.client.table("applications").update({
                "current_stage": new_stage, 
                "updated_at": "now()"
            }).eq("id", app_id).execute()

            self.client.table("process_history").insert({
                "application_id": app_id,
                "from_stage": old_stage,
                "to_stage": new_stage
            }).execute()
            self.logger.info(f"Movimiento registrado: {old_stage} -> {new_stage}")
            return True
        except Exception as e:
            self.logger.error(f"Error registrando cambio de stage: {e}")
            return False

    # --- MÉTODOS REFACTORIZADOS ---

    def get_candidate_id_by_email(self, email):
        """
        Busca el UUID del candidato por su email.
        Incluye limpieza de espacios (strip) para seguridad.
        """
        if not email: return None
        try:
            email_clean = email.strip()
            res = self.client.table("Nzyme_Talent_Network").select("id").eq("email", email_clean).execute()
            if res.data: return res.data[0]['id']
            return None
        except Exception: 
            return None

    # El método 'get_candidate_id_smart' ha sido eliminado por redundancia.

    def upsert_reference(self, ref_data):
        """Crea o actualiza una referencia en SQL."""
        try:
            # 1. Intentamos buscar si ya existe (para hacer update)
            existing = None
            
            # A. Buscamos por ID Maestro (Input original)
            if ref_data.get("master_notion_id"):
                existing = self.client.table("candidate_references").select("id").eq("master_notion_id", ref_data["master_notion_id"]).execute()
            
            # B. Si no, buscamos por ID Hijo (Update del reclutador)
            if not (existing and existing.data) and ref_data.get("child_notion_id"):
                existing = self.client.table("candidate_references").select("id").eq("child_notion_id", ref_data["child_notion_id"]).execute()

            if existing and existing.data:
                # UPDATE
                rid = existing.data[0]['id']
                self.client.table("candidate_references").update(ref_data).eq("id", rid).execute()
                self.logger.info(f"Referencia actualizada (ID: {rid})")
            else:
                # INSERT
                self.client.table("candidate_references").insert(ref_data).execute()
                self.logger.info("Referencia nueva creada.")
        except Exception as e:
            self.logger.error(f"Error gestionando referencia: {e}")