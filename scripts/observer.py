import sys
import os
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import AnalizadorCV
from core.notion_parser import NotionParser
from core.notion_builder import NotionBuilder
from core.utils import descargar_archivo
from core.logger import get_logger
from core.constants import PROP_NAME, PROP_EMAIL, PROP_PHONE, PROP_LINKEDIN, PROP_CV_FILES



load_dotenv()



# --- CONFIGURACIÓN ---
LOOKBACK_MINUTES = 25 
MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
PROCESS_DASHBOARD_DB_ID = os.getenv("NOTION_PROCESS_DASHBOARD_DB_ID")
CENTRAL_REFS_DB_ID = os.getenv("NOTION_REFERENCES_DB_ID")
INTERNAL_REFS_DB_TITLE = "References to Check"
TEMP_FOLDER = "/tmp/temp_downloads"



if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)



class Observer:
    def __init__(self, notion_client, supa_client, storage_client, ai_analyzer):
        self.logger = get_logger("Observer")
        self.notion = notion_client
        self.supa = supa_client
        self.storage = storage_client
        self.ai = ai_analyzer



        self.main_ds_id = self.notion.get_data_source_id(MAIN_DB_ID) or MAIN_DB_ID
        self.dashboard_ds_id = self.notion.get_data_source_id(PROCESS_DASHBOARD_DB_ID) or PROCESS_DASHBOARD_DB_ID
        self.refs_ds_id = self.notion.get_data_source_id(CENTRAL_REFS_DB_ID) or CENTRAL_REFS_DB_ID



    # =========================================================================
    # 1. MOTORES DE VIGILANCIA (ENGINES)
    # =========================================================================



    def _engine_sniper(self, db_id, handler_func, context=None, label="SNIPER"):
        """
        MOTOR 1: Acceso directo por ID.
        CORRECCION: Filtra por (Creado > Tiempo) O (Editado > Tiempo).
        """
        if not db_id: return



        now = datetime.now(timezone.utc)
        start_time = (now - timedelta(minutes=LOOKBACK_MINUTES)).isoformat()


        filtro = {
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


        try:
            paginas = self.notion.query_data_source(db_id, filtro)
            if paginas:
                self.logger.info(f"[{label}] Procesando {len(paginas)} cambios en DB {db_id}")
                for page in paginas:
                    try:
                        handler_func(page, context)
                    except Exception as e:
                        self.logger.error(f"Error en handler {label}: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Fallo en motor Sniper (DB {db_id}): {e}")



    def _engine_radar(self, db_title_query, handler_func, label="RADAR"):
        """
        MOTOR 2 (STRICT MATCH): Data Source Hunter.
        Busca Data Sources por nombre, pero aplica un FILTRO EXACTO en Python
        para evitar errores 400 en bases de datos que no corresponden.
        """
        url = "https://api.notion.com/v1/search"
        payload = {
            "query": db_title_query,
            "filter": {"value": "data_source", "property": "object"},
            "page_size": 50
        }


        try:
            resp = requests.post(url, headers=self.notion.headers, json=payload)
            if resp.status_code != 200:
                self.logger.error(f"[{label}] Error API Search: {resp.status_code}")
                return


            results = resp.json().get("results", [])


            for ds in results:
                ds_id = ds["id"]

                ds_name = "Sin Nombre"
                if "title" in ds and ds["title"]:
                    ds_name = ds["title"][0]["plain_text"]
                elif "name" in ds: 
                    ds_name = ds["name"]

                if ds_name.strip() != db_title_query.strip():
                    continue


                if ds.get("archived", False): continue


                try:
                    filtro_pendientes = {
                        "property": "Processed",
                        "checkbox": {"equals": False}
                    }

                    filas = self.notion.query_data_source(ds_id, filtro_pendientes)

                except Exception:
                    continue


                if not filas: continue


                parent = ds.get("parent", {})
                p_type = parent.get("type")
                p_id = parent.get(p_type)


                candidate_id = self._find_candidate_ancestor(p_id, p_type)

                if candidate_id:
                    self.logger.info(f"[{label}] Procesando {len(filas)} entradas en '{ds_name}'")
                    context = {"candidate_id": candidate_id}

                    for page in filas:
                        try:
                            handler_func(page, context)
                        except Exception as e:
                            self.logger.error(f"Error handler fila: {e}")


        except Exception as e:
            self.logger.error(f"[{label}] Excepción crítica: {e}", exc_info=True)



    # =========================================================================
    # 2. HANDLERS (LÓGICA DE NEGOCIO PURA)
    # =========================================================================

    def _handle_main_candidate(self, page, _=None):
        """Lógica: Talent Network (Enriquecimiento CV + Sync Supabase + DISPATCHER)"""
        
        # 1. Chequeo de Asignación a Nuevo Proceso (Prioridad Alta)
        # Si el usuario seleccionó un proceso en la relación, movemos al candidato y paramos.
        props = page["properties"]
        assign_rel = props.get("Assign to Active Process", {}).get("relation", [])
        
        if assign_rel:
            process_dashboard_page_id = assign_rel[0]["id"]
            self.logger.info(f"[DISPATCH] Solicitud de movimiento detectada hacia página: {process_dashboard_page_id}")
            self._logic_dispatch_candidate_to_form(page, process_dashboard_page_id)
            return # Detenemos aquí para no hacer sync normal en este ciclo

        # 2. Lógica normal (Enriquecimiento y Sync)
        se_ha_enriquecido = self._logic_enrich_cv(page)
        if se_ha_enriquecido:
            return

        page_id = page["id"]
        data_update = NotionParser.parse_candidate_properties(props)
        self.supa.gestion_candidato(data_update, page_id)



    def _handle_process_dashboard(self, page, _=None):
        """Lógica: Dashboard (Actualizar estado Open/Closed)"""
        self.sincronizar_estado_proceso(page)



    def _handle_workflow_item(self, page, process_context):
        """Lógica: Workflow (Detectar cambio de Stage y registrar)"""
        page_id = page["id"]
        props = page["properties"]


        stage_prop = props.get("Stage", {})
        current_stage = None
        if stage_prop.get("select"): current_stage = stage_prop["select"]["name"]
        elif stage_prop.get("status"): current_stage = stage_prop["status"]["name"]


        if not current_stage: return



        app_record = self.supa.obtener_aplicacion_por_notion_id(page_id)
        if not app_record: return



        if app_record["current_stage"] != current_stage:
            self.logger.info(f"Stage change: {app_record['current_stage']} -> {current_stage}")
            self.supa.registrar_cambio_stage(app_record["id"], app_record["current_stage"], current_stage)



    def _handle_feedback_form(self, page, process_context):
        """
        Lógica: Feedback Externo (PDF -> AI -> Nota en Candidato).
        Usa Identity Engine para encontrar al candidato y cruzarlo con el proceso actual.
        """
        if page["properties"].get("Processed", {}).get("checkbox"): return



        props = page["properties"]
        form_id = page["id"]


        raw_name = props.get("Name", {}).get("title", [])
        interviewer_name = raw_name[0]["plain_text"] if raw_name else "Headhunter Externo"
        files = props.get("File", {}).get("files", [])
        if not files: return



        file_obj = files[0]
        file_url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")
        if not file_url: return



        local_path = descargar_archivo(file_url, file_obj["name"], TEMP_FOLDER)
        if not local_path: return



        feedback_data = self.ai.procesar_feedback_pdf(local_path)
        try: os.remove(local_path)
        except: pass



        if not feedback_data:
            return



        cand_name_ia = feedback_data["candidate_name"]


        cand_db, _ = self.supa.resolver_identidad_candidato(None, cand_name_ia)



        if not cand_db:
            self.logger.warning(f"Candidato '{cand_name_ia}' no encontrado para feedback")
            return



        try:
            res_app = self.supa.client.table("NzymeRecruitingApplications")\
                .select("notion_page_id, current_stage")\
                .eq("candidate_id", cand_db["id"])\
                .eq("process_id", process_context["id"])\
                .execute()


            if not res_app.data:
                return


            app_data = res_app.data[0]
            target_id = app_data["notion_page_id"]
            current_stage = app_data.get("current_stage")



        except Exception as e:
            self.logger.error(f"Error buscando aplicación: {e}")
            return



        gathered_db_id = self.notion.find_child_database(target_id, "Gathered Feedback")


        if gathered_db_id:
            payload = {
                "Interviewer": {"title": [{"text": {"content": f"{interviewer_name} - {current_stage}" if current_stage else interviewer_name}}]},
            }
            if current_stage: 
                payload["Stage"] = {"select": {"name": current_stage}}


            res_create = self.notion.create_page(gathered_db_id, payload)


            if res_create.status_code == 200:
                new_page_id = res_create.json()["id"]
                bloque_texto = {
                    "object": "block", "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "text": {"content": feedback_data["feedback_text"][:2000]}}]}
                }
                self.notion.append_block_children(new_page_id, [bloque_texto])


                self.notion.update_page(form_id, {"Processed": {"checkbox": True}})
                self.logger.info(f"Feedback sincronizado para '{cand_name_ia}'")
            else:
                self.logger.error(f"Error creando nota en Notion: {res_create.text}")



    def _handle_central_reference(self, page, _=None):
        """Lógica: Referencias Centrales con Identidad Estricta"""
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
            ref_name = r_name_obj[0]["plain_text"] if r_name_obj else "Desconocido"
            ctx_obj = props.get("Context", {}).get("rich_text", [])
            context = ctx_obj[0]["plain_text"] if ctx_obj else ""
            raw_rel = props.get("Relationship to Candidate", {}).get("multi_select", [])
            rel_list = [item["name"] for item in raw_rel]
            raw_timing = props.get("Timing of such relationship", {}).get("select")
            timing_val = raw_timing["name"] if raw_timing else None
            raw_outcome = props.get("Reference Outcome", {}).get("select")
            outcome_val = raw_outcome["name"] if raw_outcome else "To contact"


        except Exception: return



        cand_db, _ = self.supa.resolver_identidad_candidato(cand_email, cand_name)


        if not cand_db:
            self.logger.warning(f"Identidad no resuelta para referencia: '{cand_name or cand_email}'")
            return



        try:
            apps_res = self.supa.client.table("NzymeRecruitingApplications")\
                .select("notion_page_id")\
                .eq("candidate_id", cand_db["id"])\
                .eq("status", "Active")\
                .execute()
            app_page_ids = [item["notion_page_id"] for item in apps_res.data if item.get("notion_page_id")]
        except Exception as e:
            self.logger.error(f"Error buscando apps: {e}")
            return



        if not app_page_ids: return



        exito_global = True
        for app_pid in app_page_ids:
            child_db_id = self.notion.find_child_database(app_pid, INTERNAL_REFS_DB_TITLE)
            if child_db_id:
                rel_payload = [{"name": r, "color": "default"} for r in rel_list]
                payload = {
                    "Referrer Name": {"title": [{"text": {"content": ref_name}}]},
                    "Candidate Email": {"email": cand_email} if cand_email else None,
                    "Referrer Email": {"email": ref_email} if ref_email else None,
                    "Referrer Phone": {"phone_number": ref_phone} if ref_phone else None,
                    "Context": {"rich_text": [{"text": {"content": context}}]},
                    "Relationship to Candidate": {"multi_select": rel_payload} if rel_payload else None,
                    "Timing of such relationship": {"select": {"name": timing_val, "color": "default"}} if timing_val else None,
                    "Reference Outcome": {"select": {"name": outcome_val, "color": "default"}}
                }
                payload = {k: v for k, v in payload.items() if v is not None}
                res = self.notion.create_page(child_db_id, payload)
                if res.status_code != 200: exito_global = False
            else:
                exito_global = False



        if exito_global:
            self.notion.update_page(ref_page_id, {"Processed": {"checkbox": True}})
            self.logger.info(f"Referencia sincronizada para '{cand_name}'")



    def _handle_outcome_entry(self, page, context):
        """Lógica: Outcome Form (Fuzzy Match + Sync Reason)"""
        candidate_id = context.get("candidate_id")
        if not candidate_id: return


        props = page["properties"]
        page_id = page["id"]


        outcome_prop = props.get("Discarded/Disqualified/Lost", {}).get("select")
        outcome_val = outcome_prop["name"] if outcome_prop else None

        explanation_obj = props.get("Explanation", {}).get("rich_text", [])
        explanation_val = explanation_obj[0]["plain_text"] if explanation_obj else "No explanation provided"


        if not outcome_val: 
            return


        final_stage_name = self._fuzzy_match_stage(candidate_id, outcome_val)

        self.logger.info(f"Outcome match: '{outcome_val}' -> '{final_stage_name}'")


        payload_cand = {"Stage": {"select": {"name": final_stage_name}}}
        res_upd = self.notion.update_page(candidate_id, payload_cand)

        if res_upd.status_code == 200:
            self.supa.actualizar_motivo_rechazo(candidate_id, explanation_val, final_stage_name)
            self.notion.update_page(page_id, {"Processed": {"checkbox": True}})
            self.logger.info("Outcome procesado correctamente")
        else:
            self.logger.error(f"Fallo actualizando candidato: {res_upd.text}")



    # =========================================================================
    # 3. HELPERS ESPECÍFICOS (LOGIC SUPPORT)
    # =========================================================================



    def _logic_enrich_cv(self, page):
        """Lógica extraída de detectar_y_enriquecer_cv para limpieza"""
        props = page["properties"]
        cv_files = props.get("CV", {}).get("files", [])
        if not cv_files: return False



        archivo = cv_files[0]
        if archivo.get("type") != "file": return False



        nombre = archivo.get("name", "cv.pdf")
        url = archivo.get("file", {}).get("url")


        self.logger.info(f"CV nuevo detectado: {nombre}")
        local_path = descargar_archivo(url, nombre, TEMP_FOLDER)
        if not local_path: return False



        public_url = self.storage.subir_cv_desde_url(url, nombre)
        if not public_url: return False



        datos_ia = self.ai.procesar_cv(local_path)
        try: os.remove(local_path)
        except: pass


        if not datos_ia: return False



        curr_hist = [t["name"] for t in props.get("Process History", {}).get("multi_select", [])]
        curr_role = [t["name"] for t in props.get("Proposed Nzyme Team & Role", {}).get("multi_select", [])]
        curr_proc_obj = props.get("Last Process Involved in", {}).get("select")
        curr_proc = curr_proc_obj["name"] if curr_proc_obj else "Referral/General"



        payload = NotionBuilder.build_candidate_payload(
            datos_ia, public_url, curr_proc, 
            existing_history=curr_hist, existing_team_role=curr_role
        )


        self.notion.update_page(page["id"], payload)
        self.logger.info("Enriquecimiento completado")
        return True
    
    def _logic_dispatch_candidate_to_form(self, candidate_page, process_dashboard_page_id):
        """
        Mueve un candidato de Main DB al Formulario destino.
        CORRECCIÓN FINAL: Usa ID resuelto para Schema, pero ID crudo para Creación.
        """
        cand_id = candidate_page["id"]
        props = candidate_page["properties"]
        
        self.logger.info(f"[DISPATCH] --- INICIO DISPATCH (Candidate ID: {cand_id}) ---")

        # --- A. EXTRACCIÓN (Standard) ---
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
            self.logger.warning(f"[DISPATCH] El candidato '{val_name}' no tiene CV. Cancelando.")
            self.notion.update_page(cand_id, {"Assign to Active Process": {"relation": []}})
            return

        # --- B. RESOLUCIÓN DE DESTINO ---
        try:
            # 1. Obtener nombre del proceso
            url_proc = f"https://api.notion.com/v1/pages/{process_dashboard_page_id}"
            resp_proc = requests.get(url_proc, headers=self.notion.headers)
            
            if resp_proc.status_code != 200: 
                self.logger.error(f"[DISPATCH] Error dashboard: {resp_proc.status_code}")
                return

            proc_title_obj = resp_proc.json()["properties"].get("Name", {}).get("title", [])
            process_name = proc_title_obj[0]["plain_text"] if proc_title_obj else None
            
            if not process_name: 
                self.logger.error("[DISPATCH] Proceso sin nombre.")
                return

            self.logger.info(f"[DISPATCH] Destino: '{process_name}'")

            # 2. Obtener ID RAW desde Supabase
            proc_record = self.supa.obtener_proceso_por_nombre(process_name)
            if not proc_record:
                self.logger.error(f"[DISPATCH] Proceso no en Supabase.")
                self.notion.update_page(cand_id, {"Assign to Active Process": {"relation": []}})
                return
            
            raw_form_id = proc_record.get("notion_form_id")
            if not raw_form_id:
                self.logger.error(f"[DISPATCH] notion_form_id NULO.")
                return

            # --- C. ESQUEMA DINÁMICO (Aquí usamos el ID resuelto) ---
            
            # Resolvemos manualmente SOLO para leer las columnas (GET)
            target_ds_id_for_schema = self.notion.get_data_source_id(raw_form_id) or raw_form_id
            
            schema = self.notion.get_database_schema(target_ds_id_for_schema)
            valid_cols = set(schema.keys()) if schema else set()
            
            self.logger.info(f"[DISPATCH] Schema check OK. Columnas: {list(valid_cols)}")

            # --- D. CONSTRUCCIÓN PAYLOAD ---
            payload = {}

            # Title
            target_title_col = PROP_NAME
            if PROP_NAME not in valid_cols:
                # Fallback: buscar columna tipo title
                target_title_col = next((k for k, v in schema.items() if v["type"] == "title"), None)
            
            if target_title_col:
                payload[target_title_col] = {"title": [{"text": {"content": val_name}}]}
            else:
                self.logger.error("[DISPATCH] Destino sin columna Title.")
                return

            # Campos condicionales (chequeando valid_cols)
            if val_email and PROP_EMAIL in valid_cols:
                payload[PROP_EMAIL] = {"email": val_email}
            
            if val_phone and PROP_PHONE in valid_cols:
                payload[PROP_PHONE] = {"phone_number": val_phone}
                
            if val_linkedin and PROP_LINKEDIN in valid_cols:
                payload[PROP_LINKEDIN] = {"url": val_linkedin}

            if PROP_CV_FILES in valid_cols:
                payload[PROP_CV_FILES] = {
                    "files": [{
                        "name": val_cv_name, 
                        "type": "external", 
                        "external": {"url": val_cv_url}
                    }]
                }

            # --- E. INSERCIÓN (Aquí usamos el ID RAW) ---
            
            res = self.notion.create_page(raw_form_id, payload)
            
            if res.status_code == 200:
                self.logger.info(f"[DISPATCH] Éxito. Candidato enviado.")
                self.notion.update_page(cand_id, {"Assign to Active Process": {"relation": []}})
            else:
                # Log detallado del error real
                self.logger.error(f"[DISPATCH] Fallo create_page: {res.status_code} - {res.text}")

        except Exception as e:
            self.logger.error(f"[DISPATCH] Excepción: {e}", exc_info=True)

    def sincronizar_estado_proceso(self, page):
        """Helper para dashboard (Mantenido igual)"""
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
                self.supa.actualizar_estado_proceso_por_nombre(name, new_status)
        except: pass

    def _search_modified_databases(self, query_title):
        """Helper para Radar Engine CON DOBLE CHEQUEO (Created OR Edited)"""
        url = "https://api.notion.com/v1/search"
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=LOOKBACK_MINUTES)

        payload = {
            "query": query_title,
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            "page_size": 50 
        }

        try:
            resp = requests.post(url, headers=self.notion.headers, json=payload)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                filtered = []

                for db in results:
                    if db["object"] not in ["database", "data_source"]: continue

                    if db.get("archived", False): continue

                    le_str = db.get("last_edited_time", "").replace("Z", "+00:00")
                    cr_str = db.get("created_time", "").replace("Z", "+00:00")

                    is_recent = False

                    if le_str:
                        if datetime.fromisoformat(le_str) > start_time: is_recent = True
                    if not is_recent and cr_str:
                        if datetime.fromisoformat(cr_str) > start_time: is_recent = True
                    if is_recent:
                        filtered.append(db)

                return filtered
        except Exception as e:
            self.logger.error(f"Search Error: {e}")
        return []

    def _find_candidate_ancestor(self, starting_id, starting_type):
        """Helper para escalar jerarquía"""
        curr_id, curr_type = starting_id, starting_type

        for i in range(8):
            if not curr_id: return None

            if curr_type in ["database_id", "block_id"]:
                endpoint = "databases" if curr_type == "database_id" else "blocks"
                url = f"https://api.notion.com/v1/{endpoint}/{curr_id}"
                resp = requests.get(url, headers=self.notion.headers)
                if resp.status_code != 200: return None
                parent = resp.json().get("parent")
                if not parent: return None
                curr_type = parent["type"]
                curr_id = parent.get(curr_type)

            elif curr_type == "page_id":
                url = f"https://api.notion.com/v1/pages/{curr_id}"
                resp = requests.get(url, headers=self.notion.headers)
                if resp.status_code == 200:
                    props = resp.json().get("properties", {})
                    if "Stage" in props: return curr_id 

                    parent = resp.json().get("parent")
                    if not parent: return None
                    curr_type = parent["type"]
                    curr_id = parent.get(curr_type)
                else: return None
        return None

    def _fuzzy_match_stage(self, candidate_page_id, partial_text):
        """Helper para Outcome: Busca el stage real en el schema del padre"""
        try:
            url_cand = f"https://api.notion.com/v1/pages/{candidate_page_id}"
            resp_cand = requests.get(url_cand, headers=self.notion.headers)
            parent_db_id = resp_cand.json()["parent"]["database_id"]

            ds_id = self.notion.get_data_source_id(parent_db_id) or parent_db_id
            schema = self.notion.get_database_schema(ds_id)

            options = []
            if "select" in schema.get("Stage", {}): options = schema["Stage"]["select"]["options"]
            elif "status" in schema.get("Stage", {}): options = schema["Stage"]["status"]["options"]

            for opt in options:
                if partial_text in opt["name"]: return opt["name"]
        except: pass
        return partial_text
    # =========================================================================
    # 4. EJECUCIÓN PRINCIPAL (ORQUESTADOR)
    # =========================================================================
    def run_once(self):
        self.logger.info("Observer iniciando")
        active_processes = self.supa.obtener_procesos_activos() or []
        self.logger.info(f"Contexto: {len(active_processes)} procesos activos")
        self._engine_sniper(self.main_ds_id, self._handle_main_candidate, label="MAIN DB")
        self._engine_sniper(self.dashboard_ds_id, self._handle_process_dashboard, label="DASHBOARD")
        self._engine_sniper(self.refs_ds_id, self._handle_central_reference, label="CENTRAL REFS")
        for proc in active_processes:
            wf_raw_id = proc["notion_workflow_id"]
            if wf_raw_id:
                wf_final_id = self.notion.get_data_source_id(wf_raw_id) or wf_raw_id
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
        self.logger.info("Ejecución completada")

if __name__ == "__main__":
    n_client = NotionClient()
    s_client = SupabaseManager()
    st_client = StorageClient()
    ai_agent = AnalizadorCV()



    obs = Observer(n_client, s_client, st_client, ai_agent)
    obs.run_once()