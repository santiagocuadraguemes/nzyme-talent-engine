import sys
import os
import time
import json
import hashlib
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
from core.logger import get_logger

load_dotenv()

POLLING_INTERVAL = 3
MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
PROCESS_DASHBOARD_DB_ID = os.getenv("NOTION_PROCESS_DASHBOARD_DB_ID")
TEMP_FOLDER = "temp_downloads"

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
        self.content_cache = {}

    def get_search_window(self):
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(minutes=3)
        return start_time.isoformat()

    def obtener_cambios_notion(self, ds_id):
        iso_time = self.get_search_window()
        filtro = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": iso_time}
        }
        return self.notion.query_data_source(ds_id, filtro)

    def calcular_hash(self, data_dict):
        s = json.dumps(data_dict, sort_keys=True, default=str)
        return hashlib.md5(s.encode('utf-8')).hexdigest()

    def ha_cambiado_el_contenido(self, page_id, data_dict):
        nuevo_hash = self.calcular_hash(data_dict)
        ultimo_hash = self.content_cache.get(page_id)
        
        if nuevo_hash != ultimo_hash:
            self.content_cache[page_id] = nuevo_hash
            return True
        return False

    def descargar_archivo(self, url, nombre):
        clean_name = "".join([c for c in nombre if c.isalnum() or c in "._-"])
        path = os.path.join(TEMP_FOLDER, clean_name)
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
            return path
        except Exception as e:
            self.logger.error(f"[ERROR DESCARGA] {e}")
            return None

    # --- CAMBIO IMPORTANTE AQUÍ ---
    def detectar_y_enriquecer_cv(self, page):
        props = page["properties"]
        page_id = page["id"]
        
        cv_files = props.get("CV", {}).get("files", [])
        if not cv_files: return False

        archivo = cv_files[0]
        tipo = archivo.get("type")

        if tipo == "file":
            nombre_archivo = archivo.get("name", "cv.pdf")
            url_privada_notion = archivo.get("file", {}).get("url")
            
            self.logger.info(f"[REFERRAL DETECTADO] CV Nuevo encontrado: {nombre_archivo}")
            
            local_path = self.descargar_archivo(url_privada_notion, nombre_archivo)
            if not local_path: return False

            self.logger.info("-> Subiendo a Storage...")
            public_url = self.storage.subir_cv_desde_url(url_privada_notion, nombre_archivo)
            if not public_url: return False

            self.logger.info("-> Analizando con IA...")
            datos_ia = self.ai.procesar_cv(local_path)
            try: os.remove(local_path)
            except: pass
            
            if not datos_ia: return False

            # --- PRESERVAR DATOS EXISTENTES ---
            
            # 1. Historial
            current_history_tags = props.get("Process History", {}).get("multi_select", [])
            current_history = [t["name"] for t in current_history_tags]
            
            # 2. Last Process
            current_process_obj = props.get("Last Process Involved in", {}).get("select")
            current_process = current_process_obj["name"] if current_process_obj else "Referral/General"

            # 3. NUEVO: Team & Role (Lectura para preservación)
            # Leemos la nueva columna fusionada
            current_team_role_tags = props.get("Proposed Nzyme Team & Role", {}).get("multi_select", [])
            current_team_role = [t["name"] for t in current_team_role_tags]

            # Generamos payload pasando los datos a preservar
            props_update = NotionBuilder.build_candidate_payload(
                datos_ia,
                public_url,
                current_process,
                existing_history=current_history,
                existing_team_role=current_team_role # <--- Pasamos la lista para no borrarla
            )
            
            self.logger.info("-> Escribiendo datos extraídos en Notion...")
            res = self.notion.update_page(page_id, props_update)
            
            if res.status_code == 200:
                self.logger.info("Enriquecimiento completado.")
                return True
            else:
                self.logger.error(f"[ERROR NOTION UPDATE] {res.status_code}: {res.text}")
                return False
            
        return False

    def vigilar_workflows(self):
        procesos = self.supa.obtener_procesos_activos()
        if not procesos: return

        for proc in procesos:
            wf_db_id = proc["notion_workflow_id"]
            ds_id = self.notion.get_data_source_id(wf_db_id)
            if not ds_id: continue

            paginas = self.obtener_cambios_notion(ds_id)
            for p in paginas:
                self.procesar_cambio_stage(p)

    def procesar_cambio_stage(self, page):
        page_id = page["id"]
        props = page["properties"]
        
        current_stage = None
        stage_prop = props.get("Stage", {})
        if stage_prop.get("type") == "select" and stage_prop.get("select"):
            current_stage = stage_prop["select"]["name"]
        
        if not current_stage: return

        data_relevante = {"stage": current_stage}
        if not self.ha_cambiado_el_contenido(page_id, data_relevante):
            return 

        app_record = self.supa.obtener_aplicacion_por_notion_id(page_id)
        if not app_record: return

        if app_record["current_stage"] != current_stage:
            self.logger.info(f"[WORKFLOW] Stage change: {app_record['current_stage']} -> {current_stage}")
            self.supa.registrar_cambio_stage(app_record["id"], app_record["current_stage"], current_stage)

    def vigilar_main_db(self):
        if not self.main_ds_id: return

        paginas = self.obtener_cambios_notion(self.main_ds_id)
        
        for p in paginas:
            # 1. Enriquecimiento
            se_ha_enriquecido = self.detectar_y_enriquecer_cv(p)
            if se_ha_enriquecido:
                continue

            # 2. Sincronización Estándar
            self.sincronizar_perfil(p)

    def sincronizar_perfil(self, page):
        page_id = page["id"]
        props = page["properties"]
        
        data_update = NotionParser.parse_candidate_properties(props)
        
        if not self.ha_cambiado_el_contenido(page_id, data_update):
            return 

        name = data_update.get("name", "Desconocido")
        self.logger.info(f"[MAIN DB] Cambio detectado en: {name}")
        self.supa.gestion_candidato(data_update, page_id)

    def vigilar_dashboard(self):
        if not self.dashboard_ds_id: return
        paginas = self.obtener_cambios_notion(self.dashboard_ds_id)
        for p in paginas: self.sincronizar_estado_proceso(p)

    def sincronizar_estado_proceso(self, page):
        page_id = page["id"]
        props = page["properties"]
        try:
            name_prop = props.get("Name", {}).get("title", [])
            if not name_prop: return
            name = name_prop[0]["plain_text"]

            status_prop = props.get("Open/Closed") or props.get("Status")
            
            new_status = None
            if status_prop:
                if status_prop["type"] == "select" and status_prop["select"]:
                    new_status = status_prop["select"]["name"]
                elif status_prop["type"] == "status" and status_prop["status"]:
                    new_status = status_prop["status"]["name"]
            
            if not new_status: return
        except Exception as e: return

        data_relevante = {"status": new_status, "name": name}
        if not self.ha_cambiado_el_contenido(page_id, data_relevante):
            return

        self.logger.info(f"[DASHBOARD] Estado proceso '{name}' -> {new_status}")
        self.supa.actualizar_estado_proceso_por_nombre(name, new_status)

    def run(self):
        self.logger.info("\n--- OBSERVER ---")
        self.logger.info(f"Polling: {POLLING_INTERVAL}s. Estrategia: Content Hash + Auto-Enrichment.")
        
        try:
            while True:
                try: self.vigilar_workflows()
                except Exception as e: self.logger.error(f"[ERR WORKFLOWS] {e}", exc_info=True)

                try: self.vigilar_main_db()
                except Exception as e: self.logger.error(f"[ERR MAIN DB] {e}", exc_info=True)

                try: self.vigilar_dashboard()
                except Exception as e: self.logger.error(f"[ERR DASHBOARD] {e}", exc_info=True)
                
                print(".", end="", flush=True) # Este lo dejamos como print para el latido visual simple
                time.sleep(POLLING_INTERVAL)
        except KeyboardInterrupt: self.logger.info("\nObserver detenido.")

if __name__ == "__main__":
    n_client = NotionClient()
    s_client = SupabaseManager()
    st_client = StorageClient()
    ai_agent = AnalizadorCV()

    obs = Observer(
        notion_client=n_client,
        supa_client=s_client,
        storage_client=st_client,
        ai_analyzer=ai_agent
    )
    
    obs.run()