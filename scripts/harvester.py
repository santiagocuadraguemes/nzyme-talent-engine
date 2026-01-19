import sys
import os
import time
import re
import requests
import json
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import AnalizadorCV
from core.notion_builder import NotionBuilder
from core.domain_mapper import DomainMapper
from core.constants import PROP_CHECKBOX_PROCESSED, PROP_ID, PROP_NAME, PROP_CV_FILES, PROP_STAGE
from core.logger import get_logger

load_dotenv()

MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
POLLING_INTERVAL = 10 
TEMP_FOLDER = "temp_downloads"

if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)

class HarvesterRelational:
    def __init__(self, notion_client, supa_client, storage_client, ai_analyzer):
        self.logger = get_logger("Harvester")
        self.notion = notion_client
        self.supa_manager = supa_client
        self.storage = storage_client
        self.ai = ai_analyzer
        
        self.logger.info("Resolviendo Data Source Madre (Talent Network)...")
        self.main_ds_id = self.notion.get_data_source_id(MAIN_DB_ID) or MAIN_DB_ID

    # --- UTILIDADES ---
    def descargar_archivo_temporal(self, url, nombre):
        clean_name = "".join([c for c in nombre if c.isalnum() or c in "._-"])
        path = os.path.join(TEMP_FOLDER, clean_name)
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
            return path
        except: return None

    # --- LOGICA DE BÚSQUEDA INTELIGENTE ---
    def buscar_candidato_smart(self, email, nombre):
        """
        Busca si el candidato ya existe en Notion para decidir si crear o actualizar.
        1. Si hay email -> Buscar por Email (Prioridad Alta).
        2. Si no -> Buscar por Nombre (Prioridad Media).
        """
        # A. Búsqueda por Email
        if email:
            res = self.notion.query_data_source(self.main_ds_id, {"property": "Email", "email": {"equals": email}})
            if res: return res[0] # Match seguro
        
        # B. Búsqueda por Nombre
        res = self.notion.query_data_source(self.main_ds_id, {"property": "Name", "title": {"equals": nombre}})
        if res:
            candidate_page = res[0]
            existing_email = candidate_page["properties"].get("Email", {}).get("email")
            
            # Validación de Seguridad:
            if not existing_email:
                return candidate_page # Match (Candidato antiguo sin email)
            
            if email and existing_email == email:
                return candidate_page # Match (Emails coinciden)
                
            # Si nombres coinciden pero emails son distintos -> SON DISTINTOS
            self.logger.info(f"Homónimo detectado: '{nombre}' existe con otro email. Creando nuevo.")
            return None
        
        return None

    def determinar_stage_inicial(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        opciones = schema.get("Stage", {}).get("select", {}).get("options", [])
        if not opciones: opciones = schema.get("Stage", {}).get("status", {}).get("options", [])
        if not opciones: return None
        return opciones[0]["name"] if opciones else None

    def encontrar_propiedad_relacion(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        for name, details in schema.items():
            if details["type"] == "relation": return name
        return "Candidate Relation"

    def encontrar_propiedad_unique_id(self, ds_id):
        schema = self.notion.get_database_schema(ds_id)
        for name, details in schema.items():
            if details["type"] == "unique_id": return name
        return "ID"

    def buscar_cv_en_auxiliar(self, aux_db_id, id_texto):
        numeros = re.findall(r'\d+', str(id_texto))
        if not numeros: return None, None
        id_numerico = int(numeros[-1])

        ds_aux = self.notion.get_data_source_id(aux_db_id)
        if not ds_aux: return None, None
        
        col_id_name = self.encontrar_propiedad_unique_id(ds_aux)
        filtro = {"property": col_id_name, "unique_id": {"equals": id_numerico}}
        
        res = self.notion.query_data_source(ds_aux, filtro)
        if not res: return None, None
        
        files = res[0]["properties"].get("CV", {}).get("files", [])
        if not files: return None, None
        
        return files[0]["file"]["url"], files[0]["name"]

    # --- PROCESAMIENTO ---
    def procesar_candidato(self, cand, process_entry, relation_col_name, stage_inicial):
        page_id = cand["id"]
        props = cand["properties"]
        wf_db_id = process_entry["notion_workflow_id"]
        aux_db_id = process_entry["notion_form_id"]
        
        # Extracción de datos del proceso
        process_name_actual = process_entry["process_name"]
        process_type_actual = process_entry.get("process_type") 
        
        # Usamos constante PROP_ID
        id_text = props.get(PROP_ID, {}).get("rich_text", [])[0]["plain_text"] if props.get(PROP_ID, {}).get("rich_text", []) else ""
        self.logger.info(f"Procesando ID Enlace: {id_text}...")

        # 1. Obtener CV
        notion_url, file_name = self.buscar_cv_en_auxiliar(aux_db_id, id_text)
        if not notion_url:
            self.logger.warning("CV no encontrado. Esperando...")
            return

        local_path = self.descargar_archivo_temporal(notion_url, file_name)
        if not local_path: return

        # 2. Subir a Storage
        public_url = self.storage.subir_cv_desde_url(notion_url, file_name)
        if not public_url: return

        # 3. Analizar con IA
        self.logger.info("Analizando con IA...")
        datos_ia = self.ai.procesar_cv(local_path)
        try: os.remove(local_path)
        except: pass
        if not datos_ia: return

        # --- 4. GESTIÓN NOTION MAIN DB ---
        candidato_existente = self.buscar_candidato_smart(datos_ia.get("email"), datos_ia["name"])
        
        historial_previo = []
        team_role_previo = []
        id_madre_notion = None

        if candidato_existente:
            id_madre_notion = candidato_existente["id"]
            
            raw_history = candidato_existente["properties"].get("Process History", {}).get("multi_select", [])
            historial_previo = [tag["name"] for tag in raw_history]
            
            raw_team_role = candidato_existente["properties"].get("Proposed Nzyme Team & Role", {}).get("multi_select", [])
            team_role_previo = [tag["name"] for tag in raw_team_role]
            
            self.logger.info(f"Historial recuperado: {len(historial_previo)} procesos.")

        # C. Generar Payload
        props_madre = NotionBuilder.build_candidate_payload(
            datos_ia, 
            public_url, 
            process_name_actual, 
            existing_history=historial_previo,
            process_type=process_type_actual,   
            existing_team_role=team_role_previo 
        )
        
        # D. Ejecución en Notion
        error_madre = False
        if not id_madre_notion:
            self.logger.info(f"Creando candidato: {datos_ia['name']}")
            # FIX APLICADO: MAIN_DB_ID en vez de self.main_ds_id
            res_create = self.notion.create_page_in_db(MAIN_DB_ID, props_madre) 
            if res_create.status_code == 200: 
                id_madre_notion = res_create.json()["id"]
            else: 
                self.logger.error(f"[ERROR NOTION CREATE] {res_create.status_code}: {res_create.text}")
                error_madre = True
                return 
        else:
            self.logger.info(f"Actualizando candidato: {datos_ia['name']}")
            res_update = self.notion.update_page(id_madre_notion, props_madre)
            if res_update.status_code != 200:
                self.logger.error(f"[ERROR NOTION UPDATE] {res_update.status_code}: {res_update.text}")
                error_madre = True
                return 

        # 5. SUPABASE SYNC
        if not error_madre:
            datos_candidato = DomainMapper.map_to_supabase_candidate(datos_ia, public_url)
            uuid_candidato = self.supa_manager.gestion_candidato(datos_candidato, id_madre_notion)

            if uuid_candidato:
                self.supa_manager.crear_aplicacion(
                    uuid_candidato, 
                    wf_db_id, 
                    page_id, 
                    stage_inicial
                )

        # 6. CIERRE USANDO CONSTANTES
        update_props = {
            PROP_CHECKBOX_PROCESSED: {"checkbox": True},
            PROP_NAME: {"title": [{"text": {"content": datos_ia["name"]}}]},
            PROP_CV_FILES: {"files": [{"name": "CV.pdf", "external": {"url": public_url}}]}
        }
        if id_madre_notion:
            update_props[relation_col_name] = {"relation": [{"id": id_madre_notion}]}
        if stage_inicial:
            update_props[PROP_STAGE] = {"select": {"name": stage_inicial}}

        self.notion.update_page(page_id, update_props)
        self.logger.info("[OK] Finalizado.")

    def run(self):
        self.logger.info("--- HARVESTER ---")
        try:
            while True:
                procesos = self.supa_manager.obtener_procesos_activos()
                for proc in procesos:
                    wf_db_id = proc["notion_workflow_id"]
                    ds_wf = self.notion.get_data_source_id(wf_db_id)
                    if not ds_wf: continue

                    rel_col = self.encontrar_propiedad_relacion(ds_wf)
                    
                    # FILTRO USANDO CONSTANTES
                    filtro = {
                        "and": [
                            {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}},
                            {"property": PROP_ID, "rich_text": {"is_not_empty": True}}
                        ]
                    }
                    candidatos = self.notion.query_data_source(ds_wf, filtro)
                    
                    if candidatos:
                        self.logger.info(f"--> Detectados {len(candidatos)} en '{proc['process_name']}'")
                        stage_init = self.determinar_stage_inicial(ds_wf)
                        for cand in candidatos:
                            self.procesar_candidato(cand, proc, rel_col, stage_init)
                time.sleep(POLLING_INTERVAL)
        except KeyboardInterrupt: self.logger.info("Bye.")

if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()
    client_storage = StorageClient()
    analyzer_ai = AnalizadorCV()

    bot = HarvesterRelational(
        notion_client=client_notion,
        supa_client=client_supa,
        storage_client=client_storage,
        ai_analyzer=analyzer_ai
    )
    
    bot.run()