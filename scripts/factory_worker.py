# factory_worker.py
import sys
import os
import time
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

    def buscar_solicitudes_pendientes(self):
        """Searches for pages created by the button (Ready=True, Processed=False)."""
        if not self.dashboard_ds_id: 
            return []
        
        filtro = {
            "and": [
                {"property": PROP_READY_TO_PROCESS, "checkbox": {"equals": True}},
                {"property": PROP_PROCESSED_DASHBOARD, "checkbox": {"equals": False}}
            ]
        }
        return self.notion.query_data_source(self.dashboard_ds_id, filtro)

    def configurar_proceso(self, pagina_padre):
        page_id = pagina_padre["id"]
        props = pagina_padre["properties"]
        
        # 1. Extract Data from Dashboard using CONSTANTS
        try:
            raw_title = props.get(PROP_NAME, {}).get("title", [])
            if not raw_title: 
                return
            nombre_proceso = raw_title[0]["plain_text"]
            
            raw_select = props.get(PROP_PROCESS_TYPE, {}).get("select")
            if not raw_select: 
                return 
            tipo_proceso = raw_select["name"]
            
            es_portco = "PortCo" in tipo_proceso
            
            self.logger.info(f"Configurando: {nombre_proceso} ({tipo_proceso})")
        except Exception as e:
            self.logger.error(f"Error extrayendo datos: {e}", exc_info=True)
            return

        # 2. Safety wait
        time.sleep(5) 

        # 3. Identify child Databases and Pages
        bloques = self.notion.get_page_blocks(page_id)
        
        wf_db_id = None
        form_db_id = None
        bulk_db_id = None 
        feedback_db_id = None
        jd_page_id = None

        for b in bloques:
            if b["type"] == "child_database":
                titulo = b.get("child_database", {}).get("title", "").lower()
                bid = b["id"]
                
                if "workflow" in titulo:
                    wf_db_id = bid
                
                elif "feedback" in titulo:
                    feedback_db_id = bid
                    
                elif "form" in titulo:
                    form_db_id = bid
                
                elif "bulk" in titulo or "import" in titulo:
                    bulk_db_id = bid
                    
            elif b["type"] == "child_page":
                titulo = b.get("child_page", {}).get("title", "").lower()
                bid = b["id"]
                if "job" in titulo or "role" in titulo:
                    jd_page_id = bid

        if not wf_db_id or not form_db_id:
            self.logger.critical("No se encontraron las DBs hijas principales (Workflow/Form). Revisa el template.")
            return

        # 4. Get Stages from Guidelines
        doc_guidelines = self.parser.buscar_documento_guidelines(tipo_proceso)
        opciones_stages = []
        if doc_guidelines:
            opciones_stages = self.parser.parsear_stages_desde_pagina(doc_guidelines["id"])
            
            ZWSP = chr(0x200B)
            for i, stage in enumerate(opciones_stages):
                stage["name"] = ZWSP * i + stage["name"]

        # --- 5. CONFIGURE WORKFLOW ---
        self.notion.update_database(wf_db_id, title=f"Feedback Tool & Workflow - {nombre_proceso}")
        
        if opciones_stages:
            wf_ds_id = self.notion.get_data_source_id(wf_db_id)
            if wf_ds_id:
                wf_updates = {"Stage": {"select": {"options": opciones_stages}}}
                self.notion.update_data_source(wf_ds_id, properties=wf_updates)

        # --- 6. CONFIGURE FORM DB ---
        self.notion.update_database(form_db_id, title=f"Single Candidate Application Upload Form - {nombre_proceso}")

        # --- CONFIGURE BULK QUEUE ---
        if bulk_db_id:
            self.notion.update_database(bulk_db_id, title=f"Bulk Candidate Application Upload Form - {nombre_proceso}")

        # --- CONFIGURE FEEDBACK FORM ---
        if feedback_db_id:
            self.notion.update_database(feedback_db_id, title=f"Bulk & Single Feedback Upload Form - {nombre_proceso}")

        # --- 7. CONFIGURE JOB DESCRIPTION ---
        if jd_page_id:
            doc_jd = self.parser.buscar_documento_job_description(tipo_proceso)
            if doc_jd:
                contenido_bloques = self.parser.extraer_contenido_pagina(doc_jd["id"])
                if contenido_bloques:
                    bloques_existentes = self.notion.get_page_blocks(jd_page_id)
                    id_ancla = bloques_existentes[0]["id"] if bloques_existentes else None
                    
                    self.notion.append_block_children(jd_page_id, contenido_bloques[:100], after=id_ancla)
            
            nuevo_titulo_jd = f"Role & Candidate Description - {nombre_proceso}" if es_portco else f"Job Description - {nombre_proceso}"
            self.notion.update_page(jd_page_id, properties={"title": [{"text": {"content": nuevo_titulo_jd}}]})

        # --- 8. REGISTER IN SUPABASE (BACKEND) ---
        exito_supa = self.supa.registrar_proceso(
            wf_db_id, 
            form_db_id, 
            bulk_db_id, 
            feedback_db_id, 
            nombre_proceso, 
            tipo_proceso
        )

        # 9. CLOSE
        if exito_supa:
            self.notion.update_page(page_id, properties={PROP_PROCESSED_DASHBOARD: {"checkbox": True}})
            self.logger.info("Proceso completado")
        else:
            self.logger.error("Error al registrar en Supabase")

    def run_once(self):
        self.logger.info("FactoryWorker iniciando")
        self._init_datasources()
        
        solicitudes = self.buscar_solicitudes_pendientes()
        if not solicitudes:
            self.logger.info("Sin solicitudes pendientes")
            return

        self.logger.info(f"Procesando {len(solicitudes)} solicitudes")
        for sol in solicitudes:
            self.configurar_proceso(sol)
        
        self.logger.info("Ejecución completada")


if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()
    parser_guidelines = GuidelinesParser(client_notion)
    
    worker = FactoryWorkerV2(client_notion, client_supa, parser_guidelines)
    worker.run_once()
