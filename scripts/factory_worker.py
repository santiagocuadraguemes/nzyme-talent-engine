import sys
import os
import time
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.guidelines_parser import GuidelinesParser
from core.constants import PROP_READY_TO_PROCESS, PROP_PROCESSED_DASHBOARD, PROP_NAME, PROP_PROCESS_TYPE
from core.logger import get_logger

load_dotenv()

# --- CONFIGURACIÓN ---
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
            self.logger.info("Resolviendo Dashboard Data Source...")
            self.dashboard_ds_id = self.notion.get_data_source_id(PROCESS_DASHBOARD_DB_ID) or PROCESS_DASHBOARD_DB_ID

    def buscar_solicitudes_pendientes(self):
        """Busca páginas creadas por el botón (Ready=True, Processed=False)."""
        if not self.dashboard_ds_id: return []
        
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
        
        # 1. Extraer Datos del Dashboard usando CONSTANTES
        try:
            raw_title = props.get(PROP_NAME, {}).get("title", [])
            if not raw_title: return
            nombre_proceso = raw_title[0]["plain_text"]
            
            raw_select = props.get(PROP_PROCESS_TYPE, {}).get("select")
            if not raw_select: return 
            tipo_proceso = raw_select["name"]
            
            # Detectar si es PortCo o Nzyme (para el renombrado de JD)
            es_portco = "PortCo" in tipo_proceso
            
            self.logger.info(f"--> Configurando: {nombre_proceso} ({tipo_proceso})")
        except Exception as e:
            self.logger.error(f"[ERROR DATOS] {e}", exc_info=True)
            return

        # 2. Espera de Seguridad (Vital para templates)
        self.logger.info("Esperando 5s para propagación de estructura interna...")
        time.sleep(1)

        # 3. Identificar Bases de Datos Hijas y Páginas (Introspección)
        bloques = self.notion.get_page_blocks(page_id)
        wf_db_id = None
        form_db_id = None
        jd_page_id = None

        for b in bloques:
            if b["type"] == "child_database":
                titulo = b.get("child_database", {}).get("title", "").lower()
                bid = b["id"]
                if "workflow" in titulo:
                    wf_db_id = bid
                    self.logger.info(f"Detectado WORKFLOW: {bid}")
                elif "form" in titulo:
                    form_db_id = bid
                    self.logger.info(f"Detectado FORM: {bid}")
            elif b["type"] == "child_page":
                titulo = b.get("child_page", {}).get("title", "").lower()
                bid = b["id"]
                if "job" in titulo or "role" in titulo:
                    jd_page_id = bid
                    self.logger.info(f"Detectada JD PAGE: {bid}")

        if not wf_db_id or not form_db_id:
            self.logger.critical("No se encontraron las DBs hijas. Revisa el template del botón.")
            return

        # 4. Obtener Stages de Guidelines
        doc_guidelines = self.parser.buscar_documento_guidelines(tipo_proceso)
        opciones_stages = []
        if doc_guidelines:
            opciones_stages = self.parser.parsear_stages_desde_pagina(doc_guidelines["id"])
            self.logger.info(f"Stages extraidos: {len(opciones_stages)}")
            # FIX ORDEN NOTION: Zero-width spaces invisibles para forzar orden secuencial
            ZWSP = chr(0x200B)  # Zero Width Space (invisible)
            for i, stage in enumerate(opciones_stages):
                stage["name"] = ZWSP * i + stage["name"]
        else:
            self.logger.warning("Guidelines no encontrados. Se usarán stages por defecto.")

        # --- 5. CONFIGURAR WORKFLOW (RENOMBRADO + STAGES) ---
        self.logger.info("Configurando Workflow DB...")
        self.notion.update_database(wf_db_id, title=f"Workflow - {nombre_proceso}")
        
        if opciones_stages:
            wf_ds_id = self.notion.get_data_source_id(wf_db_id)
            if wf_ds_id:
                self.logger.info(f"Inyectando stages en Data Source {wf_ds_id}...")
                wf_updates = {"Stage": {"select": {"options": opciones_stages}}}
                res_schema = self.notion.update_data_source(wf_ds_id, properties=wf_updates)
                if res_schema.status_code != 200:
                    self.logger.error(f"[ERROR STAGES] {res_schema.status_code}: {res_schema.text}")
            else:
                self.logger.error("No se pudo resolver Data Source ID del Workflow.")

        # --- 6. CONFIGURAR FORM DB (RENOMBRADO) ---
        self.logger.info("Configurando Form DB...")
        self.notion.update_database(form_db_id, title=f"Candidate Form - {nombre_proceso}")

        # --- 7. CONFIGURAR JOB DESCRIPTION (CON LÓGICA DE INSERCIÓN ARRIBA) ---
        if jd_page_id:
            self.logger.info("Configurando Job Description...")
            
            # A. Buscar contenido fuente
            doc_jd = self.parser.buscar_documento_job_description(tipo_proceso)
            if doc_jd:
                self.logger.info(f"Fuente JD encontrada: {doc_jd['id']}")
                contenido_bloques = self.parser.extraer_contenido_pagina(doc_jd["id"])
                
                if contenido_bloques:
                    # B. BUSCAR EL BLOQUE ANCLA (La primera línea vacía)
                    bloques_existentes = self.notion.get_page_blocks(jd_page_id)
                    id_ancla = None
                    
                    if bloques_existentes:
                        # Asumimos que el primer bloque es tu línea vacía
                        id_ancla = bloques_existentes[0]["id"]
                        self.logger.info(f"Ancla encontrada (Insertando después del primer bloque): {id_ancla}")
                    
                    # C. Pegar contenido (Usando 'after' si encontramos ancla)
                    self.logger.info(f"Copiando {len(contenido_bloques)} bloques...")
                    self.notion.append_block_children(
                        jd_page_id, 
                        contenido_bloques[:100], 
                        after=id_ancla 
                    )
            else:
                self.logger.warning("No se encontró documento 'Job Description' en Guidelines.")

            # D. Renombrar Página
            nuevo_titulo_jd = f"Role & Candidate Description - {nombre_proceso}" if es_portco else f"Job Description - {nombre_proceso}"
            self.notion.update_page(jd_page_id, properties={"title": [{"text": {"content": nuevo_titulo_jd}}]})

        # --- 8. REGISTRAR EN SUPABASE (BACKEND) ---
        exito_supa = self.supa.registrar_proceso(wf_db_id, form_db_id, nombre_proceso, tipo_proceso)

        # 9. CIERRE
        if exito_supa:
            self.logger.info("Finalizando tarea...")
            # Usamos constante para marcar como procesado
            self.notion.update_page(page_id, properties={PROP_PROCESSED_DASHBOARD: {"checkbox": True}})
            self.logger.info("[EXITO] Proceso listo para recibir candidatos.")
        else:
            self.logger.error("[ERROR SUPABASE] Fallo al registrar. No se marca como procesado.")

    def run(self):
        self.logger.info("--- FACTORY WORKER ---")
        self._init_datasources()
        try:
            while True:
                solicitudes = self.buscar_solicitudes_pendientes()
                if solicitudes: self.logger.info(f"Detectadas {len(solicitudes)} solicitudes nuevas.")
                for sol in solicitudes: self.configurar_proceso(sol)
                time.sleep(POLLING_INTERVAL)
        except KeyboardInterrupt: self.logger.info("Apagando.")

if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()

    parser_guidelines = GuidelinesParser(client_notion)
    worker = FactoryWorkerV2(
        notion_client=client_notion,
        supa_client=client_supa,
        parser=parser_guidelines
    )
    worker.run()