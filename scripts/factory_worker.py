import sys
import os
import time
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.guidelines_parser import GuidelinesParser

load_dotenv()

# --- CONFIGURACIÓN ---
PROCESS_DASHBOARD_DB_ID = os.getenv("NOTION_PROCESS_DASHBOARD_DB_ID")
MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
POLLING_INTERVAL = 10 

class FactoryWorkerV2:
    def __init__(self):
        self.notion = NotionClient()
        self.supa = SupabaseManager()
        self.parser = GuidelinesParser(self.notion)
        self.dashboard_ds_id = None

    def _init_datasources(self):
        if not self.dashboard_ds_id and PROCESS_DASHBOARD_DB_ID:
            print("Resolviendo Dashboard Data Source...")
            self.dashboard_ds_id = self.notion.get_data_source_id(PROCESS_DASHBOARD_DB_ID) or PROCESS_DASHBOARD_DB_ID

    def buscar_solicitudes_pendientes(self):
        """Busca páginas creadas por el botón (Ready=True, Processed=False)."""
        if not self.dashboard_ds_id: return []
        
        filtro = {
            "and": [
                {"property": "Ready to be Processed [Do not touch]", "checkbox": {"equals": True}},
                {"property": "Processed [Do not touch]", "checkbox": {"equals": False}}
            ]
        }
        return self.notion.query_data_source(self.dashboard_ds_id, filtro)

    def configurar_proceso(self, pagina_padre):
        page_id = pagina_padre["id"]
        props = pagina_padre["properties"]
        
        # 1. Extraer Datos del Dashboard
        try:
            raw_title = props.get("Name", {}).get("title", [])
            if not raw_title: return
            nombre_proceso = raw_title[0]["plain_text"]
            
            raw_select = props.get("Process Type", {}).get("select")
            if not raw_select: return 
            tipo_proceso = raw_select["name"]
            
            print(f"--> Configurando: {nombre_proceso} ({tipo_proceso})")
        except Exception as e:
            print(f"   [ERROR DATOS] {e}")
            return

        # 2. Espera de Seguridad (Vital para templates)
        print("   Esperando 10s para propagación de estructura interna...")
        time.sleep(10)

        # 3. Identificar Bases de Datos Hijas (Introspección)
        bloques = self.notion.get_page_blocks(page_id)
        wf_db_id = None
        form_db_id = None

        for b in bloques:
            if b["type"] == "child_database":
                titulo = b.get("child_database", {}).get("title", "").lower()
                bid = b["id"]
                if "workflow" in titulo:
                    wf_db_id = bid
                    print(f"   Detectado WORKFLOW: {bid}")
                elif "form" in titulo:
                    form_db_id = bid
                    print(f"   Detectado FORM: {bid}")

        if not wf_db_id or not form_db_id:
            print("   [ERROR CRITICO] No se encontraron las DBs hijas. Revisa el template del botón.")
            return

        # 4. Obtener Stages de Guidelines
        doc_guidelines = self.parser.buscar_documento_guidelines(tipo_proceso)
        opciones_stages = []
        if doc_guidelines:
            opciones_stages = self.parser.parsear_stages_desde_pagina(doc_guidelines["id"])
            print(f"   Stages extraidos: {len(opciones_stages)}")
        else:
            print("   [AVISO] Guidelines no encontrados. Se usarán stages por defecto.")

        # --- 5. CONFIGURAR WORKFLOW (RENOMBRADO + STAGES) ---
        print("   Configurando Workflow DB...")
        
        # A. Renombrar (Database Update)
        self.notion.update_database(wf_db_id, title=f"Workflow - {nombre_proceso}")
        
        # B. Inyectar Stages (Data Source Update)
        if opciones_stages:
            wf_ds_id = self.notion.get_data_source_id(wf_db_id)
            if wf_ds_id:
                print(f"   Inyectando stages en Data Source {wf_ds_id}...")
                wf_updates = {"Stage": {"select": {"options": opciones_stages}}}
                
                res_schema = self.notion.update_data_source(wf_ds_id, properties=wf_updates)
                if res_schema.status_code != 200:
                    print(f"   [ERROR STAGES] {res_schema.status_code}: {res_schema.text}")
            else:
                print("   [ERROR] No se pudo resolver Data Source ID del Workflow.")

        # --- 6. CONFIGURAR FORM DB (RENOMBRADO) ---
        print("   Configurando Form DB...")
        self.notion.update_database(form_db_id, title=f"Candidate Form - {nombre_proceso}")

        # --- 7. REGISTRAR EN SUPABASE (BACKEND) ---
        # Guardamos en la tabla 'processes'
        exito_supa = self.supa.registrar_proceso(wf_db_id, form_db_id, nombre_proceso, tipo_proceso)

        # 8. CIERRE
        if exito_supa:
            print("   Finalizando tarea...")
            self.notion.update_page(page_id, properties={"Processed [Do not touch]": {"checkbox": True}})
            print("   [EXITO] Proceso listo para recibir candidatos.")
        else:
            print("   [ERROR SUPABASE] Fallo al registrar. No se marca como procesado.")

    def run(self):
        print("--- FACTORY WORKER V5 ---")
        self._init_datasources()
        try:
            while True:
                solicitudes = self.buscar_solicitudes_pendientes()
                if solicitudes: print(f"Detectadas {len(solicitudes)} solicitudes nuevas.")
                for sol in solicitudes: self.configurar_proceso(sol)
                time.sleep(POLLING_INTERVAL)
        except KeyboardInterrupt: print("Apagando.")

if __name__ == "__main__":
    FactoryWorkerV2().run()