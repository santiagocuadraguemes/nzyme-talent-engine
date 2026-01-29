import sys
import os
import re
import json
import time
from dotenv import load_dotenv


# Ajuste de path para ejecución local/Lambda
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import AnalizadorCV
from core.notion_builder import NotionBuilder
from core.domain_mapper import DomainMapper
from core.utils import descargar_archivo
from core.constants import PROP_CHECKBOX_PROCESSED, PROP_ID, PROP_NAME, PROP_CV_FILES, PROP_STAGE
from core.logger import get_logger


load_dotenv()


MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
TEMP_FOLDER = "/tmp/temp_downloads"
MAX_CVS_PER_RUN = 15


if not os.path.exists(TEMP_FOLDER):
    os.makedirs(TEMP_FOLDER)


class HarvesterRelational:
    def __init__(self, notion_client, supa_client, storage_client, ai_analyzer):
        self.logger = get_logger("Harvester")
        self.notion = notion_client
        self.supa_manager = supa_client
        self.storage = storage_client
        self.ai = ai_analyzer
        
        self.main_ds_id = self.notion.get_data_source_id(MAIN_DB_ID) or MAIN_DB_ID


    def buscar_candidato_smart(self, email, nombre):
        """
        Lógica de coincidencia para Notion.
        Regla: Email > Nombre.
        IMPORTANTE: Si coincide el nombre, lo tratamos como el mismo candidato (Merge),
        aunque el email sea diferente o no exista previamente.
        """
        # A. Búsqueda por Email
        if email:
            res = self.notion.query_data_source(self.main_ds_id, {"property": "Email", "email": {"equals": email}})
            if res: return res[0]
        
        # B. Búsqueda por Nombre (Fallback)
        if nombre:
            res = self.notion.query_data_source(self.main_ds_id, {"property": "Name", "title": {"equals": nombre}})
            if res:
                self.logger.info(f"Match por Nombre: '{nombre}'. Asumiendo mismo candidato (Merge).")
                return res[0]
        
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
        """
        Busca la página del formulario (auxiliar) usando el ID único.
        """
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
        
        archivo = files[0]
        url = archivo.get("file", {}).get("url") or archivo.get("external", {}).get("url")
        
        if not url: return None, None
        
        return url, archivo["name"]


    # --- LÓGICA: BATCH SPLITTER ---
    def procesar_bulk_imports(self, procesos):
        """
        Revisa las colas de 'Bulk Queue'.
        Desglosa archivos múltiples en entradas individuales para el Candidate Form.
        """
        for proc in procesos:
            bulk_db_id = proc.get("notion_bulk_id")
            form_db_id = proc.get("notion_form_id")
            
            if not bulk_db_id or not form_db_id: continue


            ds_bulk = self.notion.get_data_source_id(bulk_db_id)
            if not ds_bulk: continue


            filtro = {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}}
            lotes = self.notion.query_data_source(ds_bulk, filtro)
            
            if lotes:
                self.logger.info(f"Desglosando {len(lotes)} lotes en '{proc['process_name']}'")


            for lote in lotes:
                lote_id = lote["id"]
                props = lote["properties"]
                
                files = props.get("CVs", {}).get("files", []) 
                if not files: 
                    self.notion.update_page(lote_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})
                    continue


                errores_en_lote = False


                for file_obj in files:
                    file_name = file_obj["name"]
                    notion_url = file_obj.get("file", {}).get("url") or file_obj.get("external", {}).get("url")
                    
                    if not notion_url: continue


                    try:
                        public_url = self.storage.subir_cv_desde_url(notion_url, file_name)
                        
                        if not public_url:
                            self.logger.error(f"Error subiendo {file_name} a storage")
                            errores_en_lote = True
                            continue
                    except Exception as e:
                        self.logger.error(f"Excepción subiendo {file_name}: {e}")
                        errores_en_lote = True
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
                        }
                    }


                    res = self.notion.create_page(form_db_id, payload)
                    
                    if res.status_code != 200:
                        errores_en_lote = True
                        self.logger.error(f"Error creando '{file_name}'. Status: {res.status_code}")


                self.notion.update_page(lote_id, {PROP_CHECKBOX_PROCESSED: {"checkbox": True}})


    # --- PROCESAMIENTO ESTÁNDAR ---
    def procesar_candidato(self, cand, process_entry, relation_col_name, stage_inicial):
        page_id = cand["id"]
        props = cand["properties"]
        
        process_name_actual = process_entry["process_name"]
        process_type_actual = process_entry.get("process_type") 
        
        # 1. Obtener CV
        id_text = props.get(PROP_ID, {}).get("rich_text", [])[0]["plain_text"] if props.get(PROP_ID, {}).get("rich_text", []) else ""
        notion_url, file_name = self.buscar_cv_en_auxiliar(process_entry["notion_form_id"], id_text)
        
        if not notion_url:
            return


        local_path = descargar_archivo(notion_url, file_name, TEMP_FOLDER)
        if not local_path: return
        
        public_url = self.storage.subir_cv_desde_url(notion_url, file_name)
        if not public_url: return


        # 2. IA
        self.logger.info(f"Analizando CV: {file_name}")
        datos_ia = self.ai.procesar_cv(local_path)
        try: os.remove(local_path)
        except: pass
        if not datos_ia: return


        # --- 3. GESTIÓN DE IDENTIDAD Y FUSIÓN ---
        
        cand_db, id_madre_notion = self.supa_manager.resolver_identidad_candidato(datos_ia.get("email"), datos_ia["name"])
        
        historial_previo = []
        team_role_previo = []


        if cand_db:
            self.logger.info(f"Candidato existente (ID: {id_madre_notion}). Fusionando datos")
            
            cand_json = cand_db.get("candidate_data") or {}
            
            historial_previo = cand_json.get("recruiting_processes_history", [])
            team_role_previo = cand_json.get("proposed_teams_roles", [])
            
        else:
            self.logger.info("Candidato nuevo")


        props_madre = NotionBuilder.build_candidate_payload(
            datos_ia, 
            public_url, 
            process_name_actual, 
            existing_history=historial_previo,
            process_type=process_type_actual,   
            existing_team_role=team_role_previo 
        )
        
        # --- 4. ESCRITURA ---
        error_madre = False
        
        if id_madre_notion:
            res_op = self.notion.update_page(id_madre_notion, props_madre)
        else:
            res_op = self.notion.create_page(MAIN_DB_ID, props_madre)
            if res_op.status_code == 200:
                id_madre_notion = res_op.json()["id"]


        if res_op.status_code != 200:
            self.logger.error(f"Error Notion API: {res_op.status_code}")
            error_madre = True
            return


        # 5. SUPABASE SYNC
        if not error_madre:
            datos_candidato_sql = DomainMapper.map_to_supabase_candidate(datos_ia, public_url)
            
            json_payload = datos_candidato_sql["candidate_data"]
            
            full_history = list(historial_previo)
            if process_name_actual not in full_history: full_history.append(process_name_actual)
            json_payload["recruiting_processes_history"] = full_history


            full_roles = list(team_role_previo)
            if process_type_actual and process_type_actual not in full_roles: full_roles.append(process_type_actual)
            json_payload["proposed_teams_roles"] = full_roles


            uuid_candidato = self.supa_manager.gestion_candidato(datos_candidato_sql, id_madre_notion)


            if uuid_candidato:
                self.supa_manager.crear_aplicacion(
                    uuid_candidato, 
                    process_entry["notion_workflow_id"], 
                    page_id, 
                    stage_inicial
                )


        # 6. Strategic Assessment
        if datos_ia.get("strategic_assessment"):
            self._rellenar_strategic_assessment(page_id, datos_ia["strategic_assessment"])


        # 7. CIERRE
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
        self.logger.info("Candidato procesado correctamente")


    def _rellenar_strategic_assessment(self, candidate_page_id, assessment_list):
        """
        Busca la tabla 'Past Experience [AI-generated]', mapea las filas existentes
        y las rellena con los datos de la IA.
        """
        if not assessment_list: return


        time.sleep(4) 


        db_title = "Past Experience [AI-generated]"
        child_db_id = self.notion.find_child_database(candidate_page_id, db_title)
        
        if not child_db_id:
            self.logger.warning(f"DB '{db_title}' no encontrada")
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
                    self.logger.error(f"Error escribiendo '{char_name}': {res.status_code}")


        self.logger.info(f"Assessment completado: {updates_count}/{len(assessment_list)} filas")


    def run_once(self):
        """Ejecuta una pasada completa por todos los procesos activos."""
        self.logger.info("Harvester iniciando")
        
        procesos = self.supa_manager.obtener_procesos_activos()
        if not procesos:
            self.logger.info("Sin procesos activos")
            return


        # --- PASO 1: BATCH SPLITTER ---
        self.procesar_bulk_imports(procesos)


        # --- PASO 2: PROCESAMIENTO ESTÁNDAR ---
        cvs_procesados_hoy = 0
        for proc in procesos:
            
            if cvs_procesados_hoy >= MAX_CVS_PER_RUN:
                self.logger.warning(f"Límite de seguridad alcanzado ({MAX_CVS_PER_RUN} CVs)")
                break


            wf_db_id = proc["notion_workflow_id"]
            ds_wf = self.notion.get_data_source_id(wf_db_id)
            if not ds_wf: continue


            rel_col = self.encontrar_propiedad_relacion(ds_wf)
            
            filtro = {
                "and": [
                    {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}},
                    {"property": PROP_ID, "rich_text": {"is_not_empty": True}}
                ]
            }
            candidatos = self.notion.query_data_source(ds_wf, filtro)
            
            if candidatos:
                self.logger.info(f"Procesando {len(candidatos)} candidatos en '{proc['process_name']}'")
                stage_init = self.determinar_stage_inicial(ds_wf)
                for cand in candidatos:


                    if cvs_procesados_hoy >= MAX_CVS_PER_RUN: 
                        break
                    self.procesar_candidato(cand, proc, rel_col, stage_init)
                    cvs_procesados_hoy += 1
            
        self.logger.info("Ejecución completada")


if __name__ == "__main__":
    client_notion = NotionClient()
    client_supa = SupabaseManager()
    client_storage = StorageClient()
    analyzer_ai = AnalizadorCV()


    bot = HarvesterRelational(client_notion, client_supa, client_storage, analyzer_ai)
    bot.run_once()
