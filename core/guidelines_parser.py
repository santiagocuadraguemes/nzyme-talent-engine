# core/guidelines_parser.py

import os
import re
from core.notion_client import NotionClient
from dotenv import load_dotenv


load_dotenv()


GUIDELINES_DB_ID = os.getenv("NOTION_GUIDELINES_DB_ID")


class GuidelinesParser:
    def __init__(self, notion_client: NotionClient):
        self.notion = notion_client
        self.guidelines_ds_id = None


    def _init_datasource(self):
        if not self.guidelines_ds_id and GUIDELINES_DB_ID:
            self.guidelines_ds_id = self.notion.get_data_source_id(GUIDELINES_DB_ID) or GUIDELINES_DB_ID


    def buscar_documento_guidelines(self, process_type_string):
        """Searches for the Interview Stages document."""
        self._init_datasource()
        if not self.guidelines_ds_id: return None
        
        partes = process_type_string.split(" - ")
        if len(partes) < 2: return None
            
        prefijo = partes[0].strip()
        rol = partes[1].strip()
        
        filtros = []
        if prefijo == "PortCo":
            filtros = [
                {"property": "Company", "select": {"equals": "PortCo"}},
                {"property": "Role", "select": {"equals": rol}}
            ]
        else:
            filtros = [
                {"property": "Company", "select": {"equals": "Nzyme"}},
                {"property": "Team", "select": {"equals": prefijo}},
                {"property": "Role", "select": {"equals": rol}}
            ]


        filtro_final = {
            "and": [
                {"property": "Document Type", "select": {"equals": "Interview Stages"}}
            ] + filtros
        }
        
        resultados = self.notion.query_data_source(self.guidelines_ds_id, filtro_final)
        return resultados[0] if resultados else None


    def buscar_documento_job_description(self, process_type_string):
        """Searches for the Job Description document."""
        self._init_datasource()
        if not self.guidelines_ds_id: return None
        
        partes = process_type_string.split(" - ")
        if len(partes) < 2: return None
            
        prefijo = partes[0].strip()
        rol = partes[1].strip()
        
        filtros = []
        if prefijo == "PortCo":
            filtros = [
                {"property": "Company", "select": {"equals": "PortCo"}},
                {"property": "Role", "select": {"equals": rol}}
            ]
        else:
            filtros = [
                {"property": "Company", "select": {"equals": "Nzyme"}},
                {"property": "Team", "select": {"equals": prefijo}},
                {"property": "Role", "select": {"equals": rol}}
            ]


        filtro_final = {
            "and": [
                {"property": "Document Type", "select": {"equals": "Job Description"}}
            ] + filtros
        }
        
        resultados = self.notion.query_data_source(self.guidelines_ds_id, filtro_final)
        return resultados[0] if resultados else None


    def extraer_contenido_pagina(self, page_id):
        """
        Recursively downloads page content to clone it.
        ROBUST version: Handles Tables, Lists, Columns and avoids 400 validation errors.
        """
        bloques_raiz = self.notion.get_page_blocks(page_id)
        return self._procesar_lista_bloques(bloques_raiz)



    def _procesar_lista_bloques(self, lista_bloques):
        """
        Recursive helper that clones blocks while cleaning read-only properties.
        """
        resultado = []
        
        for b in lista_bloques:
            tipo = b["type"]
            
            # 1. Ignore unsupported blocks
            if tipo in ["child_page", "child_database", "link_to_page", "unsupported"]: 
                continue
            
            # 2. Get block data
            datos_bloque = b.get(tipo, {})
            
            # 3. Build the new base block
            nuevo_bloque = {
                "object": "block",
                "type": tipo,
                tipo: datos_bloque.copy() if isinstance(datos_bloque, dict) else {}
            }
            
            # 4. PROCESS CHILDREN RECURSIVELY
            if b.get("has_children"):
                try:
                    hijos = self.notion.get_page_blocks(b["id"])
                    hijos_procesados = self._procesar_lista_bloques(hijos)
                    
                    if hijos_procesados:
                        # Blocks that accept children INSIDE their type property
                        bloques_children_internos = [
                            "paragraph", "bulleted_list_item", "numbered_list_item",
                            "toggle", "to_do", "quote", "callout", "column",
                            "table", "template", "synced_block"
                        ]
                        
                        if tipo in bloques_children_internos:
                            # Ensure the type property is a dict
                            if not isinstance(nuevo_bloque[tipo], dict):
                                nuevo_bloque[tipo] = {}
                            nuevo_bloque[tipo]["children"] = hijos_procesados
                        
                        # Blocks that accept children at root (mainly column_list)
                        elif tipo == "column_list":
                            nuevo_bloque["children"] = hijos_procesados
                        
                        # For any other type with children, try internal first
                        else:
                            if not isinstance(nuevo_bloque[tipo], dict):
                                nuevo_bloque[tipo] = {}
                            nuevo_bloque[tipo]["children"] = hijos_procesados
                            
                except Exception as e:
                    # If getting children fails, continue without them
                    print(f"[WARN] No se pudieron obtener hijos de {b['id']}: {e}")
            
            # 5. Final validations
            # Column_list without children is not valid
            if tipo == "column_list" and not nuevo_bloque.get("children"):
                continue
            
            # Empty Column is not valid
            if tipo == "column":
                if not nuevo_bloque[tipo].get("children"):
                    continue
            
            # Synced_block requires special configuration, better to omit
            if tipo == "synced_block":
                continue
                
            resultado.append(nuevo_bloque)
        
        return resultado



    def buscar_candidate_template_id(self, parent_page_id):
        bloques = self.notion.get_page_blocks(parent_page_id)
        for b in bloques:
            if b["type"] == "child_page":
                titulo = b["child_page"].get("title", "")
                if "candidate template" in titulo.lower():
                    print(f"   [TEMPLATE] Encontrado: {titulo} ({b['id']})")
                    return b["id"]
        return None


    # --- STAGE PARSING METHODS ---
    
    def _limpiar_celda(self, texto):
        if not texto: return ""
        return texto.strip().replace("\n", " ")


    def _determinar_color(self, stage_num):
        if not stage_num: return "default"
        match = re.match(r"(\d+)", stage_num)
        if not match: return "default"
        num = int(match.group(1))
        colores = {0: "gray", 1: "brown", 2: "orange", 3: "purple", 4: "blue", 5: "pink", 6: "yellow", 7: "red"}
        return colores.get(num, "default")


    def parsear_stages_desde_pagina(self, page_id):
        print(f"   [PARSER] Analizando pagina {page_id}...")
        bloques = self.notion.get_page_blocks(page_id)
        
        stages_dinamicos = []
        nombres_usados = set()
        ultimo_numero_stage = "0"
        
        # Buffer to save decisions and release them at the end of the round
        decisiones_pendientes = [] # key: round_number (str), value: stage dict


        # 1. Define the Fixed ones (Closures)
        stages_fijos = [
            {"name": "Offer", "color": "green"}, 
            {"name": "Hired", "color": "blue"}, 
            {"name": "Discarded completely for Nzyme", "color": "yellow"},
            {"name": "Disqualified only for this role", "color": "red"},
            {"name": "Lost for this process", "color": "gray"}
        ]
        nombres_fijos = {s["name"] for s in stages_fijos}


        # 2. Read Table (Dynamic)
        for bloque in bloques:
            if bloque["type"] == "table":
                filas = self.notion.get_page_blocks(bloque["id"])
                for fila in filas:
                    if fila["type"] != "table_row": continue
                    cells = fila["table_row"].get("cells", [])
                    if len(cells) < 2: continue 
                    
                    col_num = self._limpiar_celda("".join([t["plain_text"] for t in cells[0]]))
                    col_name = self._limpiar_celda("".join([t["plain_text"] for t in cells[1]]))
                    
                    # --- EXCLUSION FILTERS ---
                    if "Interview Type" in col_name or "#" in col_num: continue
                    if "Individual" in col_name: continue


                    nombre_final = None
                    color = "default"
                    es_decision = False
                    numero_ronda_actual = ultimo_numero_stage # By default


                    if col_num and col_num[0].isdigit():
                        ultimo_numero_stage = col_num.split('.')[0]
                        numero_ronda_actual = ultimo_numero_stage
                        nombre_final = f"{col_num} {col_name}"
                        color = self._determinar_color(col_num)
                        
                    elif "Group Decision" in col_name or "Round" in col_name:
                        nombre_final = f"{ultimo_numero_stage} [ROUND DECISION]"
                        color = self._determinar_color(ultimo_numero_stage)
                        es_decision = True
                        numero_ronda_actual = ultimo_numero_stage


                    if nombre_final and nombre_final not in nombres_usados and nombre_final not in nombres_fijos:
                        stage_obj = {"name": nombre_final[:100], "color": color}
                        nombres_usados.add(nombre_final)
                        
                        if es_decision:
                            # DON'T add it to the main list yet. Save it.
                            decisiones_pendientes.append({'ronda': numero_ronda_actual, 'stage': stage_obj})
                        else:
                            # Before adding a normal stage (e.g.: 2.1), check if there's a pending decision from the PREVIOUS round (1)
                            # If there is, it means we've changed rounds, so release the pending decision.
                            
                            # Note: This assumes sequential reading. If the table is ordered, it works.
                            # For greater robustness, simply add to the list and reorder after,
                            # but as you requested to maintain logic, we'll do direct insertion.
                            stages_dinamicos.append(stage_obj)



        # 3. Smart Decision Insertion (Post-Processing)
        # We traverse the created list and insert decisions at the end of their respective blocks.
        lista_con_decisiones = []
        
        # Group by round to facilitate insertion at the end
        rondas = {} # "1": [stage1.1, stage1.2], "2": [stage2.1]...
        orden_rondas = [] # ["1", "2"...] to maintain order of appearance
        
        for st in stages_dinamicos:
            
            # Extract the round number from the name (e.g.: "1.1 Interview" -> "1")
            match = re.match(r"(\d+)", st["name"])
            if match:
                r_num = match.group(1)
                if r_num not in rondas:
                    rondas[r_num] = []
                    orden_rondas.append(r_num)
                rondas[r_num].append(st)
            else:
                # If it has no number (rare), put it in a "0" or generic round
                if "0" not in rondas: 
                    rondas["0"] = []
                    orden_rondas.append("0")
                rondas["0"].append(st)


        # Rebuild the list
        for r_num in orden_rondas:
            lista_con_decisiones.extend(rondas[r_num])
            for dec in decisiones_pendientes:
                if dec['ronda'] == r_num:
                    lista_con_decisiones.append(dec['stage'])
                    break  # Only 1 per round
                
        # 4. Final Construction
        lista_final = lista_con_decisiones + stages_fijos
        return lista_final
