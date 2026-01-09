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

    def buscar_candidate_template_id(self, parent_page_id):
        bloques = self.notion.get_page_blocks(parent_page_id)
        for b in bloques:
            if b["type"] == "child_page":
                titulo = b["child_page"].get("title", "")
                if "candidate template" in titulo.lower():
                    print(f"   [TEMPLATE] Encontrado: {titulo} ({b['id']})")
                    return b["id"]
        return None

    # --- METODOS DE PARSEO DE STAGES ---
    
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
        
        # 1. Definimos los Fijos (Cierres)
        stages_fijos = [
            {"name": "Offer", "color": "green"}, 
            {"name": "Hired", "color": "blue"}, 
            {"name": "Rejected", "color": "red"},
            {"name": "Lost", "color": "gray"}
        ]
        nombres_fijos = {s["name"] for s in stages_fijos}

        # 2. Leer Tabla (Dinámicos)
        for bloque in bloques:
            if bloque["type"] == "table":
                filas = self.notion.get_page_blocks(bloque["id"])
                for fila in filas:
                    if fila["type"] != "table_row": continue
                    cells = fila["table_row"].get("cells", [])
                    if len(cells) < 2: continue 
                    
                    col_num = self._limpiar_celda("".join([t["plain_text"] for t in cells[0]]))
                    col_name = self._limpiar_celda("".join([t["plain_text"] for t in cells[1]]))
                    
                    # --- FILTROS DE EXCLUSIÓN ---
                    if "Interview Type" in col_name or "#" in col_num: continue
                    if "Individual" in col_name: continue

                    nombre_final = None
                    color = "default"
                    
                    if col_num and col_num[0].isdigit():
                        ultimo_numero_stage = col_num.split('.')[0]
                        nombre_final = f"{col_num} {col_name}"
                        color = self._determinar_color(col_num)
                    elif "Group Decision" in col_name or "Round" in col_name:
                        nombre_final = f"{ultimo_numero_stage}. [ROUND DECISION]"
                        color = self._determinar_color(ultimo_numero_stage)

                    # Si el nombre es válido, no está repetido y NO es uno de los fijos...
                    if nombre_final and nombre_final not in nombres_usados and nombre_final not in nombres_fijos:
                        nombre_final = nombre_final[:100]
                        stages_dinamicos.append({"name": nombre_final, "color": color})
                        nombres_usados.add(nombre_final)

        # 3. Construcción Final de la Lista
        lista_final = stages_dinamicos + stages_fijos
        
        return lista_final