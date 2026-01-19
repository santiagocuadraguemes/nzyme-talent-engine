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
        """Busca el documento de Interview Stages."""
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
        """Busca el documento de Job Description."""
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
        bloques = self.notion.get_page_blocks(page_id)
        bloques_limpios = []
        for b in bloques:
            tipo = b["type"]
            if tipo in ["child_page", "child_database"]: continue
            nuevo_bloque = {
                "object": "block",
                "type": tipo,
                tipo: b[tipo]
            }
            bloques_limpios.append(nuevo_bloque)
        return bloques_limpios

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
        
        # Buffer para guardar decisiones y soltarlas al final de la ronda
        decisiones_pendientes = [] # clave: numero_ronda (str), valor: dict del stage

        # 1. Definimos los Fijos (Cierres)
        stages_fijos = [
            {"name": "Offer", "color": "green"}, 
            {"name": "Hired", "color": "blue"}, 
            {"name": "Discarded completely for Nzyme", "color": "yellow"},
            {"name": "Disqualified only for this role", "color": "red"},
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
                    es_decision = False
                    numero_ronda_actual = ultimo_numero_stage # Por defecto

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
                            # NO lo añadimos a la lista principal todavía. Lo guardamos.
                            decisiones_pendientes.append({'ronda': numero_ronda_actual, 'stage': stage_obj})
                        else:
                            # Antes de añadir un stage normal (ej: 2.1), miramos si hay una decisión pendiente de la ronda ANTERIOR (1)
                            # Si la hay, significa que hemos cambiado de ronda, así que soltamos la decisión pendiente.
                            
                            # Nota: Esto asume lectura secuencial. Si la tabla está ordenada, funciona.
                            # Para mayor robustez, simplemente añadimos a la lista y reordenamos después, 
                            # pero como pediste mantener lógica, haremos inserción directa.
                            stages_dinamicos.append(stage_obj)


        # 3. Inserción Inteligente de Decisiones (Post-Procesado)
        # Recorremos la lista creada e insertamos las decisiones al final de sus respectivos bloques.
        lista_con_decisiones = []
        
        # Agrupamos por ronda para facilitar la inserción al final
        rondas = {} # "1": [stage1.1, stage1.2], "2": [stage2.1]...
        orden_rondas = [] # ["1", "2"...] para mantener el orden de aparición
        
        for st in stages_dinamicos:
            
            # Extraemos el número de ronda del nombre (ej: "1.1 Entrevista" -> "1")
            match = re.match(r"(\d+)", st["name"])
            if match:
                r_num = match.group(1)
                if r_num not in rondas:
                    rondas[r_num] = []
                    orden_rondas.append(r_num)
                rondas[r_num].append(st)
            else:
                # Si no tiene número (raro), lo metemos en una ronda "0" o genérica
                if "0" not in rondas: 
                    rondas["0"] = []
                    orden_rondas.append("0")
                rondas["0"].append(st)

        # Reconstruimos la lista
        for r_num in orden_rondas:
            lista_con_decisiones.extend(rondas[r_num])
            for dec in decisiones_pendientes:
                if dec['ronda'] == r_num:
                    lista_con_decisiones.append(dec['stage'])
                    break  # 1 sola por ronda
                
        # 4. Construcción Final
        lista_final = lista_con_decisiones + stages_fijos
        return lista_final