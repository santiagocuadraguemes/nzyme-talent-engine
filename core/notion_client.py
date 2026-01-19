import os
import requests
from dotenv import load_dotenv

load_dotenv()

class NotionClient:
    def __init__(self):
        self.token = os.getenv("NOTION_KEY")
        self.base_url = "https://api.notion.com/v1"
        
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2025-09-03"
        }

    def get_data_source_id(self, database_id):
        """Obtiene el ID del Data Source subyacente (Vital para API 2025)."""
        url = f"{self.base_url}/databases/{database_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            sources = data.get("data_sources", [])
            if sources: return sources[0]["id"]
        return None

    def get_page_blocks(self, block_id):
        """Descarga el contenido de una página (para inspeccionar hijos)."""
        url = f"{self.base_url}/blocks/{block_id}/children?page_size=100"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json().get("results", [])
        return []

    def append_block_children(self, block_id, children, after=None):
        """
        Añade bloques.
        - Si 'after' es None: Añade al final.
        - Si 'after' es un ID de bloque: Añade justo después de ese bloque.
        """
        url = f"{self.base_url}/blocks/{block_id}/children"
        payload = {"children": children}
        
        if after:
            payload["after"] = after
        
        response = requests.patch(url, headers=self.headers, json=payload)
        return response

    def query_data_source(self, data_source_id, filter_params):
        """Consulta datos usando el Data Source ID."""
        url = f"{self.base_url}/data_sources/{data_source_id}/query"
        payload = {}
        if filter_params: payload["filter"] = filter_params
        
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code == 200:
            return response.json().get("results", [])
        print(f"[API ERROR] Query failed ({response.status_code}): {response.text}")
        return []

    def update_database(self, database_id, title=None):
        """
        Modifica el CONTENEDOR (Base de Datos). 
        Usado para cambiar el Título.
        """
        url = f"{self.base_url}/databases/{database_id}"
        payload = {}
        if title:
            payload["title"] = [{"text": {"content": title}}]
            
        response = requests.patch(url, headers=self.headers, json=payload)
        return response

    def update_data_source(self, data_source_id, properties):
        """
        Modifica el ESQUEMA (Data Source).
        Usado para inyectar Stages y configurar Relaciones.
        """
        url = f"{self.base_url}/data_sources/{data_source_id}"
        payload = {"properties": properties}
        
        response = requests.patch(url, headers=self.headers, json=payload)
        return response

    def update_page(self, page_id, properties=None):
        """Actualiza propiedades de una página."""
        url = f"{self.base_url}/pages/{page_id}"
        payload = {}
        if properties: payload["properties"] = properties
        
        response = requests.patch(url, headers=self.headers, json=payload)
        return response

    def create_page_in_db(self, database_id, properties):
        """Crea una página en la Main DB."""
        ds_id = self.get_data_source_id(database_id)
        target_id = ds_id if ds_id else database_id
        
        url = f"{self.base_url}/pages"
        parent_struct = {
            "type": "data_source_id" if ds_id else "database_id",
            "data_source_id" if ds_id else "database_id": target_id
        }
        
        payload = {"parent": parent_struct, "properties": properties}
        response = requests.post(url, headers=self.headers, json=payload)
        return response
    
    def get_database_schema(self, data_source_id):
        """Lee las propiedades (útil para detectar nombre de columnas)."""
        url = f"{self.base_url}/data_sources/{data_source_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200: return response.json().get("properties", {})
        return {}

    def search_recently_edited(self, since_iso_timestamp):
        """
        Busca páginas modificadas ordenadas por fecha.
        Filtra en Python las que sean más recientes que el timestamp dado.
        """
        url = f"{self.base_url}/search"
        
        # Pedimos a Notion las últimas 100 páginas modificadas
        payload = {
            "filter": {"value": "page", "property": "object"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            "page_size": 100
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        
        if response.status_code == 200:
            results = response.json().get("results", [])
            filtered = []
            # Filtramos nosotros manualmente
            for page in results:
                last_edited = page.get("last_edited_time")
                # Comparación de cadenas ISO 8601 funciona correctamente
                if last_edited and last_edited > since_iso_timestamp:
                    filtered.append(page)
            return filtered
            
        print(f"[API ERROR] Search failed ({response.status_code}): {response.text}")
        return []

    def find_child_database(self, parent_page_id, db_title_contains):
        """Busca una base de datos hija dentro de una página."""
        # Nota: get_page_blocks ya debe estar implementado
        children = self.get_page_blocks(parent_page_id)
        for block in children:
            if block["type"] == "child_database":
                title = block.get("child_database", {}).get("title", "").lower()
                if db_title_contains.lower() in title:
                    return block["id"]
        return None