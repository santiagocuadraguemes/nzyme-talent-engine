# core/notion_client.py

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
            "Notion-Version": "2025-09-03" # Check if this future version is correct for your case, I assume it is.
        }


    def get_data_source_id(self, database_id):
        """Gets the underlying Data Source ID (Vital for 2025 API)."""
        url = f"{self.base_url}/databases/{database_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            sources = data.get("data_sources", [])
            if sources: return sources[0]["id"]
        return None


    def get_page_blocks(self, block_id):
        """Downloads the content of a page (to inspect children)."""
        url = f"{self.base_url}/blocks/{block_id}/children?page_size=100"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json().get("results", [])
        return []


    def append_block_children(self, block_id, children, after=None):
        """Adds child blocks to a parent block."""
        url = f"{self.base_url}/blocks/{block_id}/children"
        payload = {"children": children}

        if after:
            payload["after"] = after

        response = requests.patch(url, headers=self.headers, json=payload)
        return response


    def query_data_source(self, data_source_id, filter_params):
        """Queries data using the Data Source ID."""
        url = f"{self.base_url}/data_sources/{data_source_id}/query"
        payload = {}
        if filter_params: payload["filter"] = filter_params
        
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code == 200:
            return response.json().get("results", [])
        print(f"[API ERROR] Query failed ({response.status_code}): {response.text}")
        return []


    def update_database(self, database_id, title=None):
        """Modifies the title of a Database."""
        url = f"{self.base_url}/databases/{database_id}"
        payload = {}
        if title:
            payload["title"] = [{"text": {"content": title}}]
            
        response = requests.patch(url, headers=self.headers, json=payload)
        return response


    def update_data_source(self, data_source_id, properties):
        """Modifies the SCHEMA (Data Source)."""
        url = f"{self.base_url}/data_sources/{data_source_id}"
        payload = {"properties": properties}
        
        response = requests.patch(url, headers=self.headers, json=payload)
        return response


    def update_page(self, page_id, properties=None):
        """Updates properties of a page."""
        url = f"{self.base_url}/pages/{page_id}"
        payload = {}
        if properties: payload["properties"] = properties
        
        response = requests.patch(url, headers=self.headers, json=payload)
        return response


    def create_page(self, database_id, properties):
        """
        Creates a page in a DB.
        Automatically detects whether we need to use the data_source_id or database_id.
        """
        # Try to resolve the Data Source ID first
        ds_id = self.get_data_source_id(database_id)
        target_id = ds_id if ds_id else database_id

        url = f"{self.base_url}/pages"

        # Dynamic parent structure
        parent_struct = {
            "type": "data_source_id" if ds_id else "database_id",
            "data_source_id" if ds_id else "database_id": target_id
        }

        payload = {"parent": parent_struct, "properties": properties}

        response = requests.post(url, headers=self.headers, json=payload)

        return response


    def get_database_schema(self, data_source_id):
        """Reads the properties of a DB."""
        url = f"{self.base_url}/data_sources/{data_source_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200: return response.json().get("properties", {})
        return {}


    def search_recently_edited(self, since_iso_timestamp):
        """Searches for recently modified pages."""
        url = f"{self.base_url}/search"
        payload = {
            "filter": {"value": "page", "property": "object"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            "page_size": 100
        }
        
        response = requests.post(url, headers=self.headers, json=payload)
        
        if response.status_code == 200:
            results = response.json().get("results", [])
            filtered = []
            for page in results:
                last_edited = page.get("last_edited_time")
                if last_edited and last_edited > since_iso_timestamp:
                    filtered.append(page)
            return filtered
            
        print(f"[API ERROR] Search failed ({response.status_code}): {response.text}")
        return []


    def find_child_database(self, parent_block_id, db_title_match):
        """
        Searches for a 'child_database' by title, drilling into containers
        (Toggles, Columns, etc.). Uses BFS to avoid infinite recursion.
        """
        # Queue of blocks to inspect: [(block_id, depth)]
        queue = [(parent_block_id, 0)]
        # Safety limit to avoid searching forever
        max_depth = 4 
        checked_ids = set()


        while queue:
            current_id, depth = queue.pop(0)
            
            if current_id in checked_ids: continue
            checked_ids.add(current_id)


            if depth > max_depth: continue


            # Get the children of this block
            children = self.get_page_blocks(current_id)
            
            for block in children:
                b_type = block["type"]
                b_id = block["id"]


                # 1. Is this the database we're looking for?
                if b_type == "child_database":
                    title = block.get("child_database", {}).get("title", "")
                    # Flexible match (ignore case)
                    if db_title_match.lower() in title.lower():
                        return b_id
                
                # 2. Is it a container that could have the DB inside?
                # Notion has many container types. If it has 'has_children', we enter.
                if block.get("has_children", False):
                    # Common types where DBs hide:
                    # toggle, column_list, column, callout, synced_block, etc.
                    queue.append((b_id, depth + 1))
        
        return None
