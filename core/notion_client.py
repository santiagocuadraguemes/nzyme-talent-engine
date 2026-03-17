# core/notion_client.py

import os
import httpx
from dotenv import load_dotenv
from core.logger import get_logger


load_dotenv()

logger = get_logger("NotionClient")


class NotionClient:
    def __init__(self):
        self.token = os.getenv("NOTION_KEY")
        self.base_url = "https://api.notion.com/v1"

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Notion-Version": "2025-09-03" # Check if this future version is correct for your case, I assume it is.
        }

        self.client = httpx.Client(headers=self.headers, timeout=30.0)


    def get_page(self, page_id):
        """Fetches a single page by ID. Returns full page dict or None."""
        url = f"{self.base_url}/pages/{page_id}"
        logger.debug(f"get_page → page {page_id[:8]}...")
        response = self.client.get(url)
        logger.debug(f"get_page ← status {response.status_code}")
        if response.status_code == 200:
            return response.json()
        print(f"[API ERROR] get_page failed ({response.status_code}): {response.text[:200]}")
        return None

    def get_data_source_id(self, database_id):
        """Gets the underlying Data Source ID (Vital for 2025 API)."""
        url = f"{self.base_url}/databases/{database_id}"
        logger.debug(f"get_data_source_id → db {database_id[:8]}...")
        response = self.client.get(url)
        logger.debug(f"get_data_source_id ← status {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            sources = data.get("data_sources", [])
            if sources:
                logger.debug(f"get_data_source_id resolved → ds {sources[0]['id'][:8]}...")
                return sources[0]["id"]
        logger.debug("get_data_source_id → no data source resolved")
        return None


    def get_page_blocks(self, block_id):
        """Downloads the content of a page (to inspect children)."""
        url = f"{self.base_url}/blocks/{block_id}/children?page_size=100"
        logger.debug(f"get_page_blocks → block {block_id[:8]}...")
        response = self.client.get(url)
        logger.debug(f"get_page_blocks ← status {response.status_code}")
        if response.status_code == 200:
            return response.json().get("results", [])
        return []


    def append_block_children(self, block_id, children, after=None):
        """Adds child blocks to a parent block."""
        url = f"{self.base_url}/blocks/{block_id}/children"
        logger.debug(f"append_block_children → block {block_id[:8]}..., {len(children)} child(ren)")
        payload = {"children": children}

        if after:
            payload["after"] = after

        response = self.client.patch(url, json=payload)
        logger.debug(f"append_block_children ← status {response.status_code}")
        if response.status_code != 200:
            logger.error(f"append_block_children FAILED — block={block_id[:8]}..., children={len(children)}, status={response.status_code}, body={response.text[:300]}")
        return response


    def query_data_source(self, data_source_id, filter_params):
        """Queries data using the Data Source ID."""
        url = f"{self.base_url}/data_sources/{data_source_id}/query"
        logger.debug(f"query_data_source → ds {data_source_id[:8]}..., filter={'yes' if filter_params else 'none'}")
        payload = {}
        if filter_params: payload["filter"] = filter_params

        response = self.client.post(url, json=payload)
        logger.debug(f"query_data_source ← status {response.status_code}")
        if response.status_code == 200:
            return response.json().get("results", [])
        print(f"[API ERROR] Query failed ({response.status_code}): {response.text}")
        return []


    def update_database(self, database_id, title=None):
        """Modifies the title of a Database."""
        url = f"{self.base_url}/databases/{database_id}"
        logger.debug(f"update_database → db {database_id[:8]}..., title={'set' if title else 'unchanged'}")
        payload = {}
        if title:
            payload["title"] = [{"text": {"content": title}}]

        response = self.client.patch(url, json=payload)
        logger.debug(f"update_database ← status {response.status_code}")
        if response.status_code != 200:
            logger.error(f"update_database FAILED — db={database_id[:8]}..., title={'set' if title else 'unchanged'}, status={response.status_code}, body={response.text[:300]}")
        return response


    def update_data_source(self, data_source_id, properties):
        """Modifies the SCHEMA (Data Source)."""
        url = f"{self.base_url}/data_sources/{data_source_id}"
        logger.debug(f"update_data_source → ds {data_source_id[:8]}..., {len(properties)} property schema update(s)")
        payload = {"properties": properties}

        response = self.client.patch(url, json=payload)
        logger.debug(f"update_data_source ← status {response.status_code}")
        if response.status_code != 200:
            prop_keys = list(properties.keys()) if properties else []
            logger.error(f"update_data_source FAILED — ds={data_source_id[:8]}..., props={prop_keys}, status={response.status_code}, body={response.text[:300]}")
        return response


    def update_page(self, page_id, properties=None):
        """Updates properties of a page."""
        url = f"{self.base_url}/pages/{page_id}"
        logger.debug(f"update_page → page {page_id[:8]}..., {len(properties) if properties else 0} property update(s)")
        payload = {}
        if properties: payload["properties"] = properties

        response = self.client.patch(url, json=payload)
        logger.debug(f"update_page ← status {response.status_code}")
        if response.status_code != 200:
            prop_keys = list(properties.keys()) if properties else []
            logger.error(f"update_page FAILED — page={page_id[:8]}..., props={prop_keys}, status={response.status_code}, body={response.text[:300]}")
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

        logger.debug(f"create_page → db {database_id[:8]}..., parent_type={'data_source_id' if ds_id else 'database_id'}")
        payload = {"parent": parent_struct, "properties": properties}

        response = self.client.post(url, json=payload)
        logger.debug(f"create_page ← status {response.status_code}")
        if response.status_code != 200:
            prop_keys = list(properties.keys()) if properties else []
            logger.error(f"create_page FAILED — db={database_id[:8]}..., props={prop_keys}, status={response.status_code}, body={response.text[:300]}")
        return response


    def get_database_schema(self, data_source_id):
        """Reads the properties of a DB."""
        url = f"{self.base_url}/data_sources/{data_source_id}"
        logger.debug(f"get_database_schema → ds {data_source_id[:8]}...")
        response = self.client.get(url)
        logger.debug(f"get_database_schema ← status {response.status_code}")
        if response.status_code == 200: return response.json().get("properties", {})
        return {}


    def find_child_database(self, parent_block_id, db_title_match):
        """
        Searches for a 'child_database' by title, drilling into containers
        (Toggles, Columns, etc.). Uses BFS to avoid infinite recursion.
        """
        logger.debug(f"find_child_database → parent {parent_block_id[:8]}..., title_match='{db_title_match}'")
        # Queue of blocks to inspect: [(block_id, depth)]
        queue = [(parent_block_id, 0)]
        # Safety limit to avoid searching forever
        max_depth = 4
        checked_ids = set()
        blocks_checked = 0

        while queue:
            current_id, depth = queue.pop(0)

            if current_id in checked_ids: continue
            checked_ids.add(current_id)

            if depth > max_depth: continue

            logger.debug(f"find_child_database BFS depth={depth}, checked so far={len(checked_ids)}")

            # Get the children of this block
            children = self.get_page_blocks(current_id)

            for block in children:
                b_type = block["type"]
                b_id = block["id"]
                blocks_checked += 1

                # 1. Is this the database we're looking for?
                if b_type == "child_database":
                    title = block.get("child_database", {}).get("title", "")
                    # Flexible match (ignore case)
                    if db_title_match.lower() in title.lower():
                        logger.debug(f"find_child_database match found → db {b_id[:8]}... ('{title}') after {blocks_checked} block(s)")
                        return b_id

                # 2. Is it a container that could have the DB inside?
                # Notion has many container types. If it has 'has_children', we enter.
                if block.get("has_children", False):
                    # Common types where DBs hide:
                    # toggle, column_list, column, callout, synced_block, etc.
                    queue.append((b_id, depth + 1))

        logger.debug(f"find_child_database no match for '{db_title_match}' after {blocks_checked} block(s)")
        return None
