"""
Prints the schema (property names and types) of a Notion database.

Usage:
    python tools/notion_schema.py              # Main DB (NOTION_MAIN_DB_ID)
    python tools/notion_schema.py <db_id>      # Any database by ID
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from core.notion_client import NotionClient

load_dotenv()


def print_schema(db_id):
    client = NotionClient()
    ds_id = client.get_data_source_id(db_id)
    if not ds_id:
        print(f"Could not resolve data source for DB: {db_id}")
        return

    schema = client.get_database_schema(ds_id)
    if not schema:
        print(f"No schema returned for data source: {ds_id}")
        return

    print(f"Database: {db_id}")
    print(f"Data Source: {ds_id}")
    print(f"Properties ({len(schema)}):\n")

    for name, details in sorted(schema.items()):
        prop_type = details.get("type", "unknown")
        extra = ""

        if prop_type == "select":
            options = [o["name"] for o in details.get("select", {}).get("options", [])]
            if options:
                extra = f"  options: {options}"
        elif prop_type == "status":
            options = [o["name"] for o in details.get("status", {}).get("options", [])]
            if options:
                extra = f"  options: {options}"
        elif prop_type == "multi_select":
            options = [o["name"] for o in details.get("multi_select", {}).get("options", [])]
            if options:
                extra = f"  options({len(options)}): {options[:10]}{'...' if len(options) > 10 else ''}"

        print(f"  {name}: {prop_type}{extra}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        target_db = sys.argv[1]
    else:
        target_db = os.getenv("NOTION_MAIN_DB_ID")
        if not target_db:
            print("No DB ID provided and NOTION_MAIN_DB_ID not set in .env")
            sys.exit(1)

    print_schema(target_db)
