# tools/test_template_injection.py
"""
Interactive test for Notion API template injection.

Goal: verify that the heavy candidate template can be applied to a freshly
created Workflow-DB page purely via the API — the prerequisite for collapsing
the Harvester's Form → Workflow hop into a single "form-direct-in-Workflow" step.

Usage:
    python tools/test_template_injection.py <workflow_db_id>
    python tools/test_template_injection.py          # prompts for the DB id

Flow (mirrors FactoryWorker._apply_template_to_page):
    1. Resolve the data source id for the DB
    2. GET /data_sources/{ds}/templates  → list templates
    3. Prompt you to pick one
    4. Create a blank page in the DB (title = "Template Injection Test - DELETE ME")
    5. PATCH /pages/{id} with {template: template_id, erase_content: true}
    6. Print the resulting page URL so you can eyeball the injected content

Nothing here writes to Supabase or touches production logic — it only creates
one throwaway Notion page that you delete afterwards.
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from core.notion_client import NotionClient

load_dotenv()

TEST_PAGE_TITLE = "Template Injection Test - DELETE ME"


def _find_title_prop(notion: NotionClient, ds_id: str) -> str:
    """Return the name of the DB's title property (Notion requires it on create)."""
    schema = notion.get_database_schema(ds_id)
    for name, spec in schema.items():
        if spec.get("type") == "title":
            return name
    # Fallback — virtually every Notion DB calls it "Name"
    return "Name"


def main():
    notion = NotionClient()
    if not notion.token:
        print("[ERROR] NOTION_KEY is not set in the environment / .env")
        sys.exit(1)

    db_id = sys.argv[1].strip() if len(sys.argv) > 1 else input("Workflow DB id: ").strip()
    if not db_id:
        print("[ERROR] No DB id provided.")
        sys.exit(1)

    # 1. Resolve data source id
    ds_id = notion.get_data_source_id(db_id)
    if not ds_id:
        print(f"[ERROR] Could not resolve a data source for {db_id}.")
        print("        Check the id is a database (not a data source / page) and that")
        print("        the integration has access to it.")
        sys.exit(1)
    print(f"[OK] Data source: {ds_id}")

    # 2. List templates
    resp = notion.client.get(f"{notion.base_url}/data_sources/{ds_id}/templates")
    if resp.status_code != 200:
        print(f"[ERROR] List templates failed ({resp.status_code}): {resp.text[:300]}")
        sys.exit(1)

    templates = resp.json().get("templates", [])
    if not templates:
        print("[ERROR] This database has no templates.")
        sys.exit(1)

    print(f"\nFound {len(templates)} template(s):\n")
    for i, t in enumerate(templates, 1):
        default = "  (default)" if t.get("is_default") else ""
        print(f"  [{i}] {t.get('name', '<unnamed>')}{default}")
        print(f"      id: {t.get('id')}")

    # 3. Choose
    raw = input(f"\nChoose a template [1-{len(templates)}]: ").strip()
    try:
        idx = int(raw) - 1
        chosen = templates[idx]
        assert 0 <= idx < len(templates)
    except (ValueError, AssertionError, IndexError):
        print("[ERROR] Invalid choice.")
        sys.exit(1)

    template_id = chosen.get("id")
    print(f"\n[OK] Chosen: '{chosen.get('name')}' ({template_id})")

    # 4. Create a blank page
    title_prop = _find_title_prop(notion, ds_id)
    print(f"[..] Creating test page (title property: '{title_prop}')...")
    create_resp = notion.create_page(
        db_id,
        properties={title_prop: {"title": [{"text": {"content": TEST_PAGE_TITLE}}]}},
    )
    if create_resp.status_code != 200:
        print(f"[ERROR] Create page failed ({create_resp.status_code}): {create_resp.text[:300]}")
        sys.exit(1)

    page = create_resp.json()
    page_id = page["id"]
    print(f"[OK] Page created: {page_id}")

    # 5. Apply the template
    print("[..] Applying template (erase_content=true)...")
    patch_resp = notion.client.patch(
        f"{notion.base_url}/pages/{page_id}",
        json={
            "template": {"type": "template_id", "template_id": template_id},
            "erase_content": True,
        },
    )
    if patch_resp.status_code != 200:
        print(f"[ERROR] Apply template FAILED ({patch_resp.status_code}): {patch_resp.text[:500]}")
        print(f"\nThe blank page still exists for inspection: {page.get('url', page_id)}")
        sys.exit(1)

    print("[OK] Template applied successfully (HTTP 200).")
    print(f"\n  Inspect the result here:\n  {patch_resp.json().get('url', page.get('url', page_id))}")
    print(f"\n  Page id (to delete afterwards): {page_id}")
    print(
        "\nNote: Notion builds template child databases asynchronously — give it a few"
        "\nseconds, then refresh the page to see the full injected content."
    )


if __name__ == "__main__":
    main()
