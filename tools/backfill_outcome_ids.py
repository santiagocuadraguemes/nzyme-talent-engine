"""
Backfills notion_outcome_id on NzymeRecruitingApplications.

Iterates all applications, fetches each Workflow page's children from Notion,
finds the "Process Outcome Form" child database, and stores its database UUID.

Usage:
    python tools/backfill_outcome_ids.py              # Live run
    python tools/backfill_outcome_ids.py --dry-run    # Preview without writing
"""
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from dotenv import load_dotenv
from core.supabase_client import SupabaseManager

load_dotenv()

NOTION_KEY = os.getenv("NOTION_KEY")
HEADERS = {
    "Authorization": f"Bearer {NOTION_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2025-09-03",
}

OUTCOME_DB_TITLE = "Process Outcome Form"


def api_request(method, url, json=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = httpx.request(method, url, headers=HEADERS, json=json, timeout=30)
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", 1))
                print(f"  Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue
            return resp
        except httpx.HTTPError as e:
            print(f"  Request error: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    return None


def find_outcome_db_in_children(block_id, depth=0):
    """
    Recursively searches children of a block for a child database
    named 'Process Outcome Form'. Returns the database UUID or None.
    Recurses into container blocks (columns, toggles, etc.) up to 4 levels deep.
    """
    if depth > 4:
        return None

    start_cursor = None

    while True:
        url = f"https://api.notion.com/v1/blocks/{block_id}/children?page_size=100"
        if start_cursor:
            url += f"&start_cursor={start_cursor}"

        resp = api_request("GET", url)
        if not resp or resp.status_code != 200:
            return None

        data = resp.json()
        for block in data.get("results", []):
            if block.get("type") == "child_database":
                title = block.get("child_database", {}).get("title", "")
                if title.strip() == OUTCOME_DB_TITLE:
                    return block["id"]

            # Recurse into container blocks
            if block.get("has_children"):
                result = find_outcome_db_in_children(block["id"], depth + 1)
                if result:
                    return result

        if not data.get("has_more"):
            break
        start_cursor = data.get("next_cursor")

    return None


def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("=== DRY RUN MODE (no writes) ===\n")
    else:
        print("=== LIVE MODE ===\n")

    # 1. Load all applications from Supabase
    supa = SupabaseManager()
    apps_res = supa.client.table("NzymeRecruitingApplications") \
        .select("id, notion_page_id, notion_outcome_id") \
        .execute()
    apps = [a for a in apps_res.data if a.get("notion_page_id")]
    print(f"Loaded {len(apps)} applications with workflow page IDs\n")

    updated = 0
    skipped_no_db = 0
    skipped_already_correct = 0
    replaced = 0
    failed = 0

    for i, app in enumerate(apps, 1):
        app_id = app["id"]
        workflow_page_id = app["notion_page_id"]
        existing_outcome_id = app.get("notion_outcome_id")

        print(f"[{i}/{len(apps)}] App {app_id[:8]}... (workflow: {workflow_page_id[:8]}...)")

        # Fetch children of the Workflow page, find Outcome Form DB
        outcome_db_id = find_outcome_db_in_children(workflow_page_id)

        if not outcome_db_id:
            print(f"  -> No '{OUTCOME_DB_TITLE}' found, skipping")
            skipped_no_db += 1
            continue

        # Skip if already set to the correct value
        if existing_outcome_id == outcome_db_id:
            print(f"  -> Already correct ({outcome_db_id[:8]}...)")
            skipped_already_correct += 1
            continue

        was_wrong = existing_outcome_id is not None
        label = "Replacing" if was_wrong else "Setting"
        print(f"  -> {label}: {outcome_db_id}")

        if not dry_run:
            success = supa.update_application_outcome_id(app_id, outcome_db_id)
            if success:
                updated += 1
                if was_wrong:
                    replaced += 1
            else:
                print(f"  -> FAILED to update")
                failed += 1
        else:
            updated += 1
            if was_wrong:
                replaced += 1

    print(f"\n--- Summary ---")
    print(f"{'Would update' if dry_run else 'Updated'}: {updated} ({replaced} replaced wrong IDs)")
    print(f"Already correct: {skipped_already_correct}")
    print(f"No Outcome Form found: {skipped_no_db}")
    if failed:
        print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
