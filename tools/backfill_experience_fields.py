"""
Backfills 6 missing experience fields in candidate_data JSONB.

Fields: finance, marketing, operations, product, sales_revenue, technology.
These were written to Notion by DomainMapper but never read back by NotionParser.

Does a targeted merge — only adds the 6 keys to candidate_data.experience
without touching any other data.

Usage:
    python tools/backfill_experience_fields.py              # Live run
    python tools/backfill_experience_fields.py --dry-run    # Preview without writing
"""
import sys
import os
import time
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from dotenv import load_dotenv
from core.supabase_client import SupabaseManager
from core.domain_mapper import DomainMapper
from core.constants import (
    PROP_EXP_FINANCE, PROP_EXP_MARKETING, PROP_EXP_OPERATIONS,
    PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE, PROP_EXP_TECHNOLOGY,
)

load_dotenv()

NOTION_KEY = os.getenv("NOTION_KEY")
HEADERS = {
    "Authorization": f"Bearer {NOTION_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2025-09-03",
}

NEW_FIELDS_MAP = {
    PROP_EXP_FINANCE: "finance",
    PROP_EXP_MARKETING: "marketing",
    PROP_EXP_OPERATIONS: "operations",
    PROP_EXP_PRODUCT: "product",
    PROP_EXP_SALES_REVENUE: "sales_revenue",
    PROP_EXP_TECHNOLOGY: "technology",
}


def api_request(method, url, json_body=None, max_retries=3):
    for attempt in range(max_retries):
        try:
            resp = httpx.request(method, url, headers=HEADERS, json=json_body, timeout=30)
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


def extract_tags(prop_data):
    """Extract multi-select tag names from a Notion property."""
    if not prop_data:
        return []
    if "multi_select" in prop_data:
        return [item["name"] for item in prop_data["multi_select"]]
    if "select" in prop_data and prop_data["select"]:
        return [prop_data["select"]["name"]]
    return []


def fetch_notion_page(page_id):
    resp = api_request("GET", f"https://api.notion.com/v1/pages/{page_id}")
    if resp and resp.status_code == 200:
        return resp.json()
    return None


def parse_new_experience_fields(properties):
    """Parse only the 6 new experience fields from Notion page properties."""
    result = {}
    for notion_col, json_key in NEW_FIELDS_MAP.items():
        tags = extract_tags(properties.get(notion_col))
        result[json_key] = DomainMapper.reconstruct_experience_object(tags)
    return result


def main():
    dry_run = "--dry-run" in sys.argv

    if dry_run:
        print("=== DRY RUN MODE (no writes) ===\n")
    else:
        print("=== LIVE MODE ===\n")

    # 1. Load all candidates from Supabase
    supa = SupabaseManager()
    candidates_res = supa.client.table("NzymeTalentNetwork") \
        .select("id, name, notion_page_id, candidate_data") \
        .not_.is_("notion_page_id", "null") \
        .execute()
    candidates = candidates_res.data
    print(f"Loaded {len(candidates)} candidates with Notion page IDs\n")

    updated = 0
    skipped_has_data = 0
    skipped_no_props = 0
    failed = 0

    for i, cand in enumerate(candidates, 1):
        cand_id = cand["id"]
        page_id = cand["notion_page_id"]
        name = cand.get("name", "???")
        existing_data = cand.get("candidate_data") or {}

        print(f"[{i}/{len(candidates)}] {name} ({cand_id[:8]}...)")

        # Check if all 6 fields already exist in candidate_data.experience
        experience = existing_data.get("experience", {})
        missing_keys = [k for k in NEW_FIELDS_MAP.values() if k not in experience]

        if not missing_keys:
            print(f"  -> All 6 fields already present, skipping")
            skipped_has_data += 1
            continue

        print(f"  -> Missing: {', '.join(missing_keys)}")

        # Fetch Notion page
        page = fetch_notion_page(page_id)
        if not page or "properties" not in page:
            print(f"  -> Failed to fetch Notion page, skipping")
            skipped_no_props += 1
            failed += 1
            continue

        # Parse the 6 new fields
        new_fields = parse_new_experience_fields(page["properties"])

        # Merge into existing candidate_data
        if "experience" not in existing_data:
            existing_data["experience"] = {}
        for key, value in new_fields.items():
            existing_data["experience"][key] = value

        has_any = any(
            v and v.get("has_experience")
            for k, v in new_fields.items()
            if v is not None
        )
        print(f"  -> Parsed ({'+data' if has_any else 'empty'}) — updating")

        if not dry_run:
            try:
                supa.client.table("NzymeTalentNetwork").update({
                    "candidate_data": existing_data,
                    "updated_at": "now()"
                }).eq("id", cand_id).execute()
                updated += 1
            except Exception as e:
                print(f"  -> FAILED: {e}")
                failed += 1
        else:
            updated += 1

        # Respect Notion rate limits (~3 req/s)
        time.sleep(0.35)

    print(f"\n--- Summary ---")
    print(f"{'Would update' if dry_run else 'Updated'}: {updated}")
    print(f"Already had all 6 fields: {skipped_has_data}")
    print(f"Failed to fetch from Notion: {skipped_no_props}")
    if failed:
        print(f"Total failures: {failed}")


if __name__ == "__main__":
    main()
