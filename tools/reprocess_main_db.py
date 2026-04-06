"""
One-off script: reprocess all unprocessed candidates in Main DB in parallel.

Replicates Observer's _logic_enrich_cv / _logic_enrich_linkedin but with
ThreadPoolExecutor for concurrent processing.

Usage:
    python tools/reprocess_main_db.py                    # Live run, 5 workers
    python tools/reprocess_main_db.py --dry-run           # Preview only
    python tools/reprocess_main_db.py --workers 8         # 8 parallel workers
    python tools/reprocess_main_db.py --cv-only           # Only candidates with CVs
    python tools/reprocess_main_db.py --no-cv-only        # Only candidates without CVs
"""
import sys
import os
import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import CVAnalyzer
from core.notion_builder import NotionBuilder
from core.domain_mapper import DomainMapper
from core.utils import download_file
from core.constants import (
    PROP_CHECKBOX_PROCESSED, PROP_CV_FILES, PROP_LINKEDIN, PROP_EMAIL, PROP_PHONE,
    PROP_PROCESS_HISTORY, PROP_TEAM_ROLE,
)

MAIN_DB_ID = os.getenv("NOTION_MAIN_DB_ID")
TEMP_FOLDER = "/tmp/reprocess_downloads"
os.makedirs(TEMP_FOLDER, exist_ok=True)


def process_candidate(page, notion, supa, storage, ai, exa, dry_run=False):
    """Process a single candidate. Returns (name, status) tuple."""
    page_id = page["id"]
    props = page["properties"]

    # Extract name for logging
    name_prop = props.get("Name", {})
    title_parts = name_prop.get("title", [])
    name = title_parts[0]["plain_text"] if title_parts else f"Unknown ({page_id[:8]})"

    if dry_run:
        cv_files = props.get("CV", {}).get("files", [])
        linkedin = props.get(PROP_LINKEDIN, {}).get("url")
        source = "CV" if cv_files else ("LinkedIn" if linkedin else "No source")
        return (name, f"[DRY RUN] Would reprocess via {source}")

    # --- PATH A: CV enrichment ---
    cv_files = props.get(PROP_CV_FILES, {}).get("files", [])
    if cv_files:
        file_obj = cv_files[0]
        file_type = file_obj.get("type")
        file_name = file_obj.get("name", "cv.pdf")

        # Handle both Notion-hosted ("file") and external ("external") CV URLs
        if file_type == "file":
            url = file_obj.get("file", {}).get("url")
        elif file_type == "external":
            url = file_obj.get("external", {}).get("url")
        else:
            return (name, f"SKIP: Unknown CV type '{file_type}'")

        if not url:
            return (name, "SKIP: No CV URL")

        # For external (already in Supabase Storage), keep existing URL
        public_url = url if file_type == "external" else None

        # Use page_id prefix for unique temp file names (thread safety)
        safe_name = f"{page_id[:8]}_{file_name}"
        local_path = download_file(url, safe_name, TEMP_FOLDER)
        if not local_path:
            return (name, "FAIL: Could not download CV")

        # Only upload to storage if it's a Notion-hosted file (not already in Supabase)
        if not public_url:
            public_url = storage.upload_cv_from_url(url, file_name)
            if not public_url:
                try: os.remove(local_path)
                except OSError: pass
                return (name, "FAIL: Could not upload to storage")

        ai_data = ai.process_cv(local_path)
        try: os.remove(local_path)
        except OSError: pass

        if not ai_data:
            return (name, "FAIL: AI parsing failed")

        # Preserve existing page values
        curr_hist = [t["name"] for t in props.get(PROP_PROCESS_HISTORY, {}).get("multi_select", [])]
        curr_role = [t["name"] for t in props.get(PROP_TEAM_ROLE, {}).get("multi_select", [])]
        curr_proc_obj = props.get("Last Process Involved in", {}).get("select")
        curr_proc = curr_proc_obj["name"] if curr_proc_obj else "Referral/General"

        payload = NotionBuilder.build_candidate_payload(
            ai_data, public_url, curr_proc,
            existing_history=curr_hist, existing_team_role=curr_role
        )
        payload[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
        res = notion.update_page(page_id, payload)
        if res.status_code != 200:
            return (name, f"FAIL: Notion update {res.status_code}")

        supa_data = DomainMapper.map_to_supabase_candidate(ai_data, public_url)
        supa.manage_candidate(supa_data, page_id)

        return (name, "OK (CV)")

    # --- PATH B: LinkedIn enrichment ---
    linkedin_url = props.get(PROP_LINKEDIN, {}).get("url")
    if linkedin_url and exa:
        try:
            linkedin_text = exa.get_linkedin_profile(linkedin_url)
            if not linkedin_text:
                return (name, "FAIL: Could not fetch LinkedIn")

            ai_data = ai.process_linkedin(linkedin_text)
            if not ai_data:
                return (name, "FAIL: AI LinkedIn parsing failed")

            ai_data["linkedin_url"] = linkedin_url
            if not ai_data.get("email"):
                ai_data["email"] = props.get(PROP_EMAIL, {}).get("email")
            if not ai_data.get("phone"):
                ai_data["phone"] = props.get(PROP_PHONE, {}).get("phone_number")

            curr_hist = [t["name"] for t in props.get(PROP_PROCESS_HISTORY, {}).get("multi_select", [])]
            curr_role = [t["name"] for t in props.get(PROP_TEAM_ROLE, {}).get("multi_select", [])]
            curr_proc_obj = props.get("Last Process Involved in", {}).get("select")
            curr_proc = curr_proc_obj["name"] if curr_proc_obj else "Referral/General"

            payload = NotionBuilder.build_candidate_payload(
                ai_data, None, curr_proc,
                existing_history=curr_hist, existing_team_role=curr_role
            )
            payload.pop(PROP_PROCESS_HISTORY, None)
            payload.pop(PROP_TEAM_ROLE, None)
            payload[PROP_CHECKBOX_PROCESSED] = {"checkbox": True}
            res = notion.update_page(page_id, payload)
            if res.status_code != 200:
                return (name, f"FAIL: Notion update {res.status_code}")

            supa_data = DomainMapper.map_to_supabase_candidate(ai_data, None)
            supa.manage_candidate(supa_data, page_id)

            return (name, "OK (LinkedIn)")
        except Exception as e:
            return (name, f"FAIL: LinkedIn error: {e}")

    # --- PATH C: No source, just mark processed ---
    payload = {PROP_CHECKBOX_PROCESSED: {"checkbox": True}}
    notion.update_page(page_id, payload)
    # Still sync basic data to Supabase
    from core.notion_parser import NotionParser
    data_update = NotionParser.parse_candidate_properties(props)
    supa.manage_candidate(data_update, page_id)
    return (name, "OK (no source, synced basic data)")


def main():
    parser = argparse.ArgumentParser(description="Reprocess unprocessed Main DB candidates in parallel")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--workers", type=int, default=5, help="Number of parallel workers (default: 5)")
    parser.add_argument("--cv-only", action="store_true", help="Only process candidates with CVs")
    parser.add_argument("--no-cv-only", action="store_true", help="Only process candidates without CVs")
    args = parser.parse_args()

    print(f"=== {'DRY RUN' if args.dry_run else 'LIVE'} MODE | {args.workers} workers ===\n")

    # Initialize shared clients
    notion = NotionClient()
    supa = SupabaseManager()
    storage = StorageClient()
    ai = CVAnalyzer()

    exa = None
    try:
        from core.exa_client import ExaClient
        exa = ExaClient()
        print("Exa client loaded (LinkedIn enrichment available)")
    except Exception:
        print("Exa client not available (LinkedIn enrichment disabled)")

    # Query unprocessed candidates
    ds_id = notion.get_data_source_id(MAIN_DB_ID)
    if not ds_id:
        print("ERROR: Could not resolve Main DB data source")
        return

    pages = notion.query_data_source(ds_id, {"property": "Processed", "checkbox": {"equals": False}})
    print(f"Found {len(pages)} unprocessed candidates")

    # Filter by type if requested
    if args.cv_only:
        pages = [p for p in pages if p.get("properties", {}).get("CV", {}).get("files", [])]
        print(f"Filtered to {len(pages)} candidates with CVs")
    elif args.no_cv_only:
        pages = [p for p in pages if not p.get("properties", {}).get("CV", {}).get("files", [])]
        print(f"Filtered to {len(pages)} candidates without CVs")

    if not pages:
        print("Nothing to process.")
        return

    print(f"\nStarting parallel processing...\n")
    start = time.time()

    ok = 0
    failed = 0
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_candidate, page, notion, supa, storage, ai, exa, args.dry_run): page
            for page in pages
        }

        for i, future in enumerate(as_completed(futures), 1):
            try:
                name, status = future.result()
                results.append((name, status))
                is_ok = status.startswith("OK") or status.startswith("[DRY")
                if is_ok:
                    ok += 1
                else:
                    failed += 1
                marker = "+" if is_ok else "X"
                print(f"[{i}/{len(pages)}] [{marker}] {name}: {status}")
            except Exception as e:
                failed += 1
                page = futures[future]
                print(f"[{i}/{len(pages)}] [X] {page['id'][:8]}...: EXCEPTION: {e}")

    elapsed = time.time() - start
    print(f"\n--- Summary ---")
    print(f"Total: {len(pages)} | OK: {ok} | Failed: {failed}")
    print(f"Time: {elapsed:.1f}s ({elapsed/len(pages):.1f}s per candidate)")

    if failed:
        print(f"\nFailed candidates:")
        for name, status in results:
            if not status.startswith("OK") and not status.startswith("[DRY"):
                print(f"  - {name}: {status}")


if __name__ == "__main__":
    main()
