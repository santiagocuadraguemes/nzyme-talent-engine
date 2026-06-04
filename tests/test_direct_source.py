"""
Direct-entry source attribution (HarvesterRelational._resolve_direct_source) and
the Step 2.5 grace-period filter (HarvesterRelational._build_direct_intake_filter).

The filter-shape tests guard against the silent-[] failure mode: a filter rejected
by the Notion API makes query_data_source return [] without raising, which would
silently disable the Step 2.5 safety net.
"""
from datetime import datetime, timedelta, timezone

from scripts.harvester import HarvesterRelational
from core.constants import (
    BULK_IMPORT_TITLE_PREFIX, DIRECT_INTAKE_GRACE_MINUTES,
    SOURCE_HEADHUNTER_PREFIX, SOURCE_HEADHUNTER_FALLBACK,
    PROP_CHECKBOX_PROCESSED, PROP_ID, PROP_NAME,
)


# --- _resolve_direct_source ---

def test_per_candidate_firm_wins_over_bulk_prefix():
    source = HarvesterRelational._resolve_direct_source(
        "BAON", f"{BULK_IMPORT_TITLE_PREFIX}cv.pdf", {"headhunter_name": "OtherFirm"}
    )
    assert source == f"{SOURCE_HEADHUNTER_PREFIX}BAON"


def test_bulk_prefix_falls_back_to_process_headhunter():
    source = HarvesterRelational._resolve_direct_source(
        None, f"{BULK_IMPORT_TITLE_PREFIX}cv.pdf", {"headhunter_name": "BAON"}
    )
    assert source == f"{SOURCE_HEADHUNTER_PREFIX}BAON"


def test_bulk_prefix_without_process_headhunter_uses_fallback():
    # headhunter_name NULL on the process row
    assert HarvesterRelational._resolve_direct_source(
        None, f"{BULK_IMPORT_TITLE_PREFIX}cv.pdf", {"headhunter_name": None}
    ) == SOURCE_HEADHUNTER_FALLBACK
    # key missing entirely (rows that predate the column)
    assert HarvesterRelational._resolve_direct_source(
        None, f"{BULK_IMPORT_TITLE_PREFIX}cv.pdf", {}
    ) == SOURCE_HEADHUNTER_FALLBACK


def test_manual_direct_entry_gets_no_source():
    # Non-import title + no relation → Source left untouched (user-managed)
    assert HarvesterRelational._resolve_direct_source(None, "Jane Doe", {"headhunter_name": "BAON"}) is None


def test_relation_firm_applies_to_manual_entries_too():
    source = HarvesterRelational._resolve_direct_source("BAON", "Jane Doe", {})
    assert source == f"{SOURCE_HEADHUNTER_PREFIX}BAON"


def test_resolve_direct_source_is_none_safe():
    assert HarvesterRelational._resolve_direct_source(None, None, None) is None


# --- _build_direct_intake_filter ---

def test_intake_filter_shape():
    now = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)
    f = HarvesterRelational._build_direct_intake_filter(now=now)
    conditions = f["and"]
    assert len(conditions) == 4
    assert {"property": PROP_CHECKBOX_PROCESSED, "checkbox": {"equals": False}} in conditions
    assert {"property": PROP_ID, "rich_text": {"is_empty": True}} in conditions
    assert {"property": PROP_NAME, "title": {"is_not_empty": True}} in conditions


def test_intake_filter_grace_cutoff_uses_builtin_timestamp():
    now = datetime(2026, 6, 4, 12, 0, 0, tzinfo=timezone.utc)
    f = HarvesterRelational._build_direct_intake_filter(now=now)
    ts_conditions = [c for c in f["and"] if "timestamp" in c]
    assert len(ts_conditions) == 1
    cond = ts_conditions[0]
    # Built-in timestamp filter — never depends on a schema property
    assert cond["timestamp"] == "created_time"
    expected_cutoff = (now - timedelta(minutes=DIRECT_INTAKE_GRACE_MINUTES)).isoformat()
    assert cond["created_time"] == {"before": expected_cutoff}
    # Valid ISO-8601 with timezone (Notion requires it)
    parsed = datetime.fromisoformat(cond["created_time"]["before"])
    assert parsed.tzinfo is not None
