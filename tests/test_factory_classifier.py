"""
Child-block classification in the Factory (FactoryWorkerV2._classify_child_blocks).
The aux Form child DB is gone — the candidate form lives embedded inside a regular
child page ("form" in title → form_page_id). A leftover Form child DB is surfaced
as legacy_form_db_id (warned and skipped, never renamed or registered). Keyword
order: child_database arm checks "feedback" before "bulk" before the legacy "form";
child_page arm checks "form" LAST so a renamed JD page for a process whose name
contains "form" (e.g. "...Transformation Lead") never steals the form page slot.
"""
from scripts.factory_worker import FactoryWorkerV2


def _db(block_id, title):
    return {"type": "child_database", "id": block_id, "child_database": {"title": title}}


def _page(block_id, title):
    return {"type": "child_page", "id": block_id, "child_page": {"title": title}}


def test_full_template_classifies_everything():
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _page("form", "TEMPLATE CANDIDATE FORM"),
        _db("bulk", "TEMPLATE BULK IMPORT"),
        _db("fb", "TEMPLATE FEEDBACK UPLOAD"),
        _page("jd", "Job Description"),
        _page("is", "Interview Stages"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result == {
        "wf_db_id": "wf",
        "bulk_db_id": "bulk",
        "feedback_db_id": "fb",
        "legacy_form_db_id": None,
        "form_page_id": "form",
        "jd_page_id": "jd",
        "interview_stages_page_id": "is",
    }


def test_leftover_form_db_is_reported_as_legacy_not_form_page():
    # Un-migrated template: the old aux Form child DB was never deleted. It must
    # land in legacy_form_db_id (caller warns + skips), never in form_page_id.
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("oldform", "TEMPLATE CANDIDATE FORM"),
        _db("bulk", "TEMPLATE BULK IMPORT"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["legacy_form_db_id"] == "oldform"
    assert result["form_page_id"] is None


def test_missing_form_page_leaves_none():
    # Migrated template where the form page was forgotten — everything else
    # still classifies; form_page_id stays None (caller warns + skips rename).
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("bulk", "TEMPLATE BULK IMPORT"),
        _db("fb", "TEMPLATE FEEDBACK UPLOAD"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["form_page_id"] is None
    assert result["legacy_form_db_id"] is None
    assert result["wf_db_id"] == "wf"
    assert result["bulk_db_id"] == "bulk"
    assert result["feedback_db_id"] == "fb"


def test_bulk_title_containing_form_is_classified_as_bulk():
    # The production rename gives the bulk DB a title containing "form" — on an
    # idempotent retry it must still classify as bulk, not as a legacy form DB.
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("bulk", "Bulk Candidate Application Upload Form - NZ Test"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["bulk_db_id"] == "bulk"
    assert result["legacy_form_db_id"] is None


def test_renamed_titles_retry_case_still_correct():
    # Titles as they look AFTER configure_process renamed them (idempotent re-run).
    blocks = [
        _db("wf", "Feedback Tool & Workflow - NZ Test"),
        _page("form", "Single Candidate Application Upload Form - NZ Test"),
        _db("bulk", "Bulk Candidate Application Upload Form - NZ Test"),
        _db("fb", "Bulk & Single Feedback Upload Form - NZ Test"),
        _page("jd", "Role & Candidate Description - NZ Test"),
        _page("is", "Interview Stages - NZ Test"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["wf_db_id"] == "wf"          # "workflow" wins over "feedback"
    assert result["feedback_db_id"] == "fb"    # "feedback" wins over "form"/"bulk"
    assert result["bulk_db_id"] == "bulk"      # "bulk" wins over legacy "form"
    assert result["form_page_id"] == "form"
    assert result["jd_page_id"] == "jd"
    assert result["interview_stages_page_id"] == "is"


def test_process_name_containing_form_does_not_steal_form_page_slot():
    # "Transformation" contains "form". On an idempotent retry the renamed JD
    # page must still match "role" (checked first), and the renamed form page
    # must still match "form".
    name = "NZ Product & Transformation Lead 2026Q1"
    blocks = [
        _db("wf", f"Feedback Tool & Workflow - {name}"),
        _page("jd", f"Role & Candidate Description - {name}"),
        _page("form", f"Single Candidate Application Upload Form - {name}"),
        _page("is", f"Interview Stages - {name}"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["jd_page_id"] == "jd"
    assert result["form_page_id"] == "form"
    assert result["interview_stages_page_id"] == "is"


def test_unrelated_blocks_are_ignored():
    blocks = [
        {"type": "paragraph", "id": "p1"},
        _page("other", "Notes"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert all(v is None for v in result.values())
