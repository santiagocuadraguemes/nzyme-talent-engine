"""
Child-DB classification in the Factory (FactoryWorkerV2._classify_child_blocks).
The Form DB is optional (form-direct intake templates omit it) and the bulk check
must run BEFORE the form check so a bulk DB whose title contains "form" is never
misclassified as the Form DB.
"""
from scripts.factory_worker import FactoryWorkerV2


def _db(block_id, title):
    return {"type": "child_database", "id": block_id, "child_database": {"title": title}}


def _page(block_id, title):
    return {"type": "child_page", "id": block_id, "child_page": {"title": title}}


def test_full_template_classifies_everything():
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("form", "TEMPLATE CANDIDATE FORM"),
        _db("bulk", "TEMPLATE BULK IMPORT"),
        _db("fb", "TEMPLATE FEEDBACK UPLOAD"),
        _page("jd", "Job Description"),
        _page("is", "Interview Stages"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result == {
        "wf_db_id": "wf",
        "form_db_id": "form",
        "bulk_db_id": "bulk",
        "feedback_db_id": "fb",
        "jd_page_id": "jd",
        "interview_stages_page_id": "is",
    }


def test_formless_template_leaves_form_none():
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("bulk", "TEMPLATE BULK IMPORT"),
        _db("fb", "TEMPLATE FEEDBACK UPLOAD"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["form_db_id"] is None
    assert result["wf_db_id"] == "wf"
    assert result["bulk_db_id"] == "bulk"
    assert result["feedback_db_id"] == "fb"


def test_bulk_title_containing_form_is_classified_as_bulk():
    # The production rename gives the bulk DB a title containing "form" — on an
    # idempotent retry it must still classify as bulk, not steal the form slot.
    blocks = [
        _db("wf", "TEMPLATE WORKFLOW"),
        _db("bulk", "Bulk Candidate Application Upload Form - NZ Test"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["bulk_db_id"] == "bulk"
    assert result["form_db_id"] is None


def test_renamed_titles_retry_case_still_correct():
    # Titles as they look AFTER configure_process renamed them (idempotent re-run).
    blocks = [
        _db("wf", "Feedback Tool & Workflow - NZ Test"),
        _db("form", "Single Candidate Application Upload Form - NZ Test"),
        _db("bulk", "Bulk Candidate Application Upload Form - NZ Test"),
        _db("fb", "Bulk & Single Feedback Upload Form - NZ Test"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert result["wf_db_id"] == "wf"          # "workflow" wins over "feedback"
    assert result["feedback_db_id"] == "fb"    # "feedback" wins over "form"/"bulk"
    assert result["bulk_db_id"] == "bulk"      # "bulk" wins over "form"
    assert result["form_db_id"] == "form"


def test_unrelated_blocks_are_ignored():
    blocks = [
        {"type": "paragraph", "id": "p1"},
        _page("other", "Notes"),
    ]
    result = FactoryWorkerV2._classify_child_blocks(blocks)
    assert all(v is None for v in result.values())
