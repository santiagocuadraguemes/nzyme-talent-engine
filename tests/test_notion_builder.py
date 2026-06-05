"""
VT-1 (parte): atribución de Source y contrato de escritura del Main DB.
Valida el append acumulativo y de-duplicado (RF-10), que Source no se escribe
cuando no se aporta, y que Creator nunca es escrito por el código.
"""
from core.notion_builder import NotionBuilder
from core.constants import PROP_SOURCE, PROP_CREATOR


# --- _merge_tag: append acumulativo, de-duplicado case-insensitive ---

def test_merge_tag_appends_new():
    assert NotionBuilder._merge_tag(["A"], "B") == ["A", "B"]


def test_merge_tag_dedup_case_insensitive():
    # No re-añade un tag que ya existe ignorando mayúsculas; conserva el original
    assert NotionBuilder._merge_tag(["Applied via LinkedIn"], "applied via linkedin") == ["Applied via LinkedIn"]


def test_merge_tag_preserves_order():
    assert NotionBuilder._merge_tag(["Headhunter - BAON", "Applied via LinkedIn"], "Headhunter - X") == \
        ["Headhunter - BAON", "Applied via LinkedIn", "Headhunter - X"]


def test_merge_tag_handles_none_inputs():
    assert NotionBuilder._merge_tag(None, "A") == ["A"]
    assert NotionBuilder._merge_tag(["A"], None) == ["A"]


# --- build_candidate_payload: contrato de Source y Creator ---

_BASE = {
    "name": "Ada Lovelace",
    "experience": {},
    "education": {},
    "general": {},
    "total_years": 0,
    "languages": [],
}


def test_payload_source_absent_when_not_provided():
    props = NotionBuilder.build_candidate_payload(_BASE, None, "Proc X", source=None)
    assert PROP_SOURCE not in props


def test_payload_source_appended_to_existing():
    props = NotionBuilder.build_candidate_payload(
        _BASE, None, "Proc X",
        source="Applied via LinkedIn",
        existing_source_tags=["Headhunter - BAON"],
    )
    names = [t["name"] for t in props[PROP_SOURCE]["multi_select"]]
    assert names == ["Headhunter - BAON", "Applied via LinkedIn"]


def test_payload_never_writes_creator():
    props = NotionBuilder.build_candidate_payload(
        _BASE, None, "Proc X", source="Applied via LinkedIn",
    )
    assert PROP_CREATOR not in props
    assert "Creator" not in props
