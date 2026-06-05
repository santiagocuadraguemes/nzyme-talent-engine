"""
VT-1 (parte): motor de resolución de identidad de 4 reglas (RF-09).
Valida la lógica de fusión/separación de candidatos con un cliente de base de
datos falso (sin red).

Se omite si la librería supabase no está instalada (la importa core.supabase_client).
En el runtime de Lambda (Python 3.11) la dependencia está presente y el test corre.
"""
import pytest

pytest.importorskip("supabase")

from core.supabase_client import SupabaseManager
from core.logger import get_logger


class _Resp:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, rows_fn):
        self._fn = rows_fn
        self._col = None
        self._val = None

    def select(self, *a, **k):
        return self

    def ilike(self, col, val):
        self._col = col
        self._val = val
        return self

    def execute(self):
        return _Resp(self._fn(self._col, self._val))


class _FakeClient:
    def __init__(self, rows_fn):
        self._fn = rows_fn

    def table(self, name):
        return _Query(self._fn)


def _manager(rows_fn):
    """Construye un SupabaseManager evitando __init__ (sin conexión real)."""
    mgr = SupabaseManager.__new__(SupabaseManager)
    mgr.logger = get_logger("test-identity")
    mgr.client = _FakeClient(rows_fn)
    return mgr


def test_rule1_email_match_merges():
    row = {"id": "c1", "email": "ada@x.com", "name": "Ada", "notion_page_id": "pg1"}
    fn = lambda col, val: [row] if col == "email" else []
    cand, page = _manager(fn).resolve_candidate_identity("ada@x.com", "Ada")
    assert cand["id"] == "c1"
    assert page == "pg1"


def test_rule3_same_name_different_email_is_new():
    row = {"id": "c2", "email": "other@x.com", "name": "Ada", "notion_page_id": "pg2"}
    fn = lambda col, val: [row] if col == "name" else []
    cand, page = _manager(fn).resolve_candidate_identity("ada@x.com", "Ada")
    assert cand is None
    assert page is None


def test_rule2_4_name_match_no_conflict_merges():
    row = {"id": "c3", "email": None, "name": "Ada", "notion_page_id": "pg3"}
    fn = lambda col, val: [row] if col == "name" else []
    cand, page = _manager(fn).resolve_candidate_identity(None, "Ada")
    assert cand["id"] == "c3"
    assert page == "pg3"


def test_no_match_creates_new():
    fn = lambda col, val: []
    cand, page = _manager(fn).resolve_candidate_identity("nobody@x.com", "Ghost Name")
    assert cand is None
    assert page is None
