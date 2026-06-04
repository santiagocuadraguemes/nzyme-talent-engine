"""
VT-1 (parte): enrutado de webhooks y puerta del token.
Valida la detección de las tres formas de payload, la normalización de IDs,
el control de acceso por token (fail closed) y la resolución de handler
(registro estático y dinámico).
"""
import json

from core.webhook_router import WebhookRouter, verify_path_token, _extract_path_token
from core.constants import (
    HANDLER_MAIN_CANDIDATE, HANDLER_WORKFLOW_ITEM, HANDLER_OUTCOME_FORM,
)


def _evt(body_dict):
    return {"body": json.dumps(body_dict)}


# --- parse_event: las tres formas ---

def test_parse_event_challenge():
    r = WebhookRouter().parse_event(_evt({"challenge": "tok-123"}))
    assert r["is_challenge"] is True
    assert r["challenge"] == "tok-123"


def test_parse_event_bad_json_returns_empty():
    r = WebhookRouter().parse_event({"body": "{not valid json"})
    assert r["is_challenge"] is False
    assert r["page_id"] is None
    assert r["database_id"] is None
    assert r["event_type"] == "unknown"


def test_parse_event_shape_a_workspace():
    body = {
        "type": "page.updated",
        "entity": {"id": "PAGE", "type": "page"},
        "data": {"parent": {"id": "DB", "type": "database"}},
    }
    r = WebhookRouter().parse_event(_evt(body))
    assert r["page_id"] == "PAGE"
    assert r["database_id"] == "DB"
    assert r["event_type"] == "page.updated"
    assert r["source"]["source_type"] == "workspace"


def test_parse_event_shape_c_automation():
    body = {
        "source": {"type": "automation", "automation_id": "AUTO", "event_id": "EV"},
        "data": {"object": "page", "id": "PAGE", "parent": {"type": "data_source_id", "database_id": "DB"}},
    }
    r = WebhookRouter().parse_event(_evt(body))
    assert r["page_id"] == "PAGE"
    assert r["database_id"] == "DB"
    assert r["event_type"] == "automation"
    assert r["source"]["source_type"] == "automation"
    assert r["source"]["automation_id"] == "AUTO"
    assert r["source"]["event_id"] == "EV"


def test_parse_event_shape_b_fallback():
    body = {"data": {"id": "PAGE", "parent": {"database_id": "DB"}}}
    r = WebhookRouter().parse_event(_evt(body))
    assert r["page_id"] == "PAGE"
    assert r["database_id"] == "DB"
    assert r["source"]["source_type"] == "fallback"


# --- normalización de IDs ---

def test_normalize_id_strips_dashes():
    assert WebhookRouter._normalize_id("aaaa-bbbb-cccc") == "aaaabbbbcccc"
    assert WebhookRouter._normalize_id(None) is None


# --- puerta del token ---

def test_extract_path_token_variants():
    assert _extract_path_token({"rawPath": "/secret/"}) == "secret"
    assert _extract_path_token({"requestContext": {"http": {"path": "/tok"}}}) == "tok"
    assert _extract_path_token({}) is None
    assert _extract_path_token({"rawPath": "/"}) is None


def test_verify_path_token_fail_closed_when_unset(monkeypatch):
    monkeypatch.delenv("WEBHOOK_PATH_TOKEN", raising=False)
    assert verify_path_token({"rawPath": "/anything"}) is False


def test_verify_path_token_correct(monkeypatch):
    monkeypatch.setenv("WEBHOOK_PATH_TOKEN", "s3cr3t")
    assert verify_path_token({"rawPath": "/s3cr3t"}) is True


def test_verify_path_token_mismatch(monkeypatch):
    monkeypatch.setenv("WEBHOOK_PATH_TOKEN", "s3cr3t")
    assert verify_path_token({"rawPath": "/wrong"}) is False


def test_verify_path_token_no_path(monkeypatch):
    monkeypatch.setenv("WEBHOOK_PATH_TOKEN", "s3cr3t")
    assert verify_path_token({}) is False


# --- resolución de handler ---

def test_resolve_handler_static_hit(monkeypatch):
    monkeypatch.setenv("NOTION_MAIN_DB_ID", "aaaabbbbccccdddd")
    router = WebhookRouter()
    handler, ctx = router.resolve_handler("aaaabbbbccccdddd")
    assert handler == HANDLER_MAIN_CANDIDATE
    assert ctx is None
    # Tolera el formato con guiones (normalización)
    handler2, _ = router.resolve_handler("aaaabbbb-cccc-dddd")
    assert handler2 == HANDLER_MAIN_CANDIDATE


def test_resolve_handler_none_db():
    assert WebhookRouter().resolve_handler(None) == (None, None)


class _FakeSupa:
    def __init__(self, process=None, application=None):
        self._process = process
        self._application = application

    def resolve_process_by_notion_db_id(self, db_id):
        return self._process

    def resolve_application_by_outcome_db_id(self, db_id):
        return self._application


def test_resolve_handler_dynamic_process_by_column():
    supa = _FakeSupa(process={"id": "p1", "notion_workflow_id": "wf123abc"})
    router = WebhookRouter(supa_client=supa)
    handler, ctx = router.resolve_handler("wf123abc")
    assert handler == HANDLER_WORKFLOW_ITEM
    assert ctx["id"] == "p1"


def test_resolve_handler_application_outcome():
    supa = _FakeSupa(process=None, application={"id": "a1"})
    router = WebhookRouter(supa_client=supa)
    handler, ctx = router.resolve_handler("outcome-db")
    assert handler == HANDLER_OUTCOME_FORM
    assert ctx["id"] == "a1"


def test_resolve_handler_miss():
    supa = _FakeSupa(process=None, application=None)
    router = WebhookRouter(supa_client=supa)
    assert router.resolve_handler("nope") == (None, None)


# --- extract_event_kind: cabecera personalizada X-Nzyme-Event ---

from core.webhook_router import extract_event_kind
from core.constants import WEBHOOK_EVENT_HEADER, WEBHOOK_EVENT_EDIT, WEBHOOK_EVENT_CREATED


def test_event_kind_lowercase_header():
    # Function URL entrega las cabeceras en minusculas
    evt = {"headers": {"x-nzyme-event": "edit"}}
    assert extract_event_kind(evt) == WEBHOOK_EVENT_EDIT


def test_event_kind_mixed_case_key_and_value():
    evt = {"headers": {"X-Nzyme-Event": " Created "}}
    assert extract_event_kind(evt) == WEBHOOK_EVENT_CREATED


def test_event_kind_absent_header_returns_none():
    assert extract_event_kind({"headers": {"content-type": "application/json"}}) is None
    assert extract_event_kind({"headers": {}}) is None
    assert extract_event_kind({}) is None


def test_event_kind_malformed_values_return_none():
    assert extract_event_kind({"headers": {"x-nzyme-event": ""}}) is None
    assert extract_event_kind({"headers": {"x-nzyme-event": None}}) is None
    assert extract_event_kind({"headers": None}) is None


def test_event_kind_unknown_value_passes_through():
    # Valores futuros se normalizan pero no se validan (extensible)
    evt = {"headers": {"x-nzyme-event": "SOMETHING-NEW"}}
    assert extract_event_kind(evt) == "something-new"


def test_event_kind_header_constant_is_lowercase():
    # El matching depende de comparar contra la constante ya en minusculas
    assert WEBHOOK_EVENT_HEADER == WEBHOOK_EVENT_HEADER.lower()
