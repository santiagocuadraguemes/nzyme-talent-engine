# core/webhook_router.py

import os
import json
from core.logger import get_logger
from core.constants import (
    HANDLER_PROCESS_LAUNCHER, HANDLER_PROCESS_DASHBOARD,
    HANDLER_MAIN_CANDIDATE, HANDLER_CENTRAL_REFERENCE,
    HANDLER_WORKFLOW_ITEM, HANDLER_FEEDBACK_FORM,
    HANDLER_FORM_SUBMISSION, HANDLER_BULK_SUBMISSION,
    HANDLER_OUTCOME_FORM,
)


class WebhookRouter:
    """
    Routes Notion workspace webhook events to the correct handler.
    Static registry: env var DB IDs → handler names (no DB call needed).
    Dynamic registry: queries Supabase for process-specific DB IDs.
    """

    def __init__(self, supa_client=None):
        self.logger = get_logger("WebhookRouter")
        self.supa = supa_client
        self._static_registry = self._build_static_registry()

    @staticmethod
    def _normalize_id(raw_id):
        """Strip dashes from UUID for format-agnostic comparison."""
        return raw_id.replace("-", "") if raw_id else None

    @staticmethod
    def _build_static_registry():
        registry = {}
        mappings = {
            "NOTION_MAIN_DB_ID": HANDLER_MAIN_CANDIDATE,
            "NOTION_PROCESS_DASHBOARD_DB_ID": HANDLER_PROCESS_DASHBOARD,
            "NOTION_PROCESS_LAUNCHER_DB_ID": HANDLER_PROCESS_LAUNCHER,
            "NOTION_REFERENCES_DB_ID": HANDLER_CENTRAL_REFERENCE,
        }
        for env_var, handler in mappings.items():
            db_id = os.getenv(env_var)
            if db_id:
                registry[db_id.replace("-", "")] = handler
        return registry

    def parse_event(self, raw_event):
        """
        Extracts event_type, page_id, database_id from Lambda event body.
        Handles Notion's challenge/response verification handshake.

        Supports three Notion webhook payload shapes:
          Shape A (native workspace):
            {"type": "page.updated", "entity": {"id": "...", "type": "page"},
             "data": {"parent": {"id": "...", "type": "database"}}}
          Shape B (fallback / legacy):
            {"data": {"id": "...", "parent": {"database_id": "..."}}}
          Shape C (automation):
            {"source": {"type": "automation", "automation_id": "..."}, "data": {"id": "...",
             "parent": {"type": "data_source_id", "database_id": "..."}}}

        Returns dict with keys: is_challenge, event_type, page_id, database_id, source.
        source = {"source_type": str, "automation_id": str|None, "event_id": str|None}
        """
        _empty = {
            "is_challenge": False, "event_type": "unknown",
            "page_id": None, "database_id": None,
            "source": {"source_type": "unknown", "automation_id": None, "event_id": None},
        }

        body = raw_event.get("body", "{}")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except (json.JSONDecodeError, TypeError):
                return _empty

        # Challenge handshake
        if "challenge" in body:
            self.logger.debug("parse_event: challenge handshake detected")
            return {"is_challenge": True, "challenge": body["challenge"]}

        # --- Detect payload shape ---
        source_block = body.get("source", {})
        entity = body.get("entity", {})
        data = body.get("data", {})
        parent = data.get("parent", {})

        if source_block.get("type") == "automation":
            # Shape C: Automation webhook
            page_id = data.get("id")
            database_id = parent.get("database_id")
            event_type = "automation"
            source_meta = {
                "source_type": "automation",
                "automation_id": source_block.get("automation_id"),
                "event_id": source_block.get("event_id"),
            }
            self.logger.debug(f"parse_event: shape C (automation) — page={str(page_id)[:8]}... db={str(database_id)[:8]}...")

        elif entity.get("type") == "page":
            # Shape A: Native workspace webhook
            page_id = entity.get("id")
            if parent.get("type") == "database":
                database_id = parent.get("id")
            else:
                database_id = parent.get("database_id")
            event_type = body.get("type", "unknown")
            source_meta = {
                "source_type": "workspace",
                "automation_id": None,
                "event_id": None,
            }
            self.logger.debug(f"parse_event: shape A (workspace) — event_type={event_type} page={str(page_id)[:8]}... db={str(database_id)[:8]}...")

        else:
            # Shape B: Fallback / legacy
            page_id = data.get("id")
            database_id = parent.get("database_id")
            if not database_id and parent.get("type") == "database":
                database_id = parent.get("id")
            event_type = body.get("type", "unknown")
            source_meta = {
                "source_type": "fallback",
                "automation_id": None,
                "event_id": None,
            }
            self.logger.debug(f"parse_event: shape B (fallback) — page={str(page_id)[:8]}... db={str(database_id)[:8]}...")

        return {
            "is_challenge": False,
            "event_type": event_type,
            "page_id": page_id,
            "database_id": database_id,
            "source": source_meta,
        }

    def resolve_handler(self, database_id):
        """
        Returns (handler_name, process_context) for a given database_id.
        Static registry first, then dynamic Supabase lookup.
        """
        if not database_id:
            return None, None

        normalized = self._normalize_id(database_id)

        # 1. Static registry (keys already normalized)
        if normalized in self._static_registry:
            handler = self._static_registry[normalized]
            self.logger.debug(f"resolve_handler: static hit — db={database_id[:8]}... handler={handler}")
            return handler, None

        self.logger.debug(f"resolve_handler: static miss — db={database_id[:8]}..., trying dynamic lookup")

        # 2. Dynamic registry via Supabase (Process DBs)
        if self.supa:
            process = self.supa.resolve_process_by_notion_db_id(database_id)
            if not process and normalized != database_id:
                process = self.supa.resolve_process_by_notion_db_id(normalized)
            if process:
                handler = self._classify_process_db(normalized, process)
                self.logger.debug(f"resolve_handler: dynamic hit — process={str(process.get('id', ''))[:8]}... handler={handler}")
                return handler, process

            self.logger.debug(f"resolve_handler: dynamic miss — trying application lookup")

            # 3. Application registry (Outcome Forms)
            application = self.supa.resolve_application_by_outcome_db_id(database_id)
            if not application and normalized != database_id:
                application = self.supa.resolve_application_by_outcome_db_id(normalized)
            if application:
                self.logger.debug(f"resolve_handler: application hit — app={str(application.get('id', ''))[:8]}... handler={HANDLER_OUTCOME_FORM}")
                return HANDLER_OUTCOME_FORM, application

            self.logger.debug(f"resolve_handler: no match found for db={database_id[:8]}...")

        return None, None

    def _classify_process_db(self, database_id, process):
        """Determines handler type based on which process column matches."""
        normalized = self._normalize_id(database_id)
        for col, handler in [
            ("notion_workflow_id", HANDLER_WORKFLOW_ITEM),
            ("notion_feedback_id", HANDLER_FEEDBACK_FORM),
            ("notion_form_id", HANDLER_FORM_SUBMISSION),
            ("notion_bulk_id", HANDLER_BULK_SUBMISSION),
        ]:
            stored = process.get(col)
            if stored and self._normalize_id(stored) == normalized:
                self.logger.debug(f"_classify_process_db: matched column={col} handler={handler}")
                return handler
        self.logger.debug(f"_classify_process_db: no column matched for db={database_id[:8]}...")
        return None

