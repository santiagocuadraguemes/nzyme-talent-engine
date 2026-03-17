# main_lambda.py
import os
import sys
import json

# Make sure Python finds the modules in the root folder
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Client imports
from core.notion_client import NotionClient
from core.supabase_client import SupabaseManager
from core.storage_client import StorageClient
from core.ai_parser import CVAnalyzer
from core.guidelines_parser import GuidelinesParser
from core.logger import get_logger, set_request_id
from core.webhook_router import WebhookRouter
from core.constants import (
    HANDLER_PROCESS_LAUNCHER, HANDLER_PROCESS_DASHBOARD,
    HANDLER_MAIN_CANDIDATE, HANDLER_CENTRAL_REFERENCE,
    HANDLER_WORKFLOW_ITEM, HANDLER_FEEDBACK_FORM,
    HANDLER_FORM_SUBMISSION, HANDLER_BULK_SUBMISSION,
    HANDLER_OUTCOME_FORM,
)

# Worker imports
from scripts.factory_worker import FactoryWorkerV2
from scripts.harvester import HarvesterRelational
from scripts.observer import Observer

# Initialize the Handler Logger (Global for reuse in warm starts)
logger = get_logger("LambdaRouter")

# Feature flag → env var mapping
WEBHOOK_FLAGS = {
    HANDLER_PROCESS_LAUNCHER: "WEBHOOK_PROCESS_DASHBOARD_ENABLED",
    HANDLER_PROCESS_DASHBOARD: "WEBHOOK_PROCESS_DASHBOARD_ENABLED",
    HANDLER_MAIN_CANDIDATE: "WEBHOOK_MAIN_CANDIDATE_ENABLED",
    HANDLER_CENTRAL_REFERENCE: "WEBHOOK_CENTRAL_REFERENCE_ENABLED",
    HANDLER_WORKFLOW_ITEM: "WEBHOOK_WORKFLOW_ENABLED",
    HANDLER_FEEDBACK_FORM: "WEBHOOK_FEEDBACK_ENABLED",
    HANDLER_FORM_SUBMISSION: "WEBHOOK_FORM_SUBMISSION_ENABLED",
    HANDLER_BULK_SUBMISSION: "WEBHOOK_BULK_SUBMISSION_ENABLED",
    HANDLER_OUTCOME_FORM: "WEBHOOK_OUTCOME_ENABLED",
}


def _is_webhook_enabled(handler_name):
    """Checks if the feature flag for this webhook handler is enabled."""
    flag = WEBHOOK_FLAGS.get(handler_name)
    if not flag:
        logger.debug(f"_is_webhook_enabled: no flag registered for handler={handler_name}")
        return False
    enabled = os.getenv(flag, "false").lower() == "true"
    logger.debug(f"_is_webhook_enabled: flag={flag} value={enabled}")
    return enabled


def _init_exa():
    """Lazy ExaClient initialization."""
    try:
        from core.exa_client import ExaClient
        return ExaClient()
    except (ValueError, ImportError) as e:
        logger.warning(f"ExaClient not available: {e}. LinkedIn enrichment disabled.")
        return None


def _finalize(response):
    """Log invocation end marker and return the response."""
    logger.info(f"<<< INVOCATION END | Status: {response.get('statusCode', '?')}")
    logger.info(f"{'=' * 60}")
    return response


def _handle_workspace_webhook(handler_name, page_id, process_context):
    """Dispatches workspace webhook event to the appropriate worker."""

    logger.debug(f"_handle_workspace_webhook: handler={handler_name} page={page_id[:8]}... context_keys={list(process_context.keys()) if process_context else None}")

    # Feature flag check
    if not _is_webhook_enabled(handler_name):
        logger.info(f"[WEBHOOK] {handler_name} disabled by feature flag")
        return {"statusCode": 200, "body": f"Handler {handler_name} disabled"}

    logger.info(f"[WEBHOOK] Dispatching: {handler_name} (page={page_id[:8]}...)")

    try:
        # --- FACTORY: Process Launcher (auxiliary DB signals new process to create) ---
        if handler_name == HANDLER_PROCESS_LAUNCHER:
            n_client = NotionClient()
            s_client = SupabaseManager()
            g_parser = GuidelinesParser(n_client)

            worker = FactoryWorkerV2(n_client, s_client, g_parser)
            worker.run_once()
            return {"statusCode": 200, "body": "Process launcher webhook handled"}

        # --- FACTORY: Process Dashboard (direct page change + status sync) ---
        if handler_name == HANDLER_PROCESS_DASHBOARD:
            n_client = NotionClient()
            s_client = SupabaseManager()
            g_parser = GuidelinesParser(n_client)

            # Factory: create new process if Ready + not Processed
            worker = FactoryWorkerV2(n_client, s_client, g_parser)
            worker.run_from_webhook(page_id)

            # Observer: sync process status (Open/Closed)
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            obs = Observer(n_client, s_client, st_client, ai_agent)
            obs.handle_webhook_event(handler_name, page_id)

            return {"statusCode": 200, "body": "Process dashboard webhook handled"}

        # --- OBSERVER: Static DBs (Main DB, Central References) ---
        if handler_name in (HANDLER_MAIN_CANDIDATE, HANDLER_CENTRAL_REFERENCE):
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            obs = Observer(n_client, s_client, st_client, ai_agent, exa_client=exa)
            obs.handle_webhook_event(handler_name, page_id)
            return {"statusCode": 200, "body": f"Observer webhook handled ({handler_name})"}

        # --- OBSERVER: Dynamic DBs (Workflow stage changes, Feedback) ---
        if handler_name in (HANDLER_WORKFLOW_ITEM, HANDLER_FEEDBACK_FORM):
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            obs = Observer(n_client, s_client, st_client, ai_agent, exa_client=exa)
            obs.handle_webhook_event(handler_name, page_id, process_context)
            return {"statusCode": 200, "body": f"Observer webhook handled ({handler_name})"}

        # --- OBSERVER: Outcome Form (per-application DB, context is application row) ---
        if handler_name == HANDLER_OUTCOME_FORM:
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()

            outcome_context = {"candidate_id": process_context.get("notion_page_id")}
            obs = Observer(n_client, s_client, st_client, ai_agent)
            obs.handle_webhook_event(handler_name, page_id, outcome_context)
            return {"statusCode": 200, "body": "Observer webhook handled (outcome)"}

        # --- HARVESTER: Form submissions ---
        if handler_name == HANDLER_FORM_SUBMISSION:
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            bot = HarvesterRelational(n_client, s_client, st_client, ai_agent, exa_client=exa)
            bot.process_single_from_webhook(page_id, process_context)
            return {"statusCode": 200, "body": "Harvester webhook handled (form)"}

        # --- HARVESTER: Bulk submissions (split files into individual form entries) ---
        # Note: After splitting, Notion automation creates Workflow pages asynchronously.
        # The next EventBridge Harvester run (or a subsequent Form webhook) will process them.
        if handler_name == HANDLER_BULK_SUBMISSION:
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            bot = HarvesterRelational(n_client, s_client, st_client, ai_agent, exa_client=exa)
            bot.process_bulk_imports([process_context])
            return {"statusCode": 200, "body": "Harvester webhook handled (bulk)"}

        logger.warning(f"[WEBHOOK] No dispatch for handler: {handler_name}")
        return {"statusCode": 200, "body": "No handler matched"}

    except Exception as e:
        logger.critical(f"Error in webhook handler '{handler_name}': {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Error in {handler_name}"}


def lambda_handler(event, context):
    """
    Main Lambda router.
    CASE A: EventBridge schedule -> {"task": "harvester|observer|factory"}
    CASE B: Legacy HTTP webhook -> Factory polling
    CASE C: Notion workspace webhook -> Route by database_id
    """
    # Stamp every log line with a unique ID for this invocation.
    # Prevents interleaved logs when warm Lambda reuses the same CloudWatch log stream.
    req_id = set_request_id(getattr(context, "aws_request_id", None) if context else None)
    logger.info(f"{'=' * 60}")
    logger.info(f">>> INVOCATION START | Request: {req_id}")
    logger.info(f"Event received (Keys): {list(event.keys())}")

    trigger_source = "unknown"
    task_name = ""

    # --- 1. SOURCE DETECTION LOGIC ---

    # CASE A: EventBridge (Timer) -> {"task": "harvester"}
    if isinstance(event, dict) and "task" in event:
        trigger_source = "schedule"
        task_name = event["task"].lower()
        logger.info(f"[TRIGGER] EventBridge Schedule. Task: {task_name}")
        logger.debug(f"lambda_handler: trigger=schedule task={task_name}")

    # CASE B/C: HTTP (Webhook / Function URL)
    elif isinstance(event, dict) and ("headers" in event or "requestContext" in event or "rawPath" in event):
        # Parse body for workspace webhook detection
        router = WebhookRouter()  # Static resolution only, no Supabase yet
        parsed = router.parse_event(event)

        # Notion URL verification challenge
        if parsed.get("is_challenge"):
            logger.info("[WEBHOOK] Challenge handshake received")
            return _finalize({
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"challenge": parsed["challenge"]})
            })

        database_id = parsed.get("database_id")
        page_id = parsed.get("page_id")
        source = parsed.get("source", {})
        source_type = source.get("source_type", "unknown")

        if database_id and page_id:
            # CASE C: Webhook with routable IDs — route by database_id
            trigger_source = f"{source_type}_webhook"
            db_short = database_id[:8] if database_id else "N/A"
            page_short = page_id[:8] if page_id else "N/A"
            logger.info(f"[TRIGGER] {source_type.capitalize()} webhook. DB: {db_short}... Page: {page_short}...")

            if source_type == "automation":
                auto_id = source.get("automation_id", "N/A") or "N/A"
                evt_id = source.get("event_id", "N/A") or "N/A"
                logger.info(f"[TRIGGER] Automation details — automation_id: {auto_id}, event_id: {evt_id}")

            # Try static resolution first (no DB client needed)
            handler_name, process_ctx = router.resolve_handler(database_id)
            logger.debug(f"lambda_handler: static resolution — handler={handler_name}")

            # Dynamic resolution if static didn't match
            if not handler_name:
                logger.debug("lambda_handler: falling back to dynamic (Supabase) resolution")
                s_client = SupabaseManager()
                router.supa = s_client
                handler_name, process_ctx = router.resolve_handler(database_id)
                logger.debug(f"lambda_handler: dynamic resolution — handler={handler_name}")

            if handler_name:
                return _finalize(_handle_workspace_webhook(handler_name, page_id, process_ctx))
            else:
                logger.info(f"[WEBHOOK] Unrecognized database: {database_id}")
                return _finalize({"statusCode": 200, "body": "Unrecognized database"})
        else:
            # CASE B: Legacy HTTP webhook -> Factory
            trigger_source = "webhook"
            task_name = "factory"
            logger.info("[TRIGGER] Legacy HTTP webhook. Task: factory")
            if "body" in event:
                logger.info(f"[WEBHOOK] Body: {event['body']}")

    # --- 2. ROUTING AND EXECUTION (Lazy Init) ---

    try:
        # >>> FACTORY WORKER EXECUTION <<<
        if task_name == "factory":
            logger.info("Initializing environment for Factory...")
            n_client = NotionClient()
            s_client = SupabaseManager()
            g_parser = GuidelinesParser(n_client)

            worker = FactoryWorkerV2(n_client, s_client, g_parser)
            worker.run_once()
            return _finalize({"statusCode": 200, "body": "Factory Worker executed successfully"})

        # >>> HARVESTER EXECUTION <<<
        elif task_name == "harvester":
            logger.info("Initializing environment for Harvester...")
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            bot = HarvesterRelational(n_client, s_client, st_client, ai_agent, exa_client=exa)
            bot.run_once()
            return _finalize({"statusCode": 200, "body": "Harvester executed successfully"})

        # >>> OBSERVER EXECUTION <<<
        elif task_name == "observer":
            logger.info("Initializing environment for Observer...")
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = CVAnalyzer()
            exa = _init_exa()

            obs = Observer(n_client, s_client, st_client, ai_agent, exa_client=exa)
            obs.run_once()
            return _finalize({"statusCode": 200, "body": "Observer executed successfully"})

        # >>> UNKNOWN CASE <<<
        else:
            msg = f"No valid task recognized. Trigger: {trigger_source}"
            logger.warning(msg)
            return _finalize({"statusCode": 400, "body": msg})

    except Exception as e:
        # Capture fatal errors inside any worker to avoid silent crashes
        logger.critical(f"Fatal error executing '{task_name}': {e}", exc_info=True)
        return _finalize({"statusCode": 500, "body": f"Internal Server Error during {task_name}"})


# --- BLOCK FOR LOCAL TESTING (Simulation) ---
if __name__ == "__main__":
    print("\n--- LOCAL LAMBDA SIMULATION ---\n")

    # Uncomment the line you want to test:

    # 1. Simulate Webhook Event (Factory - Legacy)
    # fake_webhook_event = {"headers": {"Content-Type": "application/json"}, "body": "{}"}
    # lambda_handler(fake_webhook_event, None)

    # 2. Simulate Timer Event (Harvester)
    fake_schedule_event = {"task": "harvester"}
    lambda_handler(fake_schedule_event, None)

    # 3. Simulate Timer Event (Observer)
    # fake_observer_event = {"task": "observer"}
    # lambda_handler(fake_observer_event, None)

    # 4. Simulate Workspace Webhook (e.g. Main DB change)
    # fake_workspace_webhook = {
    #     "headers": {"Content-Type": "application/json"},
    #     "body": json.dumps({
    #         "type": "page.updated",
    #         "data": {
    #             "id": "page-id-here",
    #             "parent": {"database_id": "your-main-db-id-here"},
    #             "properties": {}
    #         }
    #     })
    # }
    # lambda_handler(fake_workspace_webhook, None)

    # 5. Simulate Automation Webhook (Notion automation → Lambda)
    # fake_automation_webhook = {
    #     "headers": {"Content-Type": "application/json"},
    #     "body": json.dumps({
    #         "source": {
    #             "type": "automation",
    #             "automation_id": "test-automation-id"
    #         },
    #         "event_id": "test-event-id",
    #         "data": {
    #             "object": "page",
    #             "id": "page-id-here",
    #             "parent": {
    #                 "type": "data_source_id",
    #                 "database_id": "your-db-id-here"
    #             },
    #             "properties": {}
    #         }
    #     })
    # }
    # lambda_handler(fake_automation_webhook, None)
