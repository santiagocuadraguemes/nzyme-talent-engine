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
from core.ai_parser import AnalizadorCV
from core.guidelines_parser import GuidelinesParser
from core.logger import get_logger

# Worker imports
from scripts.factory_worker import FactoryWorkerV2
from scripts.harvester import HarvesterRelational
from scripts.observer import Observer

# Initialize the Handler Logger (Global for reuse in warm starts)
logger = get_logger("LambdaRouter")

def lambda_handler(event, context):
    """
    Main brain of the Lambda.
    Decides which script to execute based on the type of event received.
    Uses lazy initialization to save resources.
    """
    # Event log (useful for debugging in CloudWatch)
    logger.info(f"Evento Recibido (Keys): {list(event.keys())}")

    trigger_source = "unknown"
    task_name = ""

    # --- 1. SOURCE DETECTION LOGIC ---

    # CASE A: EventBridge (Timer) -> {"task": "harvester"}
    if isinstance(event, dict) and "task" in event:
        trigger_source = "schedule"
        task_name = event["task"].lower()
        logger.info(f"[TRIGGER] Detectado EventBridge Schedule. Tarea: {task_name}")

    # CASE B: Webhook / Function URL (Notion)
    elif isinstance(event, dict) and ("headers" in event or "requestContext" in event or "rawPath" in event):
        trigger_source = "webhook"
        task_name = "factory"
        logger.info("[TRIGGER] Detectada Petición HTTP (Webhook).")
        
        # Log body in case Notion sends validations
        if "body" in event:
            logger.info(f"WEBHOOK BODY: {event['body']}")

    # --- 2. ROUTING AND EXECUTION (Lazy Init) ---
    
    try:
        # >>> FACTORY WORKER EXECUTION <<<
        if task_name == "factory":
            logger.info("Iniciando entorno para Factory...")
            # Initialize ONLY what is needed for this task
            n_client = NotionClient()
            s_client = SupabaseManager()
            g_parser = GuidelinesParser(n_client)
            
            worker = FactoryWorkerV2(n_client, s_client, g_parser)
            worker.run_once()
            return {"statusCode": 200, "body": "Factory Worker executed successfully"}

        # >>> HARVESTER EXECUTION <<<
        elif task_name == "harvester":
            logger.info("Iniciando entorno para Harvester...")
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = AnalizadorCV()
            
            bot = HarvesterRelational(n_client, s_client, st_client, ai_agent)
            bot.run_once()
            return {"statusCode": 200, "body": "Harvester executed successfully"}

        # >>> OBSERVER EXECUTION <<<
        elif task_name == "observer":
            logger.info("Iniciando entorno para Observer...")
            n_client = NotionClient()
            s_client = SupabaseManager()
            st_client = StorageClient()
            ai_agent = AnalizadorCV()
            
            obs = Observer(n_client, s_client, st_client, ai_agent)
            obs.run_once()
            return {"statusCode": 200, "body": "Observer executed successfully"}

        # >>> UNKNOWN CASE <<<
        else:
            msg = f"No se ha reconocido una tarea válida. Trigger: {trigger_source}"
            logger.warning(msg)
            return {"statusCode": 400, "body": msg}

    except Exception as e:
        # Capture fatal errors inside any worker to avoid silent crashes
        logger.critical(f"Error fatal ejecutando '{task_name}': {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Internal Server Error during {task_name}"}


# --- BLOCK FOR LOCAL TESTING (Simulation) ---
if __name__ == "__main__":
    print("\n--- SIMULACIÓN LOCAL DE LAMBDA ---\n")
    
    # Uncomment the line you want to test:
    
    # 1. Simulate Webhook Event (Factory)
    # fake_webhook_event = {"headers": {"Content-Type": "application/json"}, "body": "{}"}
    # lambda_handler(fake_webhook_event, None)

    # 2. Simulate Timer Event (Harvester)
    fake_schedule_event = {"task": "harvester"}
    lambda_handler(fake_schedule_event, None)
    
    # 3. Simulate Timer Event (Observer)
    # fake_observer_event = {"task": "observer"}
    # lambda_handler(fake_observer_event, None)
