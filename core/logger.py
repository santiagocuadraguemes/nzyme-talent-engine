import logging
import os
import sys

def get_logger(name):
    """
    Configura y devuelve un logger adaptado para AWS Lambda (CloudWatch).
    - No escribe en ficheros (evita errores de Read-only file system).
    - Escribe en stdout (Consola), que CloudWatch captura automáticamente.
    - El nivel de log se define por variable de entorno LOG_LEVEL.
    """
    logger = logging.getLogger(name)
    
    # Leemos el nivel desde el entorno. Por defecto INFO.
    # Opciones: DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    logger.setLevel(log_level)

    # Evitar duplicar handlers si se llama varias veces (Lambda a veces reutiliza contextos)
    if logger.hasHandlers():
        return logger

    # Formato simple. En CloudWatch el timestamp ya lo pone AWS, 
    # pero lo dejamos para claridad si lo corres en local.
    formatter = logging.Formatter(
        '[%(levelname)s] [%(name)s] %(message)s'
    )

    # Solo Handler de Consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    logger.addHandler(console_handler)

    return logger