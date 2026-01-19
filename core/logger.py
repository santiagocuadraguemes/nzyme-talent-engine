import logging
import os
from logging.handlers import RotatingFileHandler

# Crear carpeta de logs si no existe
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def get_logger(name):
    """
    Configura y devuelve un logger con el nombre especificado.
    Guarda en archivo (rotativo) y muestra en consola.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO) # Nivel mínimo global

    # Evitar duplicar handlers si se llama varias veces
    if logger.hasHandlers():
        return logger

    # Formato: [FECHA HORA] [NOMBRE] [NIVEL] Mensaje
    formatter = logging.Formatter(
        '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 1. Handler de Archivo (Rota cada 5MB, guarda los últimos 3 archivos)
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, "app.log"), 
        maxBytes=5*1024*1024, 
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # 2. Handler de Consola (Lo que ves en pantalla)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger