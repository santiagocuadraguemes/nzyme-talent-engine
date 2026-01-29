import os
import requests
import re

def descargar_archivo(url, nombre_archivo, carpeta_destino="/tmp/temp_downloads"):
    """
    Descarga un archivo desde una URL a una carpeta temporal.
    Limpia el nombre del archivo para asegurar compatibilidad con el sistema de archivos.
    """
    if not os.path.exists(carpeta_destino):
        os.makedirs(carpeta_destino)

    # Limpieza de nombre (alfanumérico, puntos, guiones y barras bajas)
    clean_name = "".join([c for c in nombre_archivo if c.isalnum() or c in "._-"]).strip()
    path = os.path.join(carpeta_destino, clean_name)

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return path
    except Exception as e:
        # En un script real podrías pasar un logger aquí, pero un print
        # o dejar que la excepción suba es suficiente para utilidades simples.
        print(f"[ERROR UTIL] Fallo descargando {nombre_archivo}: {e}")
        return None