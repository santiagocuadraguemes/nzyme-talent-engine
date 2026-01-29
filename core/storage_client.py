import os
import requests
import time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

class StorageClient:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("Faltan credenciales de Supabase en .env")

        self.supabase = create_client(url, key)
        self.bucket_name = "resumes" 

    def subir_cv_desde_url(self, notion_url, nombre_archivo):
        """
        Descarga el archivo de Notion (temporal) y lo sube a Supabase (permanente).
        Retorna la URL publica.
        """
        try:
            # 1. Descargar de Notion
            response = requests.get(notion_url)
            if response.status_code != 200:
                print(f"      [ERROR STORAGE] No se pudo descargar de Notion: {response.status_code}")
                return None
            
            file_content = response.content
            
            # 2. Limpiar nombre y hacerlo unico
            # Quitamos caracteres raros y añadimos timestamp para evitar colisiones
            safe_name = "".join([c for c in nombre_archivo if c.isalnum() or c in "._-"]).strip()
            timestamp = int(time.time())
            path = f"{timestamp}_{safe_name}"

            # 3. Subir a Supabase Storage
            # 'upsert': 'true' permite sobrescribir si por casualidad existiera
            res = self.supabase.storage.from_(self.bucket_name).upload(
                path=path,
                file=file_content,
                file_options={"content-type": "application/pdf", "upsert": "true"}
            )
            
            # 4. Obtener URL Publica
            # La libreria devuelve la URL directamente con get_public_url
            public_url = self.supabase.storage.from_(self.bucket_name).get_public_url(path)
            
            return public_url

        except Exception as e:
            print(f"      [EXCEPCION STORAGE] {e}")
            return None