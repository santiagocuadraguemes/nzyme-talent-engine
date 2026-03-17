import os
import mimetypes
import httpx
import time
import unicodedata
from supabase import create_client
from dotenv import load_dotenv
from core.logger import get_logger

load_dotenv()

class StorageClient:
    def __init__(self):
        self.logger = get_logger("StorageClient")
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("Missing Supabase credentials in .env")

        self.supabase = create_client(url, key)
        self.bucket_name = "resumes"

    def upload_cv_from_url(self, notion_url, file_name):
        """Downloads file from Notion (temporary) and uploads to Supabase (permanent).
        Returns the public URL.
        """
        try:
            # 1. Download from Notion
            self.logger.debug(f"upload_cv_from_url: downloading file '{file_name}'")
            response = httpx.get(notion_url)
            self.logger.debug(f"upload_cv_from_url: download status {response.status_code}, size {len(response.content)} bytes")
            if response.status_code != 200:
                print(f"      [ERROR STORAGE] Could not download from Notion: {response.status_code}")
                return None

            file_content = response.content

            # 2. Clean name and make unique
            ascii_name = unicodedata.normalize("NFKD", file_name).encode("ascii", "ignore").decode("ascii")
            safe_name = "".join([c for c in ascii_name if c.isalnum() or c in "._-"]).strip()
            timestamp = int(time.time())
            path = f"{timestamp}_{safe_name}"
            self.logger.debug(f"upload_cv_from_url: sanitized name '{safe_name}', upload path '{path}'")

            # 3. Upload to Supabase Storage
            res = self.supabase.storage.from_(self.bucket_name).upload(
                path=path,
                file=file_content,
                file_options={"content-type": mimetypes.guess_type(safe_name)[0] or "application/octet-stream", "upsert": "true"}
            )

            # 4. Get public URL
            public_url = self.supabase.storage.from_(self.bucket_name).get_public_url(path)
            self.logger.debug(f"upload_cv_from_url: public URL generated for path '{path}'")

            return public_url

        except Exception as e:
            print(f"      [EXCEPTION STORAGE] {e}")
            return None
