import os
import httpx
import re
from core.logger import get_logger

logger = get_logger("Utils")

def download_file(url, file_name, dest_folder="/tmp/temp_downloads"):
    """Downloads a file from a URL to a temporary folder.
    Cleans the file name to ensure filesystem compatibility.
    """
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    # Clean name (alphanumeric, dots, hyphens, underscores)
    clean_name = "".join([c for c in file_name if c.isalnum() or c in "._-"]).strip()
    path = os.path.join(dest_folder, clean_name)

    logger.debug(f"download_file: fetching '{file_name}' to '{dest_folder}'")
    try:
        with httpx.stream("GET", url) as r:
            r.raise_for_status()
            logger.debug(f"download_file: HTTP {r.status_code}, streaming to '{path}'")
            total_bytes = 0
            with open(path, 'wb') as f:
                for chunk in r.iter_bytes(chunk_size=8192):
                    f.write(chunk)
                    total_bytes += len(chunk)
        logger.debug(f"download_file: complete — {total_bytes} bytes written to '{path}'")
        return path
    except Exception as e:
        print(f"[ERROR UTIL] Failed to download {file_name}: {e}")
        return None
