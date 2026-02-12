import os
from dotenv import load_dotenv
from exa_py import Exa
from core.logger import get_logger

load_dotenv()


class ExaClient:
    def __init__(self):
        self.logger = get_logger("ExaClient")
        key = os.getenv("EXA_API_KEY")
        if not key:
            raise ValueError("Missing EXA_API_KEY in .env")
        self.client = Exa(api_key=key)

    def get_linkedin_profile(self, linkedin_url: str) -> str | None:
        """Fetches LinkedIn profile content as markdown text via Exa API."""
        try:
            self.logger.info(f"Fetching LinkedIn profile: {linkedin_url}")
            result = self.client.get_contents(urls=[linkedin_url], text=True)
            text = result.results[0].text
            if not text:
                self.logger.warning("Exa returned empty text for profile")
                return None
            self.logger.info(f"LinkedIn profile fetched ({len(text)} chars)")
            return text
        except Exception as e:
            self.logger.error(f"Exa API error: {e}")
            return None
