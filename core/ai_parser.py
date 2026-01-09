import os
import pdfplumber
from typing import List, Optional
from pydantic import BaseModel, Field
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# --- MODELOS DE DATOS ---

class SectorExperience(BaseModel):
    has_experience: bool = Field(description="True if the candidate worked in this sector.")
    years: float = Field(description="Total years worked in this sector.")
    companies: List[str] = Field(description="List of company names ONLY. Clean names (e.g. 'Deloitte', not 'Deloitte T&T'). Fix capitalization (e.g. 'JPMorgan').")

class ExperienceBreakdown(BaseModel):
    # Advisory / Services
    consulting: SectorExperience = Field(description="Strategy/Management Consulting firms (McKinsey, Bain, etc).")
    audit: SectorExperience = Field(description="Audit firms (Big4).")
    ib: SectorExperience = Field(description="Investment Banking roles (GS, JPM, Morgan Stanley).")
    pe: SectorExperience = Field(description="Private Equity funds (Blackstone, KKR).")
    vc: SectorExperience = Field(description="Venture Capital funds (Sequoia, a16z).")
    
    # Technical / Founder
    engineer_role: SectorExperience = Field(description="Technical engineering roles.")
    lawyer: SectorExperience = Field(description="Legal roles in law firms or in-house counsel.")
    founder: SectorExperience = Field(description="Founded their own company.")

    # Corporate / Industry (Management)
    management: SectorExperience = Field(description="CRITICAL: Roles inside REAL companies (Retail, Tech, Industrial). EXCLUDE Consulting/Banks/Advisors here. Only actual employment.")
    corp_ma: SectorExperience = Field(description="Internal M&A/CorpDev roles inside a real company.")
    
    # Portfolio Roles
    portco_roles: SectorExperience = Field(description="C-Level/Management roles in companies explicitly owned by PE funds.")

class GeneralData(BaseModel):
    international_locations: List[str] = Field(description="List of countries where candidate LIVED & WORKED > 6 months. Exclude deal locations.")
    industries_specialized: List[str] = Field(description="Industries where candidate held MANAGEMENT roles (Real companies). Exclude industries they only advised as a consultant. Generic category only (e.g., 'Tech', 'Energy', 'Healthcare').")

class Education(BaseModel):
    bachelors: List[str] = Field(description="Generic category only (e.g., 'Engineering', 'Law', 'Business').")
    masters: List[str] = Field(description="Generic category only.")
    mba: str = Field(description="Name of the Business School if exists, else 'No'.")
    university: List[str] = Field(description="School names.")

class CVData(BaseModel):
    name: str
    email: Optional[str]
    phone: Optional[str]
    linkedin_url: Optional[str]
    total_years: float = Field(description="Total professional experience sum.")
    
    education: Education
    experience: ExperienceBreakdown
    general: GeneralData
    languages: List[str] = Field(description="Language names only.")

# --- CLASE PRINCIPAL ---

class AnalizadorCV:
    def __init__(self):
        key = os.getenv("OPENAI_API_KEY")
        if not key: raise ValueError("Falta OPENAI_API_KEY en .env")
        self.client = OpenAI(api_key=key)
        self.model_name = "gpt-5-mini" 

    def _leer_pdf(self, ruta_archivo):
        texto = ""
        try:
            with pdfplumber.open(ruta_archivo) as pdf:
                for pagina in pdf.pages:
                    texto += (pagina.extract_text() or "") + "\n"
            return texto
        except Exception as e:
            print(f"   [IA ERROR] Leyendo PDF: {e}")
            return None

    def procesar_cv(self, ruta_archivo):
        print("      -> Analizando con IA...")
        texto_cv = self._leer_pdf(ruta_archivo)
        if not texto_cv: return None

        texto_cv = texto_cv[:20000]

        prompt_system = """
        You are an elite Headhunter data entry specialist. Your job is to extract data from CVs with extreme precision and standardize it.

        ### CAPITALIZATION & CLEANING RULES (Apply to ALL text fields)
        1. **Title Case**: Always convert names to standard business casing (e.g., "KEARNEY" -> "Kearney", "mckinsey" -> "McKinsey", "jp morgan" -> "JP Morgan").
        2. **Clean Names**: Remove legal suffixes (Ltd, Inc, S.A, GmbH). Remove division names unless vital (e.g., "Santander CIB" -> "Santander", "Deloitte T&T" -> "Deloitte").

        ### EXPERIENCE RULES (The Golden Rules)
        1. **Management (at companies)**: Include ONLY real companies (Retail, Pharma, Tech, etc.). NEVER include Consulting firms (McKinsey), Big4, or Banks here.
        2. **PE PortCo Roles**: Only include if the candidate explicitly states the company was a Private Equity Portfolio Company during their tenure.
        3. **Corporate M&A**: Only internal M&A/Integration roles at real companies. Exclude IB/Advisory deals.
        4. **Industries**: Only list industries where the candidate was an EMPLOYEE (Management). Do not list industries they advised as a consultant. Generic category only (e.g., 'Tech', 'Energy', 'Healthcare').
        5. **International**: Only countries where they LIVED and WORKED for >6 months. Ignore project travel.

        ### EDUCATION RULES
        1. **Simplification**: Map specific degrees to: 'Engineering', 'Law', 'Economics', 'Business', 'Science', 'Humanities'.
        2. **MBA**: If present, extract ONLY the School Name (e.g., "INSEAD", "Harvard"). If none, output "No".

        ### OUTPUT
        Extract strictly into the JSON schema provided.
        """

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"CV Content:\n{texto_cv}"},
                ],
                response_format=CVData,
            )
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            print(f"   [IA ERROR] OpenAI: {e}")
            return None