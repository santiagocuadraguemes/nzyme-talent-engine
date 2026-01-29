import os
import pdfplumber
from typing import List, Optional
from pydantic import BaseModel, Field
from openai import OpenAI
from dotenv import load_dotenv
import logfire
from enum import Enum

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

class StrategicScore(str, Enum):
    HIGH = "High"
    MEDIUM = "Medium"
    LOW = "Low"
    NO = "No"

class AssessmentItem(BaseModel):
    characteristic: str = Field(description="The exact name of the characteristic as listed in the instructions.")
    score: StrategicScore = Field(description="The assessment score based on the candidate's profile.")
    comment: str = Field(description="A brief, concise justification for the score (max 15 words).")

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
    strategic_assessment: List[AssessmentItem] = Field(description="List of 13 strategic evaluations based on the provided definitions.")

# --- NUEVO MODELO PARA FEEDBACK ---
class FeedbackData(BaseModel):
    candidate_name: str = Field(description="The full name of the candidate being evaluated. Look for 'Candidate:', 'Name:', or the header.")
    feedback_text: str = Field(description="The qualitative feedback content. Summarize the interviewer's opinion, strengths, and weaknesses. Exclude boilerplate text, confidential disclaimers, logos, or dates.")

# --- CLASE PRINCIPAL ---

class AnalizadorCV:
    def __init__(self):
        key = os.getenv("OPENAI_API_KEY")
        if not key: raise ValueError("Falta OPENAI_API_KEY en .env")
        self.client = OpenAI(api_key=key)
        logfire.configure()
        logfire.instrument_openai(self.client)
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
        print("      -> Analizando CV con IA (+ Strategic Assessment)...")
        texto_cv = self._leer_pdf(ruta_archivo)
        if not texto_cv: return None

        texto_cv = texto_cv[:25000] # Aumentado ligeramente para dar cabida a CVs largos

        # Definiciones estratégicas (Hardcoded para consistencia)
        strat_definitions = """
        1. Analogous Industry Dynamics: Experience in an industry with similar mechanics to the PortCo.
        2. Analogous Company Dynamics: Experience in a company with similar characteristics to the PortCo (size, ownership, history, needs).
        3. Prior CEO Role (PE-Backed): Experience as CEO within a Private Equity-backed environment.
        4. Prior CEO Role (Not PE-Backed): Experience as CEO but not within the Private Equity ecosystem.
        5. General Management Position (PE-Backed): Experience in top executive/leadership roles in a sponsor-owned company.
        6. General Management Position (Not PE-Backed): Top executive roles not within the PE ecosystem.
        7. Prior role in a PE-Backed company: Working for a sponsor-owned company but not in C-level role.
        8. Decision-Making Track Record: Experience making high-stake decisions and proof of having learned from consequences.
        9. Full P&L Responsibility: Experience managing a complete Profit & Loss statement.
        10. Corporate Transformation: Experience in complex corporate transformations or leading transformational initiatives.
        11. Build-ups or M&A: Experience conducting build-ups or M&A and/or integrations.
        12. Margin & Working Capital Mgmt.: Experience in optimizing financial margins and liquidity.
        13. Processes/ERP Implementation: Experience in process optimization/implementation and ERP deployment.
        """

        prompt_system = f"""
        You are an elite Headhunter data entry specialist. Your job is to extract data from CVs and perform a Strategic Assessment.

        ### CAPITALIZATION & CLEANING RULES (Apply to ALL text fields)
        1. **Title Case**: Always convert names to standard business casing.
        2. **Clean Names**: Remove legal suffixes (Ltd, Inc, S.A).

        ### EXPERIENCE RULES (The Golden Rules)
        1. **Management**: Include ONLY real companies. NEVER Consulting firms/Big4/Banks.
        2. **PE PortCo**: Only if explicitly stated as PE Portfolio Company.
        3. **International**: LIVED and WORKED > 6 months.

        ### EDUCATION RULES
        1. **Simplification**: Map degrees to 'Engineering', 'Law', 'Economics', 'Business', 'Science', 'Humanities'.
        2. **MBA**: Extract School Name or "No".

        ### STRATEGIC ASSESSMENT INSTRUCTIONS
        Evaluate the candidate against the following 13 characteristics using the definitions below.
        For EACH characteristic, provide:
        - **Score**: High, Medium, Low, or No.
        - **Comment**: A very brief justification.
        
        DEFINITIONS:
        {strat_definitions}

        ### OUTPUT
        Extract strictly into the JSON schema provided. Ensure 'strategic_assessment' contains exactly 13 items corresponding to the list above.
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

    # --- NUEVO MÉTODO DE FEEDBACK ---
    def procesar_feedback_pdf(self, ruta_archivo):
        print("      -> Analizando Feedback con IA...")
        texto_pdf = self._leer_pdf(ruta_archivo)
        if not texto_pdf: return None
        
        # Recortamos por si es muy largo, pero el feedback suele ser corto
        texto_pdf = texto_pdf[:15000]

        prompt_system = """
        You are an assistant for a Recruiting Agency. You will receive a PDF text containing interview feedback from a Headhunter or external interviewer.
        
        Your Goal:
        1. Identify the **Candidate Name** mentioned in the document. It is usually at the top or in a field labeled "Candidate".
        2. Extract the **Feedback Body**. This is the qualitative evaluation, strengths, weaknesses, and comments. 
        
        Constraints:
        - IGNORE headers, footers, logos, company addresses, or confidential disclaimers.
        - IGNORE the name of the interviewer or the date, unless it's part of the narrative.
        - Return the candidate name in Title Case.
        - Keep the feedback text clean but preserve the original meaning and structure (bullet points are fine).
        """

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"Document Content:\n{texto_pdf}"},
                ],
                response_format=FeedbackData,
            )
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            print(f"   [IA ERROR] OpenAI Feedback: {e}")
            return None