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

    # Corporate Functions
    finance: SectorExperience = Field(description="Finance roles at companies (CFO, Controller, FP&A, Treasury).")
    marketing: SectorExperience = Field(description="Marketing roles at companies (CMO, Brand, Growth, Digital Marketing).")
    operations: SectorExperience = Field(description="Operations roles at companies (COO, Supply Chain, Logistics, Manufacturing).")
    product: SectorExperience = Field(description="Product roles at companies (CPO, Product Manager, Product Owner).")
    sales_revenue: SectorExperience = Field(description="Sales/Revenue roles at companies (CRO, Sales Director, Account Executive, Business Development).")
    technology: SectorExperience = Field(description="Technology roles at companies (CTO, VP Engineering, Software Engineer, IT Director).")

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
    strategic_assessment: List[AssessmentItem] = Field(description="List of strategic evaluations based on the provided definitions. Return empty list if no definitions provided.")

# --- MODELO PARA FEEDBACK (Markdown output) ---
class FeedbackResponse(BaseModel):
    candidate_name: str = Field(description="Full name of the candidate. Look for 'Candidate:', 'Name:', or the header.")
    feedback_markdown: str = Field(description="The entire feedback converted to clean Markdown. Use ## for sections, - for bullets, **bold** for emphasis. Convert tables to bullet summaries like '- **Category**: Value'. Preserve ALL content — do NOT summarize or omit anything.")

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

    def procesar_cv(self, ruta_archivo, matrix_characteristics=None):
        """
        Args:
            ruta_archivo: Path to CV file
            matrix_characteristics: Optional list of dicts with 'characteristic' and 'definition'.
                                   If None, skip strategic assessment entirely.
        """
        print("      -> Analizando CV con IA...")
        texto_cv = self._leer_pdf(ruta_archivo)
        if not texto_cv: return None

        texto_cv = texto_cv[:25000]

        # Build dynamic strat_definitions based on matrix_characteristics
        if matrix_characteristics:
            strat_definitions = "\n".join(
                f"{i+1}. {item['characteristic']}: {item['definition']}"
                for i, item in enumerate(matrix_characteristics)
            )
            num_items = len(matrix_characteristics)
            assessment_instruction = f"""
        ### STRATEGIC ASSESSMENT INSTRUCTIONS
        Evaluate the candidate against the following {num_items} characteristics using the definitions below.
        For EACH characteristic, provide:
        - **Score**: High, Medium, Low, or No.
        - **Comment**: A very brief justification.

        DEFINITIONS:
        {strat_definitions}

        ### OUTPUT
        Extract strictly into the JSON schema provided. Ensure 'strategic_assessment' contains exactly {num_items} items corresponding to the list above.
        """
        else:
            assessment_instruction = """
        ### STRATEGIC ASSESSMENT
        No strategic characteristics configured for this process.
        Return an empty list for 'strategic_assessment'.

        ### OUTPUT
        Extract strictly into the JSON schema provided.
        """

        prompt_system = f"""
        You are an elite Headhunter data entry specialist. Your job is to extract data from CVs.

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
        {assessment_instruction}
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

    def procesar_linkedin(self, linkedin_text: str, matrix_characteristics=None):
        """
        Parses LinkedIn profile markdown text (from Exa) into structured CVData.

        Args:
            linkedin_text: Raw markdown text of the LinkedIn profile.
            matrix_characteristics: Optional list of dicts with 'characteristic' and 'definition'.
        """
        print("      -> Analizando perfil LinkedIn con IA...")
        if not linkedin_text:
            return None

        linkedin_text = linkedin_text[:25000]

        # Build strategic assessment instruction (same logic as procesar_cv)
        if matrix_characteristics:
            strat_definitions = "\n".join(
                f"{i+1}. {item['characteristic']}: {item['definition']}"
                for i, item in enumerate(matrix_characteristics)
            )
            num_items = len(matrix_characteristics)
            assessment_instruction = f"""
        ### STRATEGIC ASSESSMENT INSTRUCTIONS
        Evaluate the candidate against the following {num_items} characteristics using the definitions below.
        For EACH characteristic, provide:
        - **Score**: High, Medium, Low, or No.
        - **Comment**: A very brief justification.

        DEFINITIONS:
        {strat_definitions}

        ### OUTPUT
        Extract strictly into the JSON schema provided. Ensure 'strategic_assessment' contains exactly {num_items} items corresponding to the list above.
        """
        else:
            assessment_instruction = """
        ### STRATEGIC ASSESSMENT
        No strategic characteristics configured for this process.
        Return an empty list for 'strategic_assessment'.

        ### OUTPUT
        Extract strictly into the JSON schema provided.
        """

        prompt_system = f"""
        You are an elite Headhunter data entry specialist. Your job is to extract structured candidate data from a LinkedIn profile.

        ### INPUT FORMAT
        The input is a LinkedIn profile converted to markdown. It may contain sections like:
        Experience, Education, Skills, Languages, About, Activity, Recommendations, Licenses & Certifications.
        Use ALL sections — any section may contain useful career context.

        ### CONTACT INFO RULES
        - Set email to null — LinkedIn rarely shows emails, and the candidate's contact info is already captured from the form.
        - Set phone to null — same reason.
        - Set linkedin_url to null — the URL is already known from the form data.

        ### CAPITALIZATION & CLEANING RULES (Apply to ALL text fields)
        1. **Title Case**: Always convert names to standard business casing.
        2. **Clean Names**: Remove legal suffixes (Ltd, Inc, S.A).

        ### EXPERIENCE RULES (The Golden Rules)
        1. **Management**: Include ONLY real companies. NEVER Consulting firms/Big4/Banks.
        2. **PE PortCo**: Only if explicitly stated as PE Portfolio Company.
        3. **International**: LIVED and WORKED > 6 months.
        4. **Date Calculation**: LinkedIn uses formats like "Jan 2020 - Present", "2018 - 2021", "Mar 2015 - Dec 2019". Calculate durations accurately. "Present" means the role is current.

        ### EDUCATION RULES
        1. **Simplification**: Map degrees to 'Engineering', 'Law', 'Economics', 'Business', 'Science', 'Humanities'.
        2. **MBA**: Extract School Name or "No".
        {assessment_instruction}
        """

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"LinkedIn Profile:\n{linkedin_text}"},
                ],
                response_format=CVData,
            )
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            print(f"   [IA ERROR] OpenAI LinkedIn: {e}")
            return None

    # --- FEEDBACK PDF → MARKDOWN ---
    def procesar_feedback_pdf(self, ruta_archivo):
        print("      -> Analizando Feedback con IA...")
        texto_pdf = self._leer_pdf(ruta_archivo)
        if not texto_pdf: return None

        texto_pdf = texto_pdf[:50000]

        prompt_system = """
        You receive raw text extracted from an executive search candidate report PDF. The text may be messy due to PDF extraction — columns may be interleaved, tables may be flattened, and sections may be out of order.

        Your job:
        1. Identify the **Candidate Name** (usually at the top). Return it in Title Case.
        2. Reformat the entire document into clean Markdown ready to paste into Notion.

        Formatting rules:
        - Use # for the candidate name as page title.
        - Use ## for major sections (e.g., Resumen, Trayectoria Profesional, Educación, etc.).
        - Use ### for subsections (e.g., individual roles within a company).
        - Reconstruct any tables (like fit/scorecard assessments) as proper Markdown tables using | Column | Column | syntax. Notion renders these natively.
        - Group related fields logically even if the PDF extraction scrambled them (e.g., keep all summary fields together, all compensation fields together).
        - Use **bold** for field labels followed by their values on the same line.
        - Use bullet points only for actual lists, not for key-value pairs.
        - Use --- between major sections.
        - For career history, nest roles under their parent company using ### for the company/period and #### for individual roles.

        Critical constraints:
        - Preserve ALL content. Do NOT summarize, shorten, or omit anything.
        - IGNORE headers, footers, page numbers, logos, and repeated candidate name/position footers.
        - IGNORE the search firm's contact details (emails, phones of the firm — NOT the candidate's).
        - Keep the original language of the document.
        - Output ONLY valid Markdown in the feedback_markdown field.
        """

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": f"Document Content:\n{texto_pdf}"},
                ],
                response_format=FeedbackResponse,
            )
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            print(f"   [IA ERROR] OpenAI Feedback: {e}")
            return None