import os
import pdfplumber
import docx
from typing import List, Optional
from pydantic import BaseModel, Field
from openai import OpenAI
from dotenv import load_dotenv
import logfire
from enum import Enum
from core.logger import get_logger

load_dotenv()

# --- DATA MODELS ---

class SectorExperience(BaseModel):
    has_experience: bool = Field(description="True if the candidate worked in this sector.")
    years: float = Field(description="Total years worked in this sector.")
    companies: List[str] = Field(
        description=(
            "List of COMPANY NAMES only. Rules: "
            "1) NEVER include cities, countries, or locations (e.g. NO 'Luxembourg', 'Madrid', 'London'). "
            "2) Use canonical short names: 'McKinsey' not 'McKinsey & Company', 'Bain' not 'Bain & Company', "
            "'BCG' not 'Boston Consulting Group', 'JPMorgan' not 'J.P. Morgan Chase & Co.', "
            "'Goldman Sachs' not 'Goldman Sachs Group Inc'. "
            "3) Remove legal suffixes (Ltd, Inc, S.A., GmbH, SL, LLC, Corp). "
            "4) Fix capitalization to standard business casing. "
            "5) One entry per company — no duplicates."
        )
    )

class FunctionalExperience(BaseModel):
    has_experience: bool = Field(description="True if the candidate held this type of functional role at a real company.")
    years: float = Field(description="Total years in this functional area.")
    roles: List[str] = Field(
        description=(
            "List of ROLE TITLES only (e.g. 'CFO', 'VP Finance', 'Product Manager', 'COO'). "
            "Rules: 1) NEVER include company names here. 2) Use clean standard titles. "
            "3) One entry per distinct role title held in this function."
        )
    )

class ExperienceBreakdown(BaseModel):
    # Advisory / Services (SECTOR-BASED: use SectorExperience with company names)
    consulting: SectorExperience = Field(description="Strategy/Management Consulting firms ONLY (McKinsey, Bain, BCG, Roland Berger, LEK, Oliver Wyman, etc). Do NOT include Big4 here — they go in 'audit'. Do NOT include in-house strategy roles — those go in 'management'.")
    audit: SectorExperience = Field(description="Big4 and audit firms ONLY (Deloitte, PwC, EY, KPMG, BDO, Grant Thornton). Include all service lines (advisory, tax, audit) if the employer is a Big4 firm.")
    ib: SectorExperience = Field(description="Investment Banking divisions of banks ONLY (Goldman Sachs, JPMorgan, Morgan Stanley, Lazard, Rothschild, Citi, etc). The candidate must have worked IN the IB division, not just at a bank in another role.")
    pe: SectorExperience = Field(description="Private Equity funds ONLY (Blackstone, KKR, Carlyle, Apollo, CVC, Advent, etc). Must be employed BY the PE fund. Do NOT include PE portfolio company roles here (those go in 'portco_roles').")
    vc: SectorExperience = Field(description="Venture Capital funds ONLY (Sequoia, a16z, Accel, Index Ventures, etc). Must be employed BY the VC fund.")

    # Technical / Founder (SECTOR-BASED: use SectorExperience with company names)
    engineer_role: SectorExperience = Field(description="Technical/engineering roles where the PRIMARY title is engineer (Software Engineer, Civil Engineer, Mechanical Engineer, Data Engineer, etc). Do NOT include CTO/VP Engineering here — those go in 'technology'.")
    lawyer: SectorExperience = Field(description="Legal roles: lawyers at law firms or in-house counsel/General Counsel at companies.")
    founder: SectorExperience = Field(description="Founded or co-founded their own company. The company name goes in 'companies'.")

    # Corporate / Industry (SECTOR-BASED: use SectorExperience with company names)
    management: FunctionalExperience = Field(description="CRITICAL: General management ROLE TITLES (CEO, GM, Managing Director, Country Manager) inside REAL operating companies. EXCLUDE: Consulting firms, Big4, Banks, PE/VC funds — those have their own fields. Only include roles where the candidate was an actual employee in a general management position.")
    corp_ma: SectorExperience = Field(description="Internal M&A / Corporate Development roles inside a real company (not at advisory firms). Head of M&A, VP Corp Dev, etc.")

    # Portfolio Roles (SECTOR-BASED: use SectorExperience with company names)
    portco_roles: SectorExperience = Field(description="C-Level/Management roles in companies EXPLICITLY described as PE portfolio companies or PE-backed companies.")

    # Corporate Functions (FUNCTIONAL: use FunctionalExperience with role titles, NOT company names)
    finance: FunctionalExperience = Field(description="Finance function roles: CFO, VP Finance, Controller, FP&A Director, Treasurer, Head of Finance. Only classify here if the PRIMARY job title is finance-focused. Do NOT include: investment bankers (those go in 'ib'), financial advisors at consulting firms (those go in 'consulting').")
    marketing: FunctionalExperience = Field(description="Marketing function roles: CMO, VP Marketing, Head of Brand, Growth Director, Digital Marketing Manager. Only classify here if the PRIMARY job title is marketing-focused.")
    operations: FunctionalExperience = Field(description="Operations function roles: COO, VP Operations, Supply Chain Director, Head of Logistics, Plant Manager, Manufacturing Director. Only classify here if the PRIMARY job title is operations-focused.")
    product: FunctionalExperience = Field(description="Product function roles: CPO, VP Product, Product Manager, Product Owner, Head of Product. Only classify here if the PRIMARY job title is product-focused. A Product Manager goes here, NOT in finance even if they manage a P&L.")
    sales_revenue: FunctionalExperience = Field(description="Sales/Revenue function roles: CRO, VP Sales, Sales Director, Account Executive, Head of Business Development, Commercial Director. Only classify here if the PRIMARY job title is sales/revenue-focused.")
    technology: FunctionalExperience = Field(description="Technology leadership roles: CTO, VP Engineering, IT Director, Head of Technology, Chief Digital Officer. Only classify here if the PRIMARY job title is technology leadership. Do NOT include hands-on engineers (those go in 'engineer_role').")

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

# --- FEEDBACK ASSESSMENT MODEL (CV + Interview Feedback) ---
class FeedbackAssessmentItem(BaseModel):
    characteristic: str = Field(description="The exact name of the characteristic as listed in the instructions.")
    score: StrategicScore = Field(description="The assessment score based on CV and interview feedback evidence.")
    cv_evidence: str = Field(description="Brief evidence from the CV supporting this score (max 20 words). Use 'No CV available' if no CV was provided.")
    feedback_evidence: str = Field(description="Brief evidence from interview feedback supporting this score (max 20 words). Use 'No feedback available' if no feedback was provided.")

class FeedbackAssessmentResponse(BaseModel):
    assessment: List[FeedbackAssessmentItem] = Field(description="List of scored assessments, one per characteristic.")
    overall_summary: str = Field(description="2-3 sentence synthesis of the candidate's overall fit based on all evidence.")

# --- FEEDBACK MODEL (Markdown output) ---
class FeedbackResponse(BaseModel):
    candidate_name: str = Field(description="Full name of the candidate. Look for 'Candidate:', 'Name:', or the header.")
    feedback_markdown: str = Field(description="The entire feedback converted to clean Markdown. Use ## for sections, - for bullets, **bold** for emphasis. Convert tables to bullet summaries like '- **Category**: Value'. Preserve ALL content — do NOT summarize or omit anything.")

# --- MAIN CLASS ---

class CVAnalyzer:
    def __init__(self):
        self.logger = get_logger("AIParser")
        key = os.getenv("OPENAI_API_KEY")
        if not key: raise ValueError("Missing OPENAI_API_KEY in .env")
        self.client = OpenAI(api_key=key)
        if os.getenv("LOGFIRE_TOKEN"):
            logfire.configure()
            logfire.instrument_openai(self.client)
        self.model_name = "gpt-5-mini"

    def _read_pdf(self, file_path):
        text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    text += (page.extract_text() or "") + "\n"
            self.logger.debug(f"PDF read OK: {len(text)} chars from {file_path}")
            return text
        except Exception as e:
            self.logger.error(f"Error reading PDF: {e}")
            return None

    def _read_docx(self, file_path):
        text = ""
        try:
            doc = docx.Document(file_path)
            for para in doc.paragraphs:
                text += (para.text or "") + "\n"
            self.logger.debug(f"DOCX read OK: {len(text)} chars from {file_path}")
            return text
        except Exception as e:
            self.logger.error(f"Error reading DOCX: {e}")
            return None

    def _read_file(self, file_path):
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        if ext == ".pdf":
            return self._read_pdf(file_path)
        elif ext in (".docx", ".doc"):
            return self._read_docx(file_path)
        else:
            self.logger.warning(f"Unsupported file format: {ext}")
            return None

    def process_cv(self, file_path, matrix_characteristics=None):
        """
        Args:
            file_path: Path to CV file
            matrix_characteristics: Optional list of dicts with 'characteristic' and 'definition'.
                                   If None, skip strategic assessment entirely.
        """
        self.logger.info("Analyzing CV with AI...")
        cv_text = self._read_file(file_path)
        if not cv_text: return None

        self.logger.debug(f"CV text before truncation: {len(cv_text)} chars")
        cv_text = cv_text[:25000]
        self.logger.debug(f"CV text after truncation: {len(cv_text)} chars")

        # Build dynamic strat_definitions based on matrix_characteristics
        self.logger.debug(f"Strategic assessment configured: {bool(matrix_characteristics)} ({len(matrix_characteristics) if matrix_characteristics else 0} characteristics)")
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

        ### FUNCTIONAL ROLE CLASSIFICATION RULES
        1. **Classify by PRIMARY job title**, not secondary responsibilities. A "Product Manager" who handles budgets goes in Product, NOT Finance.
        2. **Each role belongs to ONE functional category only.** Use this priority: if the title matches a specific function (Finance, Marketing, Operations, Product, Sales, Technology), put it there. Only use Management for general management titles (CEO, GM, Managing Director) that don't fit a specific function.
        3. **Consulting/Big4/IB/PE/VC firms NEVER appear in functional fields.** A "CFO at McKinsey" goes in Consulting (sector), not Finance (function). The employer type determines sector fields; the role title determines functional fields only for real operating companies.
        4. **Company names NEVER go in functional role fields.** Functional fields (finance, marketing, operations, product, sales_revenue, technology) use the 'roles' list for role TITLES only.
        5. **No locations in sector company fields.** Never put cities or countries in the 'companies' list.

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
                    {"role": "user", "content": f"CV Content:\n{cv_text}"},
                ],
                response_format=CVData,
            )
            usage = completion.usage
            self.logger.debug(f"CV parse OK — tokens: prompt={usage.prompt_tokens}, completion={usage.completion_tokens}, total={usage.total_tokens}")
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            self.logger.error(f"OpenAI CV parsing error: {e}")
            self.logger.debug(f"CV parse FAILED: {type(e).__name__}: {e}")
            return None

    def process_linkedin(self, linkedin_text: str, matrix_characteristics=None):
        """
        Parses LinkedIn profile markdown text (from Exa) into structured CVData.

        Args:
            linkedin_text: Raw markdown text of the LinkedIn profile.
            matrix_characteristics: Optional list of dicts with 'characteristic' and 'definition'.
        """
        self.logger.info("Analyzing LinkedIn profile with AI...")
        if not linkedin_text:
            return None

        self.logger.debug(f"LinkedIn text length: {len(linkedin_text)} chars (pre-truncation)")
        linkedin_text = linkedin_text[:25000]
        self.logger.debug(f"LinkedIn text after truncation: {len(linkedin_text)} chars")

        # Build strategic assessment instruction (same logic as process_cv)
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

        ### FUNCTIONAL ROLE CLASSIFICATION RULES
        1. **Classify by PRIMARY job title**, not secondary responsibilities. A "Product Manager" who handles budgets goes in Product, NOT Finance.
        2. **Each role belongs to ONE functional category only.** Use this priority: if the title matches a specific function (Finance, Marketing, Operations, Product, Sales, Technology), put it there. Only use Management for general management titles (CEO, GM, Managing Director) that don't fit a specific function.
        3. **Consulting/Big4/IB/PE/VC firms NEVER appear in functional fields.** A "CFO at McKinsey" goes in Consulting (sector), not Finance (function). The employer type determines sector fields; the role title determines functional fields only for real operating companies.
        4. **Company names NEVER go in functional role fields.** Functional fields (finance, marketing, operations, product, sales_revenue, technology) use the 'roles' list for role TITLES only.
        5. **No locations in sector company fields.** Never put cities or countries in the 'companies' list.

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
            usage = completion.usage
            self.logger.debug(f"LinkedIn parse OK — tokens: prompt={usage.prompt_tokens}, completion={usage.completion_tokens}, total={usage.total_tokens}")
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            self.logger.error(f"OpenAI LinkedIn parsing error: {e}")
            self.logger.debug(f"LinkedIn parse FAILED: {type(e).__name__}: {e}")
            return None

    # --- FEEDBACK PDF → MARKDOWN ---
    def process_feedback_pdf(self, file_path):
        self.logger.info("Analyzing feedback with AI...")
        pdf_text = self._read_pdf(file_path)
        if not pdf_text: return None

        self.logger.debug(f"Feedback PDF text: {len(pdf_text)} chars (pre-truncation)")
        pdf_text = pdf_text[:50000]
        self.logger.debug(f"Feedback PDF text after truncation: {len(pdf_text)} chars")

        prompt_system = """
        You receive raw text extracted from an executive search candidate report PDF. The text may be messy due to PDF extraction — columns may be interleaved, tables may be flattened, and sections may be out of order.

        Your job:
        1. Identify the **Candidate Name** (usually at the top). Return it in Title Case.
        2. Reformat the entire document into clean Markdown ready to paste into Notion.

        Formatting rules:
        - Use # for the candidate name as page title.
        - Use ## for major sections (e.g., Resumen, Trayectoria Profesional, Educación, etc.).
        - Use ### for subsections (e.g., individual roles within a company).
        - DO NOT USE #### as they don't render well in Notion and can break the formatting.
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
                    {"role": "user", "content": f"Document Content:\n{pdf_text}"},
                ],
                response_format=FeedbackResponse,
            )
            usage = completion.usage
            self.logger.debug(f"Feedback parse OK — tokens: prompt={usage.prompt_tokens}, completion={usage.completion_tokens}, total={usage.total_tokens}")
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            self.logger.error(f"OpenAI feedback parsing error: {e}")
            self.logger.debug(f"Feedback parse FAILED: {type(e).__name__}: {e}")
            return None

    # --- FEEDBACK ASSESSMENT (CV + Interview Feedback → Scored Matrix) ---
    def process_feedback_assessment(self, cv_text, feedback_texts, assessment_characteristics):
        """
        Generates a scored assessment matrix using CV + gathered interview feedback.

        Args:
            cv_text: Extracted CV text (or None if unavailable).
            feedback_texts: List of {"title": str, "content": str} from Gathered Feedback pages.
            assessment_characteristics: List of {"characteristic": str, "definition": str}.
        Returns:
            Dict with 'assessment' list and 'overall_summary', or None on failure.
        """
        self.logger.info("Generating feedback assessment with AI...")

        num_chars = len(assessment_characteristics)
        char_definitions = "\n".join(
            f"{i+1}. {item['characteristic']}: {item['definition']}"
            for i, item in enumerate(assessment_characteristics)
        )

        # Build CV section
        if cv_text:
            cv_text = cv_text[:25000]
            cv_section = f"## CANDIDATE CV\n{cv_text}"
        else:
            cv_section = "## CANDIDATE CV\nNo CV available for this candidate."

        # Build feedback section
        if feedback_texts:
            feedback_parts = []
            for fb in feedback_texts:
                content = fb["content"][:10000] if fb.get("content") else ""
                feedback_parts.append(f"### {fb.get('title', 'Unknown Interviewer')}\n{content}")
            feedback_section = "## GATHERED INTERVIEW FEEDBACK\n" + "\n\n".join(feedback_parts)
        else:
            feedback_section = "## GATHERED INTERVIEW FEEDBACK\nNo interview feedback available yet."

        prompt_system = f"""You are an executive recruitment assessment specialist. Your job is to evaluate a candidate against specific characteristics using ALL available evidence: their CV and gathered interview feedback.

### EVALUATION RULES
1. Evaluate EACH characteristic independently using the provided definition.
2. For each characteristic, cite specific evidence from the CV and from interview feedback SEPARATELY.
3. Be objective and evidence-based. If evidence is weak or absent for a source, say so explicitly.
4. Scores: High = strong evidence of fit, Medium = some evidence, Low = weak/negative evidence, No = no relevant evidence at all.
5. Keep cv_evidence and feedback_evidence to max 20 words each.
6. The overall_summary should synthesize key strengths, gaps, and overall fit in 2-3 sentences.

### CHARACTERISTICS TO EVALUATE ({num_chars} total)
{char_definitions}

### OUTPUT
Return exactly {num_chars} assessment items, one per characteristic, in the same order as listed above."""

        user_message = f"{cv_section}\n\n{feedback_section}"

        try:
            completion = self.client.beta.chat.completions.parse(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": prompt_system},
                    {"role": "user", "content": user_message},
                ],
                response_format=FeedbackAssessmentResponse,
            )
            usage = completion.usage
            self.logger.debug(f"Feedback assessment OK — tokens: prompt={usage.prompt_tokens}, completion={usage.completion_tokens}, total={usage.total_tokens}")
            return completion.choices[0].message.parsed.model_dump()

        except Exception as e:
            self.logger.error(f"OpenAI feedback assessment error: {e}")
            self.logger.debug(f"Feedback assessment FAILED: {type(e).__name__}: {e}")
            return None
