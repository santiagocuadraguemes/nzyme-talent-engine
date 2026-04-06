# core/constants.py

# --- MAIN COLUMNS (NOTION) ---
PROP_NAME = "Name"
PROP_EMAIL = "Email"
PROP_LINKEDIN = "LinkedIn"
PROP_PHONE = "Phone"
PROP_CV_FILES = "CV"


# --- NEW FIELDS (SEPARATED) ---
PROP_CREATOR = "Creator"
PROP_SOURCE = "Source"
PROP_ASSESSMENT = "Assessment"


# --- CLASSIFICATION AND PROCESS ---
PROP_TEAM_ROLE = "Proposed Nzyme Teams & Roles"  # Pluralized
PROP_PROCESS_HISTORY = "Recruiting Processes History" # Renamed


# --- EDUCATION ---
PROP_EDU_BACHELORS = "EDUCATION: Bachelors"
PROP_EDU_MASTERS = "EDUCATION: Masters"
PROP_EDU_UNIVERSITIES = "EDUCATION: Universities" # Renamed
PROP_EDU_MBAS = "EDUCATION: MBAs"               # Renamed


# --- OTHER ---
PROP_LANGUAGES = "Languages"
PROP_DATE_ADDED = "Date Added" 


# --- EXPERIENCE ---
PROP_EXP_CONSULTING = "EXPERIENCE: Consulting"
PROP_EXP_AUDIT = "EXPERIENCE: Audit"
PROP_EXP_IB = "EXPERIENCE: Investment Banking"
PROP_EXP_PE = "EXPERIENCE: Private Equity"
PROP_EXP_VC = "EXPERIENCE: Venture Capital"
PROP_EXP_ENGINEER = "EXPERIENCE: Engineer"
PROP_EXP_LAWYER = "EXPERIENCE: Lawyer"
PROP_EXP_FOUNDER = "EXPERIENCE: Founder"
PROP_EXP_MANAGEMENT = "EXPERIENCE: Management (at companies)"
PROP_EXP_CORP_MA = "EXPERIENCE: Corporate M&A or Integrations"
PROP_EXP_PORTCO = "EXPERIENCE: PE PortCo Roles"
PROP_EXP_FINANCE = "EXPERIENCE: Finance (at companies)"
PROP_EXP_MARKETING = "EXPERIENCE: Marketing (at companies)"
PROP_EXP_OPERATIONS = "EXPERIENCE: Operations (at companies)"
PROP_EXP_PRODUCT = "EXPERIENCE: Product (at companies)"
PROP_EXP_SALES_REVENUE = "EXPERIENCE: Sales/Revenue (at companies)"
PROP_EXP_TECHNOLOGY = "EXPERIENCE: Technology (at companies)"
PROP_EXP_TOTAL_YEARS = "EXPERIENCE: Total Years"
PROP_EXP_INTERNATIONAL = "EXPERIENCE: International"
PROP_EXP_INDUSTRIES = "EXPERIENCE: Industries"


# --- SYSTEM PROPERTIES ---
PROP_ID = "ID"
PROP_CHECKBOX_PROCESSED = "Processed"
PROP_STAGE = "Stage"
PROP_HEADHUNTER = "Headhunter"  # Checkbox in Form DB
PROP_HEADHUNTER_FEEDBACK = "Headhunter's Feedback"  # File in Workflow DB
PROP_NEXT_STEPS = "Next Steps"
PROP_AI_PENDING = "AI Pending"
PROP_ASSESSMENT_REQUESTED = "Assessment Requested"


# --- PROCESS DASHBOARD (FACTORY WORKER) --- <--- NEW BLOCK!
PROP_READY_TO_PROCESS = "Ready to be Processed [Do not touch]"
PROP_PROCESSED_DASHBOARD = "Processed [Do not touch]"
PROP_PROCESS_TYPE = "Process Type"


# --- FEEDBACK ASSESSMENT CHILD DB ---
PROP_ASSESS_CHARACTERISTIC = "Characteristic"
PROP_ASSESS_DEFINITION = "Definition"
PROP_ASSESS_SCORE = "Score"
PROP_ASSESS_CV_EVIDENCE = "CV Evidence"
PROP_ASSESS_FEEDBACK_EVIDENCE = "Feedback Evidence"


# --- WEBHOOK HANDLER NAMES ---
HANDLER_PROCESS_LAUNCHER = "process_launcher"
HANDLER_PROCESS_DASHBOARD = "process_dashboard"
HANDLER_MAIN_CANDIDATE = "main_candidate"
HANDLER_CENTRAL_REFERENCE = "central_reference"
HANDLER_WORKFLOW_ITEM = "workflow_item"
HANDLER_FEEDBACK_FORM = "feedback_form"
HANDLER_FORM_SUBMISSION = "form_submission"
HANDLER_BULK_SUBMISSION = "bulk_submission"
HANDLER_OUTCOME_FORM = "outcome_form"
