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
PROP_CONFIDENTIAL_RELATION = "Relation to Main DB"


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
PROP_HEADHUNTER_RELATION = "Headhunter"  # Relation on Process Launcher -> Headhunters DB
PROP_HEADHUNTER_FEEDBACK = "Headhunter's Feedback"  # File in Workflow DB
PROP_NEXT_STEPS = "Next Steps"
PROP_AI_PENDING = "AI Pending"
PROP_ASSESSMENT_REQUESTED = "Assessment Requested"


# --- PROCESS DASHBOARD (FACTORY WORKER) ---
PROP_READY_TO_PROCESS = "Ready to be Processed [Do not touch]"
PROP_PROCESSED_DASHBOARD = "Processed [Do not touch]"
PROP_PROCESS_TYPE = "Process Type"
PROP_PROCESS_VISIBILITY = "Process Visibility"                    # Select: "Standard" | "Confidential"
PROP_GOVERNANCE_ACCESS = "Governance: Edit & View Access"         # People property (Dashboard + Main DB)


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


# --- DIRECT ENTRY ---
SOURCE_DIRECT_ENTRY_PREFIX = "Direct Entry"


# --- SOURCE VALUES (multi-select tag on Main DB) ---
# Code only writes to Source (never Creator), and only for:
#   - Form submission without Headhunter checkbox  → SOURCE_APPLIED_LINKEDIN
#   - Form submission with Headhunter checkbox     → "Headhunter - {firm}" (or SOURCE_HEADHUNTER_FALLBACK)
SOURCE_HEADHUNTER_PREFIX = "Headhunter - "
SOURCE_HEADHUNTER_FALLBACK = "Headhunter"  # when headhunter=true but process has no firm set
SOURCE_APPLIED_LINKEDIN = "Applied via LinkedIn"


# --- OUTCOME FORM (Discarded/Disqualified/Lost select on Outcome Form DB) ---
PROP_OUTCOME_SELECT = "Discarded/Disqualified/Lost"   # select on Outcome Form
PROP_OUTCOME_EXPLANATION = "Explanation"              # rich_text on Outcome Form

OUTCOME_DISCARDED    = "Discarded completely for Nzyme"
OUTCOME_DISQUALIFIED = "Disqualified only for this role"
OUTCOME_LOST         = "Lost for this process"

# Title prefix for the page created in the Confidential Assessments DB
OUTCOME_TITLE_PREFIX = {
    OUTCOME_DISCARDED:    "Discarded",
    OUTCOME_DISQUALIFIED: "Disqualified",
    OUTCOME_LOST:         "Lost",
}

# Assessment select option to write on the Confidential Assessments page.
# None means leave the property unset (no matching option exists in Notion).
ASSESSMENT_DISCARDED = "4. Discarded"
OUTCOME_ASSESSMENT_VALUE = {
    OUTCOME_DISCARDED:    ASSESSMENT_DISCARDED,
    OUTCOME_DISQUALIFIED: None,
    OUTCOME_LOST:         None,
}
