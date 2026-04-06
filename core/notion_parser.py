from core.domain_mapper import DomainMapper
from core.logger import get_logger
from core.constants import (
    PROP_NAME, PROP_EMAIL, PROP_PHONE, PROP_LINKEDIN, PROP_CV_FILES,
    PROP_CREATOR, PROP_SOURCE, PROP_ASSESSMENT, PROP_PROCESS_HISTORY,
    PROP_TEAM_ROLE, PROP_EXP_TOTAL_YEARS, PROP_LANGUAGES,
    PROP_EXP_CONSULTING, PROP_EXP_AUDIT, PROP_EXP_IB, PROP_EXP_PE,
    PROP_EXP_VC, PROP_EXP_ENGINEER, PROP_EXP_LAWYER, PROP_EXP_FOUNDER,
    PROP_EXP_MANAGEMENT, PROP_EXP_CORP_MA, PROP_EXP_PORTCO,
    PROP_EXP_FINANCE, PROP_EXP_MARKETING, PROP_EXP_OPERATIONS,
    PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE, PROP_EXP_TECHNOLOGY,
    PROP_EXP_INTERNATIONAL, PROP_EXP_INDUSTRIES,
    PROP_EDU_BACHELORS, PROP_EDU_MASTERS, PROP_EDU_UNIVERSITIES,
    PROP_EDU_MBAS,
)

logger = get_logger("NotionParser")


class NotionParser:

    # --- EXTRACTION HELPERS (Private) ---

    @staticmethod
    def _extract_tags(prop_data):
        """Extracts list of strings from Select or Multi-select properties."""
        if not prop_data: return []
        if "multi_select" in prop_data:
            return [item["name"] for item in prop_data["multi_select"]]
        if "select" in prop_data and prop_data["select"]:
            return [prop_data["select"]["name"]]
        return []

    @staticmethod
    def _extract_text(prop_data):
        """Extracts string from Title, Rich Text or Text properties."""
        if not prop_data: return None
        if "rich_text" in prop_data:
            parts = prop_data["rich_text"]
            return "".join([p["plain_text"] for p in parts]) if parts else None
        if "title" in prop_data:
            parts = prop_data["title"]
            return "".join([p["plain_text"] for p in parts]) if parts else None
        return None

    @staticmethod
    def _extract_select_name(prop_data):
        if not prop_data: return None
        select = prop_data.get("select")
        return select["name"] if select else None

    # --- MAIN PARSING METHOD ---

    @staticmethod
    def parse_candidate_properties(notion_props):
        data = {}
        
        # 1. SQL FIELDS (First-class columns)
        raw_name = notion_props.get(PROP_NAME, {}).get("title", [])
        if raw_name: data["name"] = raw_name[0]["plain_text"]
        
        email = notion_props.get(PROP_EMAIL, {}).get("email")
        data["email"] = email if email else None
        
        phone = notion_props.get(PROP_PHONE, {}).get("phone_number")
        data["phone"] = phone if phone else None

        linkedin = notion_props.get(PROP_LINKEDIN, {}).get("url")
        data["linkedin_url"] = linkedin if linkedin else None

        # --- NEW SQL FIELDS ---

        # Creator and Source are MULTI-SELECT in Notion
        tags_creator = NotionParser._extract_tags(notion_props.get(PROP_CREATOR))
        data["creator"] = ", ".join(tags_creator) if tags_creator else None

        tags_source = NotionParser._extract_tags(notion_props.get(PROP_SOURCE))
        data["source"] = ", ".join(tags_source) if tags_source else None
        
        # Assessment is TAG in Notion -> Convert to String for SQL
        tags_assessment = NotionParser._extract_tags(notion_props.get(PROP_ASSESSMENT))
        data["assessment"] = ", ".join(tags_assessment) if tags_assessment else None

        # CV URL
        cv_files = notion_props.get(PROP_CV_FILES, {}).get("files", [])
        if cv_files:
            archivo = cv_files[0]
            if "external" in archivo:
                data["cv_url"] = archivo["external"]["url"]
            elif "file" in archivo:
                data["cv_url"] = archivo["file"]["url"]
        else:
            data["cv_url"] = None

        # 2. JSON FIELDS (Flexible Data)

        process_history = NotionParser._extract_tags(notion_props.get(PROP_PROCESS_HISTORY))
        proposed_teams = NotionParser._extract_tags(notion_props.get(PROP_TEAM_ROLE))

        # Experience column mapping
        exp_fields_map = {
            PROP_EXP_CONSULTING: "consulting",
            PROP_EXP_AUDIT: "audit",
            PROP_EXP_IB: "ib",
            PROP_EXP_PE: "pe",
            PROP_EXP_VC: "vc",
            PROP_EXP_ENGINEER: "engineer_role",
            PROP_EXP_LAWYER: "lawyer",
            PROP_EXP_FOUNDER: "founder",
            PROP_EXP_MANAGEMENT: "management",
            PROP_EXP_CORP_MA: "corp_ma",
            PROP_EXP_PORTCO: "portco_roles",
            PROP_EXP_FINANCE: "finance",
            PROP_EXP_MARKETING: "marketing",
            PROP_EXP_OPERATIONS: "operations",
            PROP_EXP_PRODUCT: "product",
            PROP_EXP_SALES_REVENUE: "sales_revenue",
            PROP_EXP_TECHNOLOGY: "technology",
        }

        candidate_data_json = {
            "name": data.get("name"),
            "email": data.get("email"),
            "linkedin_url": data.get("linkedin_url"),
            "total_years_range": NotionParser._extract_select_name(notion_props.get(PROP_EXP_TOTAL_YEARS)),
            "languages": NotionParser._extract_tags(notion_props.get(PROP_LANGUAGES)),
            
            # Plural fields
            "recruiting_processes_history": process_history,
            "proposed_teams_roles": proposed_teams,
            
            "general": {
                "international_locations": NotionParser._extract_tags(notion_props.get(PROP_EXP_INTERNATIONAL)),
                "industries_specialized": NotionParser._extract_tags(notion_props.get(PROP_EXP_INDUSTRIES)),
            },
            "education": {
                "bachelors": NotionParser._extract_tags(notion_props.get(PROP_EDU_BACHELORS)),
                "masters": NotionParser._extract_tags(notion_props.get(PROP_EDU_MASTERS)),
                "universities": NotionParser._extract_tags(notion_props.get(PROP_EDU_UNIVERSITIES)),
                "mbas": NotionParser._extract_tags(notion_props.get(PROP_EDU_MBAS))
            },
            "experience": {}
        }

        for notion_col, json_key in exp_fields_map.items():
            tags = NotionParser._extract_tags(notion_props.get(notion_col))
            candidate_data_json["experience"][json_key] = DomainMapper.reconstruct_experience_object(tags)

        data["candidate_data"] = candidate_data_json
        data["updated_at"] = "now()"

        present = [k for k, v in data.items() if v is not None and k != "updated_at"]
        missing = [k for k, v in data.items() if v is None and k != "updated_at"]
        logger.debug(f"Parsed candidate props — present: {present}, missing: {missing}")

        return data