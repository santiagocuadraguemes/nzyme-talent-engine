from core.domain_mapper import DomainMapper
from core.constants import *

class NotionParser:
    
    # --- HELPERS DE EXTRACCIÓN (Privados) ---
    
    @staticmethod
    def _extract_tags(prop_data):
        """Extrae lista de strings de propiedades Select o Multi-select."""
        if not prop_data: return []
        if "multi_select" in prop_data:
            return [item["name"] for item in prop_data["multi_select"]]
        if "select" in prop_data and prop_data["select"]:
            return [prop_data["select"]["name"]]
        return []

    @staticmethod
    def _extract_text(prop_data):
        """Extrae string de propiedades Title, Rich Text o Text."""
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

    # --- MÉTODO PRINCIPAL DE PARSEO ---

    @staticmethod
    def parse_candidate_properties(notion_props):
        data = {}
        
        # 1. CAMPOS SQL (Columnas de primera clase)
        raw_name = notion_props.get(PROP_NAME, {}).get("title", [])
        if raw_name: data["name"] = raw_name[0]["plain_text"]
        
        email = notion_props.get(PROP_EMAIL, {}).get("email")
        data["email"] = email if email else None
        
        phone = notion_props.get(PROP_PHONE, {}).get("phone_number")
        data["phone"] = phone if phone else None

        linkedin = notion_props.get(PROP_LINKEDIN, {}).get("url")
        data["linkedin_url"] = linkedin if linkedin else None

        # --- NUEVOS CAMPOS SQL ---
        
        # Creator y Source son TEXTO en Notion
        data["creator"] = NotionParser._extract_text(notion_props.get(PROP_CREATOR))
        data["source"] = NotionParser._extract_text(notion_props.get(PROP_SOURCE))
        
        # Assessment es TAG en Notion -> Lo convertimos a String para SQL
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

        # 2. CAMPOS JSON (Flexible Data)
        
        process_history = NotionParser._extract_tags(notion_props.get(PROP_PROCESS_HISTORY))
        proposed_teams = NotionParser._extract_tags(notion_props.get(PROP_TEAM_ROLE))
        
        # Mapeo de columnas de Experiencia
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
            PROP_EXP_PORTCO: "portco_roles"
        }

        candidate_data_json = {
            "name": data.get("name"),
            "email": data.get("email"),
            "linkedin_url": data.get("linkedin_url"),
            "total_years_range": NotionParser._extract_select_name(notion_props.get(PROP_EXP_TOTAL_YEARS)),
            "languages": NotionParser._extract_tags(notion_props.get(PROP_LANGUAGES)),
            
            # Plurales
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
        
        return data