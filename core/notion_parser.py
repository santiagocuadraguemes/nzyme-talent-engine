from core.domain_mapper import DomainMapper
from core.constants import *

class NotionParser:
    
    # --- HELPERS DE EXTRACCIÓN (Privados) ---
    
    @staticmethod
    def _extract_tags(prop_data):
        if not prop_data: return []
        if "multi_select" in prop_data:
            return [item["name"] for item in prop_data["multi_select"]]
        if "select" in prop_data and prop_data["select"]:
            return [prop_data["select"]["name"]]
        return []

    @staticmethod
    def _extract_text(prop_data):
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

    @staticmethod
    def extract_title(prop):
        if not prop: return ""
        return "".join([t["plain_text"] for t in prop.get("title", [])])

    @staticmethod
    def extract_multiselect_as_string(prop):
        """Devuelve valores unidos por comas (para logs o SQL simple)."""
        if not prop or not prop.get("multi_select"): return None
        return ", ".join([opt["name"] for opt in prop["multi_select"]])

    # --- MÉTODO PRINCIPAL DE PARSEO (Observer) ---

    @staticmethod
    def parse_candidate_properties(notion_props):
        """
        Convierte las propiedades crudas de Notion en un diccionario limpio.
        """
        data = {}
        
        # Extracción básica usando CONSTANTES
        raw_name = notion_props.get(PROP_NAME, {}).get("title", [])
        if raw_name: data["name"] = raw_name[0]["plain_text"]
        
        email = notion_props.get(PROP_EMAIL, {}).get("email")
        data["email"] = email if email else None
        
        linkedin = notion_props.get(PROP_LINKEDIN, {}).get("url")
        data["linkedin_url"] = linkedin if linkedin else None

        # Extracción de URL de CV
        cv_files = notion_props.get(PROP_CV_FILES, {}).get("files", [])
        if cv_files:
            archivo = cv_files[0]
            if "external" in archivo:
                data["cv_url"] = archivo["external"]["url"]
            elif "file" in archivo:
                data["cv_url"] = archivo["file"]["url"]
        else:
            data["cv_url"] = None

        # Datos extra (Listas y Selects)
        extra_data = {
            "cultural_fit": NotionParser._extract_tags(notion_props.get(PROP_CULTURAL_FIT)),
            "capabilities_assessment": NotionParser._extract_tags(notion_props.get(PROP_CAPABILITIES)),
            "referred_by": NotionParser._extract_text(notion_props.get(PROP_SOURCE)),
            "last_process": NotionParser._extract_select_name(notion_props.get(PROP_LAST_PROCESS)),
            "process_history": NotionParser._extract_tags(notion_props.get(PROP_PROCESS_HISTORY)),
            "proposed_team_role": NotionParser._extract_tags(notion_props.get(PROP_TEAM_ROLE))
        }

        # Mapeo de columnas de Experiencia usando CONSTANTES
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

        # Construcción del objeto JSON completo
        candidate_data_json = {
            "name": data.get("name"),
            "email": data.get("email"),
            "linkedin_url": data.get("linkedin_url"),
            "total_years_range": NotionParser._extract_select_name(notion_props.get(PROP_EXP_TOTAL_YEARS)),
            "languages": NotionParser._extract_tags(notion_props.get(PROP_LANGUAGES)),
            
            "last_process": extra_data["last_process"],
            "process_history": extra_data["process_history"],
            "proposed_team_role": extra_data["proposed_team_role"],
            
            "general": {
                "international_locations": NotionParser._extract_tags(notion_props.get(PROP_EXP_INTERNATIONAL)),
                "industries_specialized": NotionParser._extract_tags(notion_props.get(PROP_EXP_INDUSTRIES)),
                "cultural_fit": extra_data["cultural_fit"],
                "capabilities": extra_data["capabilities_assessment"],
                "source": extra_data["referred_by"]
            },
            "education": {
                "bachelors": NotionParser._extract_tags(notion_props.get(PROP_EDU_BACHELORS)),
                "masters": NotionParser._extract_tags(notion_props.get(PROP_EDU_MASTERS)),
                "university": NotionParser._extract_tags(notion_props.get(PROP_EDU_UNIVERSITY)),
                "mba": NotionParser._extract_tags(notion_props.get(PROP_EDU_MBA))
            },
            "experience": {}
        }

        # Reconstrucción de objetos de experiencia
        for notion_col, json_key in exp_fields_map.items():
            tags = NotionParser._extract_tags(notion_props.get(notion_col))
            candidate_data_json["experience"][json_key] = DomainMapper.reconstruct_experience_object(tags)

        data["candidate_data"] = candidate_data_json
        data["updated_at"] = "now()"
        
        return data