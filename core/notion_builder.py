from datetime import date
from core.domain_mapper import DomainMapper
from core.constants import *

class NotionBuilder:
    
    # --- HELPERS DE FORMATO (Privados) ---

    @staticmethod
    def _format_multi_select(items_list):
        if not items_list: return []
        return [{"name": str(i)[:100].strip().replace(",", ""), "color": "default"} for i in items_list]

    @staticmethod
    def _create_experience_tags(sector_data):
        tags = []
        if not sector_data or not sector_data.get("has_experience"):
            return [{"name": "No", "color": "default"}]
        
        companies = sector_data.get("companies", [])
        for c in companies:
            if c: tags.append({"name": str(c)[:100].strip().replace(",", ""), "color": "default"})
            
        years = sector_data.get("years", 0)
        # Usamos DomainMapper para la lógica de rangos
        range_tag = DomainMapper.get_years_range_tag(years)
        
        if range_tag: tags.append({"name": range_tag, "color": "default"})
        if not tags: return [{"name": "No", "color": "default"}]
        return tags

    # --- MÉTODO PRINCIPAL DE CONSTRUCCIÓN (Harvester) ---

    @staticmethod
    def build_candidate_payload(candidate_data, public_cv_url, process_name, existing_history=None, process_type=None, existing_team_role=None):
        """
        Construye el cuerpo de la petición para Crear/Actualizar pagina en Notion.
        """
        exp = candidate_data.get("experience", {})
        edu = candidate_data.get("education", {})
        gen = candidate_data.get("general", {})
        
        # 1. Gestión de Historial de Procesos
        history_list = existing_history if existing_history else []
        if process_name and process_name not in history_list:
            history_list.append(process_name)
        history_tags = [{"name": p, "color": VAL_DEFAULT_COLOR} for p in history_list[-100:]]

        # 2. Gestión de Proposed Nzyme Team & Role
        team_role_list = existing_team_role if existing_team_role else []
        if process_type and process_type not in team_role_list:
            team_role_list.append(process_type)
        team_role_tags = [{"name": t, "color": VAL_DEFAULT_COLOR} for t in team_role_list]

        props = {
            PROP_NAME: {"title": [{"text": {"content": candidate_data["name"][:200]}}]},
            PROP_PHONE: {"phone_number": candidate_data.get("phone")},
            PROP_LINKEDIN: {"url": candidate_data.get("linkedin_url")},
            PROP_CV_FILES: {"files": [{"name": "CV.pdf", "external": {"url": public_cv_url}}]},
            PROP_DATE_ADDED: {"date": {"start": date.today().isoformat()}},
            
            PROP_LAST_PROCESS: {"select": {"name": process_name, "color": VAL_DEFAULT_COLOR}} if process_name else None,
            PROP_PROCESS_HISTORY: {"multi_select": history_tags},
            PROP_TEAM_ROLE: {"multi_select": team_role_tags}
        }
        
        # Limpieza de None
        if not props[PROP_LAST_PROCESS]:
            del props[PROP_LAST_PROCESS]

        if candidate_data.get("email"):
            props[PROP_EMAIL] = {"email": candidate_data.get("email")}

        rango_total = DomainMapper.get_years_range_tag(candidate_data.get("total_years", 0))
        if rango_total:
            props[PROP_EXP_TOTAL_YEARS] = {"select": {"name": rango_total, "color": VAL_DEFAULT_COLOR}}

        # Mapeo de Sectores usando CONSTANTES
        sector_mapping = {
            PROP_EXP_CONSULTING: exp.get("consulting"),
            PROP_EXP_AUDIT: exp.get("audit"),
            PROP_EXP_IB: exp.get("ib"),
            PROP_EXP_PE: exp.get("pe"),
            PROP_EXP_VC: exp.get("vc"),
            PROP_EXP_ENGINEER: exp.get("engineer_role"),
            PROP_EXP_LAWYER: exp.get("lawyer"),
            PROP_EXP_FOUNDER: exp.get("founder"),
            PROP_EXP_MANAGEMENT: exp.get("management"),
            PROP_EXP_CORP_MA: exp.get("corp_ma"),
            PROP_EXP_PORTCO: exp.get("portco_roles")
        }

        for prop_name, data in sector_mapping.items():
            props[prop_name] = {"multi_select": NotionBuilder._create_experience_tags(data)}

        # Listas Simples
        props[PROP_EXP_INTERNATIONAL] = {"multi_select": NotionBuilder._format_multi_select(gen.get("international_locations"))}
        props[PROP_EXP_INDUSTRIES] = {"multi_select": NotionBuilder._format_multi_select(gen.get("industries_specialized"))}
        props[PROP_LANGUAGES] = {"multi_select": NotionBuilder._format_multi_select(candidate_data.get("languages"))}

        # Educación
        props[PROP_EDU_BACHELORS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("bachelors"))}
        props[PROP_EDU_MASTERS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("masters"))}
        props[PROP_EDU_UNIVERSITY] = {"multi_select": NotionBuilder._format_multi_select(edu.get("university"))}
        
        mba_val = edu.get("mba", VAL_NO)
        props[PROP_EDU_MBA] = {"multi_select": [{"name": mba_val, "color": VAL_DEFAULT_COLOR}]}

        return props