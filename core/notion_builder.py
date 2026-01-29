from datetime import date
from core.domain_mapper import DomainMapper
from core.constants import *

# Definimos aquí el valor por si no está en constants.py
VAL_DEFAULT_COLOR = "default"
VAL_NO = "No"

class NotionBuilder:
    
    # --- HELPERS DE FORMATO (Privados - ANTIGUOS RESTAURADOS) ---

    @staticmethod
    def _format_multi_select(items_list):
        """Convierte lista de strings a tags de Notion con color default."""
        if not items_list: return []
        # Filtramos nulos, limpiamos strings y forzamos color default
        unique_items = []
        seen = set()
        for i in items_list:
            if i:
                clean = str(i)[:100].strip().replace(",", "")
                if clean and clean not in seen:
                    unique_items.append({"name": clean, "color": VAL_DEFAULT_COLOR})
                    seen.add(clean)
        return unique_items

    @staticmethod
    def _create_experience_tags(sector_data):
        """
        Lógica ANTIGUA: Extrae explícitamente solo 'companies' y calcula el rango de años.
        Esto soluciona el problema de que aparezcan claves como 'has_experience'.
        """
        tags = []
        # Si es nulo o has_experience es False, devolvemos "No"
        if not sector_data or not sector_data.get("has_experience"):
            return [{"name": VAL_NO, "color": VAL_DEFAULT_COLOR}]
        
        # 1. Extraemos SOLO las empresas (ignoramos el resto de claves basura)
        companies = sector_data.get("companies", [])
        for c in companies:
            if c: 
                clean = str(c)[:100].strip().replace(",", "")
                tags.append({"name": clean, "color": VAL_DEFAULT_COLOR})
            
        # 2. Calculamos el rango de años (DomainMapper usa el float original)
        years = sector_data.get("years", 0)
        range_tag = DomainMapper.get_years_range_tag(years)
        
        # Solo añadimos el tag de años si es válido y distinto de "No"
        if range_tag and range_tag != VAL_NO: 
            tags.append({"name": range_tag, "color": VAL_DEFAULT_COLOR})
            
        # Si tras todo esto la lista está vacía, devolvemos "No"
        if not tags: return [{"name": VAL_NO, "color": VAL_DEFAULT_COLOR}]
        
        return tags

    # --- MÉTODO PRINCIPAL DE CONSTRUCCIÓN ---

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
        # Añadimos el tipo de proceso actual si no está
        if process_type and process_type not in team_role_list:
            team_role_list.append(process_type)
        team_role_tags = [{"name": t, "color": VAL_DEFAULT_COLOR} for t in team_role_list]

        props = {
            PROP_NAME: {"title": [{"text": {"content": candidate_data.get("name", "Unnamed")[:200]}}]},
            PROP_PHONE: {"phone_number": candidate_data.get("phone")},
            PROP_LINKEDIN: {"url": candidate_data.get("linkedin_url")},
            PROP_CV_FILES: {"files": [{"name": "CV.pdf", "external": {"url": public_cv_url}}]},
            
            # Auditoría
            PROP_DATE_ADDED: {"date": {"start": date.today().isoformat()}},
            
            
            # Clasificación (Usando constantes RENOMBRADAS/PLURALES)
            PROP_PROCESS_HISTORY: {"multi_select": history_tags},
            PROP_TEAM_ROLE: {"multi_select": team_role_tags}
        }
        
        # Email
        if candidate_data.get("email"):
            props[PROP_EMAIL] = {"email": candidate_data.get("email")}

        # Rango Total Años
        rango_total = DomainMapper.get_years_range_tag(candidate_data.get("total_years", 0))
        if rango_total:
            props[PROP_EXP_TOTAL_YEARS] = {"select": {"name": rango_total, "color": VAL_DEFAULT_COLOR}}

        # Mapeo de Sectores usando CONSTANTES
        # (Nombres de propiedades actualizados a lo que definimos en constants.py)
        sector_mapping = {
            PROP_EXP_CONSULTING: exp.get("consulting"),
            PROP_EXP_AUDIT: exp.get("audit"),
            PROP_EXP_IB: exp.get("ib") or exp.get("investment_banking"),
            PROP_EXP_PE: exp.get("pe") or exp.get("private_equity"),
            PROP_EXP_VC: exp.get("vc") or exp.get("venture_capital"),
            PROP_EXP_ENGINEER: exp.get("engineer_role"),
            PROP_EXP_LAWYER: exp.get("lawyer"),
            PROP_EXP_FOUNDER: exp.get("founder"),
            PROP_EXP_MANAGEMENT: exp.get("management"),
            PROP_EXP_CORP_MA: exp.get("corp_ma"),
            PROP_EXP_PORTCO: exp.get("portco_roles") or exp.get("portco")
        }

        for prop_name, data in sector_mapping.items():
            props[prop_name] = {"multi_select": NotionBuilder._create_experience_tags(data)}

        # Listas Simples
        if gen.get("international_locations"):
            props[PROP_EXP_INTERNATIONAL] = {"multi_select": NotionBuilder._format_multi_select(gen.get("international_locations"))}
        
        if gen.get("industries_specialized"):
            props[PROP_EXP_INDUSTRIES] = {"multi_select": NotionBuilder._format_multi_select(gen.get("industries_specialized"))}
        
        if candidate_data.get("languages"):
            props[PROP_LANGUAGES] = {"multi_select": NotionBuilder._format_multi_select(candidate_data.get("languages"))}

        # Educación (ADAPTADO A PLURALES NUEVOS)
        if edu.get("bachelors"):
            props[PROP_EDU_BACHELORS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("bachelors"))}
        
        if edu.get("masters"):
            props[PROP_EDU_MASTERS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("masters"))}
        
        if edu.get("university"):
            props[PROP_EDU_UNIVERSITIES] = {"multi_select": NotionBuilder._format_multi_select(edu.get("university"))}
        
        # MBA (Adaptado para manejar lista o string)
        mba_val = edu.get("mba")
        mba_list = []
        if isinstance(mba_val, list): mba_list = mba_val
        elif isinstance(mba_val, str) and mba_val != "No": mba_list = [mba_val]
        
        # Usamos PROP_EDU_MBAS (Plural)
        if mba_list:
            props[PROP_EDU_MBAS] = {"multi_select": NotionBuilder._format_multi_select(mba_list)}
        else:
            # Si quieres que ponga "No", descomenta esto, pero en multi-select suele ser mejor dejarlo vacío
            pass 

        return props