from datetime import date

class DataMapper:
    YEAR_TAGS = {
        "0-3 Years", "3-5 Years", "5-7 Years", "7-10 Years", 
        "10-15 Years", "15+ Years", "No"
    }

    # =========================================================================
    #  A. HELPERS DE LECTURA
    # =========================================================================

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
    def _reconstruct_experience_object(tag_list):
        companies = []
        years_range = None
        for tag in tag_list:
            if tag in DataMapper.YEAR_TAGS:
                years_range = tag
            elif tag != "No":
                companies.append(tag)
        return {
            "companies": companies,
            "years_range": years_range,
            "has_experience": len(companies) > 0
        }

    # =========================================================================
    #  B. HELPERS DE ESCRITURA (Todo default)
    # =========================================================================

    @staticmethod
    def _get_years_range_tag(years_float):
        y = years_float
        if not y or y <= 0: return None
        if y < 3: return "0-3 Years"
        if y < 5: return "3-5 Years"
        if y < 7: return "5-7 Years"
        if y < 10: return "7-10 Years"
        if y < 15: return "10-15 Years"
        return "15+ Years"

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
        range_tag = DataMapper._get_years_range_tag(years)
        if range_tag: tags.append({"name": range_tag, "color": "default"})
        if not tags: return [{"name": "No", "color": "default"}]
        return tags

    # =========================================================================
    #  C. MAPEO A NOTION (Harvester / Observer Enricher)
    # =========================================================================

    @staticmethod
    def map_to_notion(candidate_data, public_cv_url, process_name, existing_history=None, process_type=None, existing_team_role=None):
        """
        Construye el payload para Notion.
        - existing_history: Lista para 'Process History'.
        - process_type: String con el tipo de proceso actual (ej: "Investment - Associate").
        - existing_team_role: Lista actual de 'Proposed Nzyme Team & Role'.
        """
        exp = candidate_data.get("experience", {})
        edu = candidate_data.get("education", {})
        gen = candidate_data.get("general", {})
        
        # 1. Gestión de Historial de Procesos
        history_list = existing_history if existing_history else []
        if process_name and process_name not in history_list:
            history_list.append(process_name)
        history_tags = [{"name": p, "color": "default"} for p in history_list[-100:]]

        # 2. Gestión de Proposed Nzyme Team & Role (NUEVO REQUISITO)
        team_role_list = existing_team_role if existing_team_role else []
        # Si venimos de un proceso (Harvester) y hay un tipo de proceso, lo añadimos
        if process_type and process_type not in team_role_list:
            team_role_list.append(process_type)
        team_role_tags = [{"name": t, "color": "default"} for t in team_role_list]

        props = {
            "Name": {"title": [{"text": {"content": candidate_data["name"][:200]}}]},
            "Phone": {"phone_number": candidate_data.get("phone")},
            "LinkedIn": {"url": candidate_data.get("linkedin_url")},
            "CV": {"files": [{"name": "CV.pdf", "external": {"url": public_cv_url}}]},
            "Date Added": {"date": {"start": date.today().isoformat()}},
            
            # Columnas de Proceso y Equipo
            "Last Process Involved in": {"select": {"name": process_name, "color": "default"}} if process_name else None,
            "Process History": {"multi_select": history_tags},
            "Proposed Nzyme Team & Role": {"multi_select": team_role_tags} # <--- NUEVA COLUMNA
        }
        
        # Limpieza de None (si process_name es None, no enviamos Last Process para no borrarlo si no queremos)
        if not props["Last Process Involved in"]:
            del props["Last Process Involved in"]

        if candidate_data.get("email"):
            props["Email"] = {"email": candidate_data.get("email")}

        rango_total = DataMapper._get_years_range_tag(candidate_data.get("total_years", 0))
        if rango_total:
            props["EXPERIENCE: Total Years"] = {"select": {"name": rango_total, "color": "default"}}

        # Mapeo de Sectores
        sector_mapping = {
            "EXPERIENCE: Consulting": exp.get("consulting"),
            "EXPERIENCE: Audit": exp.get("audit"),
            "EXPERIENCE: Investment Banking": exp.get("ib"),
            "EXPERIENCE: Private Equity": exp.get("pe"),
            "EXPERIENCE: Venture Capital": exp.get("vc"),
            "EXPERIENCE: Engineer": exp.get("engineer_role"),
            "EXPERIENCE: Lawyer": exp.get("lawyer"),
            "EXPERIENCE: Founder": exp.get("founder"),
            "EXPERIENCE: Management (at companies)": exp.get("management"),
            "EXPERIENCE: Corporate M&A or Integrations": exp.get("corp_ma"),
            "EXPERIENCE: PE PortCo Roles": exp.get("portco_roles")
        }

        for prop_name, data in sector_mapping.items():
            props[prop_name] = {"multi_select": DataMapper._create_experience_tags(data)}

        # Listas Simples
        props["EXPERIENCE: International"] = {"multi_select": DataMapper._format_multi_select(gen.get("international_locations"))}
        props["EXPERIENCE: Industries"] = {"multi_select": DataMapper._format_multi_select(gen.get("industries_specialized"))}
        props["Languages"] = {"multi_select": DataMapper._format_multi_select(candidate_data.get("languages"))}

        # Educación
        props["EDUCATION: Bachelors"] = {"multi_select": DataMapper._format_multi_select(edu.get("bachelors"))}
        props["EDUCATION: Masters"] = {"multi_select": DataMapper._format_multi_select(edu.get("masters"))}
        props["EDUCATION: University"] = {"multi_select": DataMapper._format_multi_select(edu.get("university"))}
        
        mba_val = edu.get("mba", "No")
        props["EDUCATION: MBA"] = {"multi_select": [{"name": mba_val, "color": "default"}]}

        return props

    # =========================================================================
    #  D. MAPEO A SUPABASE (Harvester)
    # =========================================================================

    @staticmethod
    def map_to_supabase_candidate(candidate_data, public_cv_url):
        return {
            "name": candidate_data["name"],
            "email": candidate_data.get("email"),
            "linkedin_url": candidate_data.get("linkedin_url"),
            "cv_url": public_cv_url,
            "candidate_data": candidate_data, 
            "updated_at": "now()"
        }

    # =========================================================================
    #  E. MAPEO INVERSO COMPLETO (Observer)
    # =========================================================================

    @staticmethod
    def map_notion_to_supabase_update(notion_props):
        data = {}
        
        raw_name = notion_props.get("Name", {}).get("title", [])
        if raw_name: data["name"] = raw_name[0]["plain_text"]
        
        email = notion_props.get("Email", {}).get("email")
        data["email"] = email if email else None
        
        linkedin = notion_props.get("LinkedIn", {}).get("url")
        data["linkedin_url"] = linkedin if linkedin else None

        # Extracción de URL de CV
        cv_files = notion_props.get("CV", {}).get("files", [])
        if cv_files:
            archivo = cv_files[0]
            if "external" in archivo:
                data["cv_url"] = archivo["external"]["url"]
            elif "file" in archivo:
                data["cv_url"] = archivo["file"]["url"]
        else:
            data["cv_url"] = None

        extra_data = {
            "cultural_fit": DataMapper._extract_tags(notion_props.get("Cultural Fit")),
            "capabilities_assessment": DataMapper._extract_tags(notion_props.get("Capabilities Assessment")),
            "referred_by": DataMapper._extract_text(notion_props.get("Referred by/Sourced from...")),
            
            "last_process": DataMapper._extract_select_name(notion_props.get("Last Process Involved in")),
            "process_history": DataMapper._extract_tags(notion_props.get("Process History")),
            
            # --- NUEVA COLUMNA FUSIONADA ---
            "proposed_team_role": DataMapper._extract_tags(notion_props.get("Proposed Nzyme Team & Role"))
        }

        exp_fields_map = {
            "EXPERIENCE: Consulting": "consulting",
            "EXPERIENCE: Audit": "audit",
            "EXPERIENCE: Investment Banking": "ib",
            "EXPERIENCE: Private Equity": "pe",
            "EXPERIENCE: Venture Capital": "vc",
            "EXPERIENCE: Engineer": "engineer_role",
            "EXPERIENCE: Lawyer": "lawyer",
            "EXPERIENCE: Founder": "founder",
            "EXPERIENCE: Management (at companies)": "management",
            "EXPERIENCE: Corporate M&A or Integrations": "corp_ma",
            "EXPERIENCE: PE PortCo Roles": "portco_roles"
        }

        candidate_data_json = {
            "name": data.get("name"),
            "email": data.get("email"),
            "linkedin_url": data.get("linkedin_url"),
            "total_years_range": DataMapper._extract_select_name(notion_props.get("EXPERIENCE: Total Years")),
            "languages": DataMapper._extract_tags(notion_props.get("Languages")),
            
            "last_process": extra_data["last_process"],
            "process_history": extra_data["process_history"],
            "proposed_team_role": extra_data["proposed_team_role"], # Guardado aquí
            
            "general": {
                "international_locations": DataMapper._extract_tags(notion_props.get("EXPERIENCE: International")),
                "industries_specialized": DataMapper._extract_tags(notion_props.get("EXPERIENCE: Industries")),
                "cultural_fit": extra_data["cultural_fit"],
                "capabilities": extra_data["capabilities_assessment"],
                "source": extra_data["referred_by"]
            },
            "education": {
                "bachelors": DataMapper._extract_tags(notion_props.get("EDUCATION: Bachelors")),
                "masters": DataMapper._extract_tags(notion_props.get("EDUCATION: Masters")),
                "university": DataMapper._extract_tags(notion_props.get("EDUCATION: University")),
                "mba": DataMapper._extract_tags(notion_props.get("EDUCATION: MBA"))
            },
            "experience": {}
        }

        for notion_col, json_key in exp_fields_map.items():
            tags = DataMapper._extract_tags(notion_props.get(notion_col))
            candidate_data_json["experience"][json_key] = DataMapper._reconstruct_experience_object(tags)

        data["candidate_data"] = candidate_data_json
        data["updated_at"] = "now()"
        
        return data