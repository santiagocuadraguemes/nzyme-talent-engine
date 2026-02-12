from datetime import date

class DomainMapper:
    YEAR_TAGS = {
        "0-3 Years", "3-5 Years", "5-7 Years", "7-10 Years", 
        "10-15 Years", "15+ Years", "No"
    }

    @staticmethod
    def get_years_range_tag(years_float):
        """Calcula el rango de años basado en un número flotante."""
        y = years_float
        if y is None or y <= 0: return "No" # Devolvemos "No" por defecto si es 0
        if y < 3: return "0-3 Years"
        if y < 5: return "3-5 Years"
        if y < 7: return "5-7 Years"
        if y < 10: return "7-10 Years"
        if y < 15: return "10-15 Years"
        return "15+ Years"

    @staticmethod
    def _format_experience(sector_data):
        """
        Helper privado: Convierte el dato crudo de la IA (Float) 
        al formato limpio que quieres en el JSON (String Range).
        """
        if not sector_data:
            return {
                "companies": [],
                "years_range": "No",
                "has_experience": False
            }
        
        # 1. Extraemos los datos crudos de la IA
        raw_years = sector_data.get("years", 0)
        companies = sector_data.get("companies", [])
        has_exp = sector_data.get("has_experience", False)

        # 2. Calculamos el Tag (transformación Float -> String)
        range_tag = DomainMapper.get_years_range_tag(raw_years)

        # 3. Devolvemos el diccionario con la estructura EXACTA que quieres
        return {
            "companies": companies,
            "years_range": range_tag,
            "has_experience": has_exp
        }

    @staticmethod
    def map_to_supabase_candidate(ai_data, public_cv_url, source=None):
        """
        Prepara el diccionario híbrido para Supabase.
        Combina columnas SQL nuevas y transforma el JSON para que quede limpio.
        """
        # 1. Columnas SQL (Campos nuevos fuera del JSON)
        sql_columns = {
            "name": ai_data.get("name"),
            "email": ai_data.get("email"),
            "phone": ai_data.get("phone"),
            "linkedin_url": ai_data.get("linkedin_url"), # Ojo: key suele ser linkedin_url en el modelo IA
            "cv_url": public_cv_url,
            "assessment": None,
            "source": source
        }

        # Extraemos el bloque de experiencia crudo para procesarlo
        raw_exp = ai_data.get("experience", {})
        raw_edu = ai_data.get("education", {})
        raw_gen = ai_data.get("general", {})

        # 2. JSON Data (Estructura Limpia y Transformada)
        json_payload = {
            "name": ai_data.get("name"),
            "email": ai_data.get("email"),
            "linkedin_url": ai_data.get("linkedin_url"),
            
            # Calculamos el rango total también
            "total_years_range": DomainMapper.get_years_range_tag(ai_data.get("total_years", 0)),
            
            "languages": ai_data.get("languages", []),
            "recruiting_processes_history": [],
            "proposed_team_role": [], # Corregido a singular si así estaba en tu JSON "bueno"

            "general": {
                "international_locations": raw_gen.get("international_locations", []),
                "industries_specialized": raw_gen.get("industries_specialized", []),
            },

            "education": {
                "bachelors": raw_edu.get("bachelors", []),
                "masters": raw_edu.get("masters", []),
                "university": raw_edu.get("university", []),
                "mba": [raw_edu.get("mba")] if raw_edu.get("mba") and raw_edu.get("mba") != "No" else [] 
                # Nota: La IA a veces devuelve string "No" o el nombre. Lo metemos en lista para consistencia si quieres.
            },

            # Aquí aplicamos la limpieza sector por sector
            "experience": {
                "consulting": DomainMapper._format_experience(raw_exp.get("consulting")),
                "audit": DomainMapper._format_experience(raw_exp.get("audit")),
                "ib": DomainMapper._format_experience(raw_exp.get("ib") or raw_exp.get("investment_banking")),
                "pe": DomainMapper._format_experience(raw_exp.get("pe") or raw_exp.get("private_equity")),
                "vc": DomainMapper._format_experience(raw_exp.get("vc") or raw_exp.get("venture_capital")),
                "engineer_role": DomainMapper._format_experience(raw_exp.get("engineer_role")),
                "lawyer": DomainMapper._format_experience(raw_exp.get("lawyer")),
                "founder": DomainMapper._format_experience(raw_exp.get("founder")),
                "management": DomainMapper._format_experience(raw_exp.get("management")),
                "corp_ma": DomainMapper._format_experience(raw_exp.get("corp_ma")),
                "portco_roles": DomainMapper._format_experience(raw_exp.get("portco_roles")),
                "finance": DomainMapper._format_experience(raw_exp.get("finance")),
                "marketing": DomainMapper._format_experience(raw_exp.get("marketing")),
                "operations": DomainMapper._format_experience(raw_exp.get("operations")),
                "product": DomainMapper._format_experience(raw_exp.get("product")),
                "sales_revenue": DomainMapper._format_experience(raw_exp.get("sales_revenue")),
                "technology": DomainMapper._format_experience(raw_exp.get("technology")),
            }
        }

        return {**sql_columns, "candidate_data": json_payload}

    @staticmethod
    def reconstruct_experience_object(tag_list):
        """
        Reconstruye el objeto de experiencia desde tags planos.
        (Se mantiene para lectura desde Notion si hiciera falta).
        """
        if not tag_list: return None
        
        companies = []
        years_range = None
        for tag in tag_list:
            if tag in DomainMapper.YEAR_TAGS:
                years_range = tag
            elif tag != "No":
                companies.append(tag)
        
        return {
            "companies": companies,
            "years_range": years_range,
            "has_experience": len(companies) > 0
        }