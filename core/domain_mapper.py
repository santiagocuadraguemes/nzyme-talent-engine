from core.logger import get_logger

logger = get_logger("DomainMapper")


class DomainMapper:
    YEAR_TAGS = {
        "0-3 Years", "3-5 Years", "5-7 Years", "7-10 Years", 
        "10-15 Years", "15+ Years", "No"
    }

    @staticmethod
    def get_years_range_tag(years_float):
        """Calculates the years range based on a float value."""
        y = years_float
        if y is None or y <= 0: return "No"
        if y < 3: return "0-3 Years"
        if y < 5: return "3-5 Years"
        if y < 7: return "5-7 Years"
        if y < 10: return "7-10 Years"
        if y < 15: return "10-15 Years"
        return "15+ Years"

    @staticmethod
    def _format_experience(sector_data):
        """
        Converts raw AI output (float years) to clean JSON format (string range).
        Handles both sector-based (companies) and functional (roles) experience types.
        """
        if not sector_data:
            return {
                "companies": [],
                "roles": [],
                "years_range": "No",
                "has_experience": False
            }

        raw_years = sector_data.get("years", 0)
        companies = sector_data.get("companies", [])
        roles = sector_data.get("roles", [])
        has_exp = sector_data.get("has_experience", False)

        range_tag = DomainMapper.get_years_range_tag(raw_years)
        logger.debug(f"_format_experience → has_experience={has_exp}, years={raw_years}, years_range='{range_tag}', companies={len(companies)}, roles={len(roles)}")

        return {
            "companies": companies,
            "roles": roles,
            "years_range": range_tag,
            "has_experience": has_exp
        }

    @staticmethod
    def map_to_supabase_candidate(ai_data, public_cv_url, source=None):
        """
        Prepares the hybrid dictionary for Supabase.
        Combines new SQL columns and transforms the JSON for a clean structure.
        """
        # 1. SQL Columns (fields outside JSON)
        sql_columns = {
            "name": ai_data.get("name"),
            "email": ai_data.get("email"),
            "phone": ai_data.get("phone"),
            "linkedin_url": ai_data.get("linkedin_url"),
            "cv_url": public_cv_url,
            "assessment": None,
            "source": source
        }
        logger.debug(f"map_to_supabase_candidate SQL columns: name={'set' if sql_columns['name'] else 'missing'}, email={'set' if sql_columns['email'] else 'missing'}, phone={'set' if sql_columns['phone'] else 'missing'}, linkedin={'set' if sql_columns['linkedin_url'] else 'missing'}, cv_url={'set' if sql_columns['cv_url'] else 'missing'}")

        raw_exp = ai_data.get("experience", {})
        raw_edu = ai_data.get("education", {})
        raw_gen = ai_data.get("general", {})

        # 2. JSON Data (Clean and Transformed Structure)
        json_payload = {
            "name": ai_data.get("name"),
            "email": ai_data.get("email"),
            "linkedin_url": ai_data.get("linkedin_url"),

            "total_years_range": DomainMapper.get_years_range_tag(ai_data.get("total_years", 0)),

            "languages": ai_data.get("languages", []),
            "recruiting_processes_history": [],
            "proposed_teams_roles": [],

            "general": {
                "international_locations": raw_gen.get("international_locations", []),
                "industries_specialized": raw_gen.get("industries_specialized", []),
            },

            "education": {
                "bachelors": raw_edu.get("bachelors", []),
                "masters": raw_edu.get("masters", []),
                "university": raw_edu.get("university", []),
                "mba": [raw_edu.get("mba")] if raw_edu.get("mba") and raw_edu.get("mba") != "No" else []
            },

            # Apply cleanup sector by sector
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

        logger.debug(f"map_to_supabase_candidate JSON payload keys: {list(json_payload.keys())}")
        return {**sql_columns, "candidate_data": json_payload}

    @staticmethod
    def reconstruct_experience_object(tag_list):
        """
        Reconstructs experience object from flat Notion multi-select tags.
        Tags could be company names or role titles (both stored the same way in Notion).
        """
        if not tag_list: return None

        logger.debug(f"reconstruct_experience_object → {len(tag_list)} tag(s) in")
        companies = []
        years_range = None
        for tag in tag_list:
            if tag in DomainMapper.YEAR_TAGS:
                years_range = tag
            elif tag != "No":
                companies.append(tag)

        logger.debug(f"reconstruct_experience_object → {len(companies)} company/role(s), years_range='{years_range}', has_experience={len(companies) > 0}")
        return {
            "companies": companies,
            "roles": [],
            "years_range": years_range,
            "has_experience": len(companies) > 0
        }