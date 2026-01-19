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
        if not y or y <= 0: return None
        if y < 3: return "0-3 Years"
        if y < 5: return "3-5 Years"
        if y < 7: return "5-7 Years"
        if y < 10: return "7-10 Years"
        if y < 15: return "10-15 Years"
        return "15+ Years"

    @staticmethod
    def map_to_supabase_candidate(candidate_data, public_cv_url):
        """Prepara el diccionario para insertar en la tabla SQL de Supabase."""
        return {
            "name": candidate_data["name"],
            "email": candidate_data.get("email"),
            "linkedin_url": candidate_data.get("linkedin_url"),
            "cv_url": public_cv_url,
            "candidate_data": candidate_data, 
            "updated_at": "now()"
        }

    @staticmethod
    def reconstruct_experience_object(tag_list):
        """Reconstruye el objeto de experiencia desde tags planos (Lectura)."""
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