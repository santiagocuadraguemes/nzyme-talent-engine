import re
from datetime import date
from core.domain_mapper import DomainMapper
from core.logger import get_logger
from core.constants import (
    PROP_NAME, PROP_PHONE, PROP_LINKEDIN, PROP_EMAIL, PROP_CV_FILES,
    PROP_DATE_ADDED, PROP_PROCESS_HISTORY, PROP_TEAM_ROLE, PROP_SOURCE,
    PROP_EXP_TOTAL_YEARS, PROP_EXP_CONSULTING, PROP_EXP_AUDIT, PROP_EXP_IB,
    PROP_EXP_PE, PROP_EXP_VC, PROP_EXP_ENGINEER, PROP_EXP_LAWYER,
    PROP_EXP_FOUNDER, PROP_EXP_CORP_MA, PROP_EXP_PORTCO,
    PROP_EXP_MANAGEMENT, PROP_EXP_FINANCE, PROP_EXP_MARKETING,
    PROP_EXP_OPERATIONS, PROP_EXP_PRODUCT, PROP_EXP_SALES_REVENUE,
    PROP_EXP_TECHNOLOGY, PROP_EXP_INTERNATIONAL, PROP_EXP_INDUSTRIES,
    PROP_LANGUAGES, PROP_EDU_BACHELORS, PROP_EDU_MASTERS,
    PROP_EDU_UNIVERSITIES, PROP_EDU_MBAS,
    PROP_GOVERNANCE_ACCESS,
)

logger = get_logger("NotionBuilder")

# Default values
VAL_NO = "No"

# Canonical company name mappings for deduplication
CANONICAL_COMPANY_NAMES = {
    "mckinsey & company": "McKinsey",
    "mckinsey and company": "McKinsey",
    "mckinsey&company": "McKinsey",
    "bain & company": "Bain",
    "bain &company": "Bain",
    "bain and company": "Bain",
    "boston consulting group": "BCG",
    "the boston consulting group": "BCG",
    "deloitte touche tohmatsu": "Deloitte",
    "deloitte & touche": "Deloitte",
    "pricewaterhousecoopers": "PwC",
    "ernst & young": "EY",
    "ernst and young": "EY",
    "goldman sachs group": "Goldman Sachs",
    "jp morgan": "JPMorgan",
    "j.p. morgan": "JPMorgan",
    "jpmorgan chase": "JPMorgan",
    "j.p. morgan chase": "JPMorgan",
    "morgan stanley & co": "Morgan Stanley",
    "bank of america merrill lynch": "Bank of America",
    "merrill lynch": "Bank of America",
    "lazard frères": "Lazard",
    "n m rothschild": "Rothschild",
    "rothschild & co": "Rothschild",
}

def _normalize_company_name(raw_name):
    """Normalize a company name: canonical lookup, legal suffix removal, title case."""
    clean = re.sub(r'\s+', ' ', raw_name.strip())  # collapse multiple spaces
    if not clean:
        return ""
    # 1. Canonical lookup (case-insensitive)
    lookup = clean.lower()
    if lookup in CANONICAL_COMPANY_NAMES:
        canonical = CANONICAL_COMPANY_NAMES[lookup]
        logger.debug(f"_normalize_company_name canonical hit: '{clean}' → '{canonical}'")
        return canonical
    # 2. Remove legal suffixes at end of string
    clean = re.sub(r',?\s*(Ltd\.?|Inc\.?|S\.?A\.?|GmbH|SL|LLC|LLP|PLC|Corp\.?|Group|Co\.?)$', '', clean, flags=re.IGNORECASE).strip()
    # 3. Re-check canonical after suffix removal
    lookup = clean.lower()
    if lookup in CANONICAL_COMPANY_NAMES:
        canonical = CANONICAL_COMPANY_NAMES[lookup]
        logger.debug(f"_normalize_company_name canonical hit (post-suffix): '{clean}' → '{canonical}'")
        return canonical
    return clean

class NotionBuilder:
    
    # --- FORMAT HELPERS (Private) ---

    @staticmethod
    def _format_multi_select(items_list):
        """Converts list of strings to Notion tags with default color."""
        if not items_list: return []
        unique_items = []
        seen = set()
        for i in items_list:
            if i:
                clean = str(i)[:100].strip().replace(",", "")
                if clean and clean not in seen:
                    unique_items.append({"name": clean, "color": "default"})
                    seen.add(clean)
        return unique_items

    @staticmethod
    def _create_experience_tags(sector_data):
        """
        Builds Notion multi-select tags for SECTOR-based experience fields.
        Uses company names (normalized + deduplicated) plus a years-range tag.
        """
        tags = []
        if not sector_data or not sector_data.get("has_experience"):
            return [{"name": VAL_NO, "color": "default"}]

        # 1. Extract and normalize company names with deduplication
        seen = set()
        companies = sector_data.get("companies", [])
        for c in companies:
            if c:
                normalized = _normalize_company_name(str(c)[:100].replace(",", ""))
                if normalized and normalized.lower() not in seen:
                    tags.append({"name": normalized, "color": "default"})
                    seen.add(normalized.lower())

        # 2. Add years range tag
        years = sector_data.get("years", 0)
        range_tag = DomainMapper.get_years_range_tag(years)
        if range_tag and range_tag != VAL_NO:
            tags.append({"name": range_tag, "color": "default"})

        if not tags:
            return [{"name": VAL_NO, "color": "default"}]
        logger.debug(f"_create_experience_tags → {len(tags)} tag(s) ({len(companies)} unique company name(s) + years tag)")
        return tags

    @staticmethod
    def _create_functional_tags(sector_data):
        """
        Builds Notion multi-select tags for FUNCTIONAL experience fields.
        Uses role titles (deduplicated) plus a years-range tag.
        """
        tags = []
        if not sector_data or not sector_data.get("has_experience"):
            return [{"name": VAL_NO, "color": "default"}]

        # 1. Extract role titles with deduplication
        seen = set()
        roles = sector_data.get("roles", [])
        for r in roles:
            if r:
                clean = str(r)[:100].strip().replace(",", "")
                if clean and clean.lower() not in seen:
                    tags.append({"name": clean, "color": "default"})
                    seen.add(clean.lower())

        # 2. Add years range tag
        years = sector_data.get("years", 0)
        range_tag = DomainMapper.get_years_range_tag(years)
        if range_tag and range_tag != VAL_NO:
            tags.append({"name": range_tag, "color": "default"})

        if not tags:
            return [{"name": VAL_NO, "color": "default"}]
        logger.debug(f"_create_functional_tags → {len(tags)} tag(s) ({len(roles)} unique role(s) + years tag)")
        return tags

    # --- MAIN BUILD METHOD ---

    @staticmethod
    def build_candidate_payload(candidate_data, public_cv_url, process_name, existing_history=None, process_type=None, existing_team_role=None, source=None, governance_entries=None, skip_process_history=False):
        """
        Builds the request body for Creating/Updating a page in Notion.
        """
        exp = candidate_data.get("experience", {})
        edu = candidate_data.get("education", {})
        gen = candidate_data.get("general", {})
        
        # 1. Process History Management (skip for confidential processes)
        history_list = existing_history if existing_history else []
        if process_name and process_name not in history_list and not skip_process_history:
            history_list.append(process_name)
        history_tags = [{"name": p, "color": "default"} for p in history_list[-100:]]

        # 2. Proposed Nzyme Team & Role Management
        team_role_list = existing_team_role if existing_team_role else []
        if process_type and process_type not in team_role_list:
            team_role_list.append(process_type)
        team_role_tags = [{"name": t, "color": "default"} for t in team_role_list]

        props = {
            PROP_NAME: {"title": [{"text": {"content": candidate_data.get("name", "Unnamed")[:200]}}]},
            PROP_PHONE: {"phone_number": candidate_data.get("phone")},
            PROP_LINKEDIN: {"url": candidate_data.get("linkedin_url")},

            # Audit
            PROP_DATE_ADDED: {"date": {"start": date.today().isoformat()}},

            # Classification
            PROP_PROCESS_HISTORY: {"multi_select": history_tags},
            PROP_TEAM_ROLE: {"multi_select": team_role_tags}
        }

        # CV file - only set if we have a URL
        if public_cv_url:
            props[PROP_CV_FILES] = {"files": [{"name": "CV.pdf", "external": {"url": public_cv_url}}]}
        
        # Email
        if candidate_data.get("email"):
            props[PROP_EMAIL] = {"email": candidate_data.get("email")}

        # Source (only set for new candidates)
        if source:
            props[PROP_SOURCE] = {"multi_select": [{"name": source, "color": "default"}]}

        # Governance (people property for page-level access control)
        # governance_entries: list of {"object": "user"/"group", "id": "..."} dicts
        if governance_entries is not None:
            props[PROP_GOVERNANCE_ACCESS] = {"people": governance_entries}

        # Total Years Range
        rango_total = DomainMapper.get_years_range_tag(candidate_data.get("total_years", 0))
        if rango_total:
            props[PROP_EXP_TOTAL_YEARS] = {"select": {"name": rango_total, "color": "default"}}

        # Sector-based fields (use company names as tags)
        company_sector_mapping = {
            PROP_EXP_CONSULTING: exp.get("consulting"),
            PROP_EXP_AUDIT: exp.get("audit"),
            PROP_EXP_IB: exp.get("ib") or exp.get("investment_banking"),
            PROP_EXP_PE: exp.get("pe") or exp.get("private_equity"),
            PROP_EXP_VC: exp.get("vc") or exp.get("venture_capital"),
            PROP_EXP_ENGINEER: exp.get("engineer_role"),
            PROP_EXP_LAWYER: exp.get("lawyer"),
            PROP_EXP_FOUNDER: exp.get("founder"),
            PROP_EXP_CORP_MA: exp.get("corp_ma"),
            PROP_EXP_PORTCO: exp.get("portco_roles") or exp.get("portco"),
        }

        for prop_name, data in company_sector_mapping.items():
            props[prop_name] = {"multi_select": NotionBuilder._create_experience_tags(data)}

        # Functional fields (use role titles as tags)
        functional_sector_mapping = {
            PROP_EXP_MANAGEMENT: exp.get("management"),
            PROP_EXP_FINANCE: exp.get("finance"),
            PROP_EXP_MARKETING: exp.get("marketing"),
            PROP_EXP_OPERATIONS: exp.get("operations"),
            PROP_EXP_PRODUCT: exp.get("product"),
            PROP_EXP_SALES_REVENUE: exp.get("sales_revenue"),
            PROP_EXP_TECHNOLOGY: exp.get("technology"),
        }

        for prop_name, data in functional_sector_mapping.items():
            props[prop_name] = {"multi_select": NotionBuilder._create_functional_tags(data)}

        # Simple Lists
        if gen.get("international_locations"):
            props[PROP_EXP_INTERNATIONAL] = {"multi_select": NotionBuilder._format_multi_select(gen.get("international_locations"))}
        
        if gen.get("industries_specialized"):
            props[PROP_EXP_INDUSTRIES] = {"multi_select": NotionBuilder._format_multi_select(gen.get("industries_specialized"))}
        
        if candidate_data.get("languages"):
            props[PROP_LANGUAGES] = {"multi_select": NotionBuilder._format_multi_select(candidate_data.get("languages"))}

        # Education
        if edu.get("bachelors"):
            props[PROP_EDU_BACHELORS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("bachelors"))}
        
        if edu.get("masters"):
            props[PROP_EDU_MASTERS] = {"multi_select": NotionBuilder._format_multi_select(edu.get("masters"))}
        
        if edu.get("university"):
            props[PROP_EDU_UNIVERSITIES] = {"multi_select": NotionBuilder._format_multi_select(edu.get("university"))}
        
        # MBA (handles both list and string)
        mba_val = edu.get("mba")
        mba_list = []
        if isinstance(mba_val, list): mba_list = mba_val
        elif isinstance(mba_val, str) and mba_val != "No": mba_list = [mba_val]
        
        if mba_list:
            props[PROP_EDU_MBAS] = {"multi_select": NotionBuilder._format_multi_select(mba_list)}

        logger.debug(f"build_candidate_payload → {len(props)} property key(s): {list(props.keys())}")
        return props