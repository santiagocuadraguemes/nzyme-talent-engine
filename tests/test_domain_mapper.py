"""
VT-1 (parte): mapeo de dominio determinista.
Valida la traducción de años a rango (RF-07), el formateo de experiencia
y la construcción del payload de Supabase.
"""
import pytest

from core.domain_mapper import DomainMapper


@pytest.mark.parametrize("years,expected", [
    (None, "No"),
    (0, "No"),
    (-4, "No"),
    (0.5, "0-3 Years"),
    (2.99, "0-3 Years"),
    (3, "3-5 Years"),
    (4.99, "3-5 Years"),
    (5, "5-7 Years"),
    (6.99, "5-7 Years"),
    (7, "7-10 Years"),
    (9.99, "7-10 Years"),
    (10, "10-15 Years"),
    (14.99, "10-15 Years"),
    (15, "15+ Years"),
    (40, "15+ Years"),
])
def test_year_range_boundaries(years, expected):
    assert DomainMapper.get_years_range_tag(years) == expected


def test_format_experience_empty_returns_default():
    out = DomainMapper._format_experience(None)
    assert out == {"companies": [], "roles": [], "years_range": "No", "has_experience": False}


def test_format_experience_maps_years_to_range():
    out = DomainMapper._format_experience(
        {"years": 6, "companies": ["McKinsey"], "roles": ["Partner"], "has_experience": True}
    )
    assert out["years_range"] == "5-7 Years"
    assert out["companies"] == ["McKinsey"]
    assert out["roles"] == ["Partner"]
    assert out["has_experience"] is True


def test_reconstruct_experience_object_splits_years_and_companies():
    out = DomainMapper.reconstruct_experience_object(["McKinsey", "5-7 Years"])
    assert out["companies"] == ["McKinsey"]
    assert out["years_range"] == "5-7 Years"
    assert out["has_experience"] is True


def test_reconstruct_experience_object_empty_is_none():
    assert DomainMapper.reconstruct_experience_object([]) is None


def test_map_to_supabase_candidate_structure_and_source():
    ai_data = {
        "name": "Ada Lovelace",
        "email": "ada@example.com",
        "phone": "+34 600 000 000",
        "linkedin_url": "https://linkedin.com/in/ada",
        "total_years": 8,
        "languages": ["English"],
        "education": {},
        "general": {},
        "experience": {"investment_banking": {"years": 6, "companies": ["JPMorgan"], "has_experience": True}},
    }
    row = DomainMapper.map_to_supabase_candidate(ai_data, "https://storage/cv.pdf", source="Applied via LinkedIn")

    # Columnas SQL
    assert row["name"] == "Ada Lovelace"
    assert row["cv_url"] == "https://storage/cv.pdf"
    assert row["source"] == "Applied via LinkedIn"
    assert row["assessment"] is None

    # JSON
    data = row["candidate_data"]
    assert data["total_years_range"] == "7-10 Years"
    # El alias investment_banking se mapea al sector ib
    assert data["experience"]["ib"]["years_range"] == "5-7 Years"
    assert data["experience"]["ib"]["companies"] == ["JPMorgan"]
