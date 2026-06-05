"""
VT-1 (parte): esquema cerrado de extracción con IA (RF-07, RF-12).
Valida que los modelos Pydantic aceptan datos conformes y rechazan los que no
lo son (puntuaciones fuera del enum, campos obligatorios ausentes, tipos inválidos).

Se omite si openai/logfire no están instalados (los importa core.ai_parser).
"""
import pytest

pytest.importorskip("openai")
pytest.importorskip("logfire")

from pydantic import ValidationError
from core.ai_parser import (
    SectorExperience, AssessmentItem, StrategicScore, FeedbackAssessmentItem,
)


def test_sector_experience_valid():
    s = SectorExperience(has_experience=True, years=5.0, companies=["JPMorgan"])
    assert s.years == 5.0
    assert s.companies == ["JPMorgan"]


def test_sector_experience_missing_required_field():
    with pytest.raises(ValidationError):
        SectorExperience(has_experience=True, companies=[])  # falta 'years'


def test_sector_experience_invalid_year_type():
    with pytest.raises(ValidationError):
        SectorExperience(has_experience=True, years="not-a-number", companies=[])


def test_strategic_score_enum_membership():
    assert StrategicScore("High") == StrategicScore.HIGH
    assert {s.value for s in StrategicScore} == {"High", "Medium", "Low", "No"}


def test_assessment_item_valid_score():
    item = AssessmentItem(characteristic="Leadership", score="High", comment="strong track record")
    assert item.score == StrategicScore.HIGH


def test_assessment_item_rejects_invalid_score():
    with pytest.raises(ValidationError):
        AssessmentItem(characteristic="Leadership", score="Maybe", comment="x")


def test_feedback_assessment_item_valid():
    item = FeedbackAssessmentItem(
        characteristic="Proactive",
        score="Medium",
        cv_evidence="Led two turnarounds",
        feedback_evidence="Interviewer noted initiative",
    )
    assert item.score == StrategicScore.MEDIUM
