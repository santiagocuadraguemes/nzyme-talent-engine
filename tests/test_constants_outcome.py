"""
VT-1 (parte): mapas de enrutado de outcome (RF-15).
Valida la correspondencia entre el valor del formulario de desenlace, el prefijo
de título de la página confidencial y el valor del campo Assessment.
"""
from core.constants import (
    OUTCOME_DISCARDED, OUTCOME_DISQUALIFIED, OUTCOME_LOST,
    OUTCOME_TITLE_PREFIX, OUTCOME_ASSESSMENT_VALUE,
)


def test_title_prefix_mapping():
    assert OUTCOME_TITLE_PREFIX[OUTCOME_DISCARDED] == "Discarded"
    assert OUTCOME_TITLE_PREFIX[OUTCOME_DISQUALIFIED] == "Disqualified"
    assert OUTCOME_TITLE_PREFIX[OUTCOME_LOST] == "Lost"


def test_assessment_value_mapping():
    # Solo el descarte completo tiene opción de Assessment; el resto se deja sin asignar.
    assert OUTCOME_ASSESSMENT_VALUE[OUTCOME_DISCARDED] == "4. Discarded"
    assert OUTCOME_ASSESSMENT_VALUE[OUTCOME_DISQUALIFIED] is None
    assert OUTCOME_ASSESSMENT_VALUE[OUTCOME_LOST] is None


def test_both_maps_cover_the_same_three_outcomes():
    keys = {OUTCOME_DISCARDED, OUTCOME_DISQUALIFIED, OUTCOME_LOST}
    assert set(OUTCOME_TITLE_PREFIX) == keys
    assert set(OUTCOME_ASSESSMENT_VALUE) == keys
