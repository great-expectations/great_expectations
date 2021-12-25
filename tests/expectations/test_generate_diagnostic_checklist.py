from .expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)

def test_generate_diagnostic_checklist__first_iteration():
    ExpectColumnValuesToEqualThree().generate_diagnostic_checklist()

    assert False

def test_generate_diagnostic_checklist__second_iteration():
    ExpectColumnValuesToEqualThree__SecondIteration().generate_diagnostic_checklist()

    assert False

def test_generate_diagnostic_checklist__third_iteration():
    ExpectColumnValuesToEqualThree__ThirdIteration().generate_diagnostic_checklist()

    assert False
