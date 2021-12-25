from .expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)

def test_generate_diagnostic_checklist__first_iteration(capsys):
    output_message = ExpectColumnValuesToEqualThree().generate_diagnostic_checklist()
    print(output_message)

    assert output_message == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
   library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Core logic exists and passes tests on at least one Execution Engine
"""

def test_generate_diagnostic_checklist__second_iteration():
    output_message = ExpectColumnValuesToEqualThree__SecondIteration().generate_diagnostic_checklist()
    print(output_message)

    assert output_message == """\
Completeness checklist for ExpectColumnValuesToEqualThree__SecondIteration:
 ✔ library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Core logic exists and passes tests on at least one Execution Engine
"""

def test_generate_diagnostic_checklist__third_iteration():
    output_message = ExpectColumnValuesToEqualThree__ThirdIteration().generate_diagnostic_checklist()
    print(output_message)

    assert output_message == """\
Completeness checklist for ExpectColumnValuesToEqualThree__ThirdIteration:
 ✔ library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Core logic exists and passes tests on at least one Execution Engine
"""
