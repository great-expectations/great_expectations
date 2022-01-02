import pytest

from .fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
    ExpectColumnValuesToEqualThree__BrokenIteration,
)

@pytest.mark.skip("This is broken because Expectation._get_execution_engine_diagnostics is broken")
def test_print_diagnostic_checklist__first_iteration():
    output_message = ExpectColumnValuesToEqualThree().print_diagnostic_checklist()

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
   library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Core logic exists and passes tests on at least one Execution Engine
"""
    )


def test_print_diagnostic_checklist__second_iteration():
    output_message = (
        ExpectColumnValuesToEqualThree__SecondIteration().print_diagnostic_checklist()
    )
    print(output_message)

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree__SecondIteration:
 ✔ library_metadata object exists
 ✔ Has a docstring, including a one-line short description
    ✔ "Expect values in this column to equal the number three."
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""
    )


def test_print_diagnostic_checklist__third_iteration():
    output_message = (
        ExpectColumnValuesToEqualThree__ThirdIteration().print_diagnostic_checklist()
    )
    print(output_message)

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree__ThirdIteration:
 ✔ library_metadata object exists
   Has a docstring, including a one-line short description
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""
    )
