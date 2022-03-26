import pytest

from .fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__BrokenIteration,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)


@pytest.mark.skip(
    "This is broken because Expectation._get_execution_engine_diagnostics is broken"
)
def test_print_diagnostic_checklist__first_iteration():
    output_message = ExpectColumnValuesToEqualThree().print_diagnostic_checklist()

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
   library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Has core logic and passes tests on at least one Execution Engine
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
 ✔ Has a valid library_metadata object
 ✔ Has a docstring, including a one-line short description
    ✔ "Expect values in this column to equal the number three."
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Has core logic and passes tests on at least one Execution Engine
    ✔ All 3 tests for pandas are passing
   Passes all linting checks
      The snake_case of ExpectColumnValuesToEqualThree__SecondIteration (expect_column_values_to_equal_three___second_iteration) does not match filename part (expect_column_values_to_equal_three)
   Has basic input validation and type checking
      No validate_configuration method defined on subclass
 ✔ Has both statement Renderers: prescriptive and diagnostic
 ✔ Has core logic that passes tests for all applicable Execution Engines and SQL dialects
    ✔ All 3 tests for pandas are passing
   Has a full suite of tests, as determined by a code owner
   Has passed a manual review by a code owner for code standards and style guides
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
 ✔ Has a valid library_metadata object
   Has a docstring, including a one-line short description
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Has core logic and passes tests on at least one Execution Engine
    ✔ All 3 tests for pandas are passing
   Passes all linting checks
      The snake_case of ExpectColumnValuesToEqualThree__ThirdIteration (expect_column_values_to_equal_three___third_iteration) does not match filename part (expect_column_values_to_equal_three)
   Has basic input validation and type checking
      No validate_configuration method defined on subclass
   Has both statement Renderers: prescriptive and diagnostic
 ✔ Has core logic that passes tests for all applicable Execution Engines and SQL dialects
    ✔ All 3 tests for pandas are passing
   Has a full suite of tests, as determined by a code owner
   Has passed a manual review by a code owner for code standards and style guides
"""
    )
