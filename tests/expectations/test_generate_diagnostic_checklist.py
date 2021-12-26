import json

from .expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
)

def test__convert_checks_into_output_message():
    checks = [{
        "message": "AAA",
        "passed": True,
    },{
        "message": "BBB",
        "passed": False,
    },{
        "message": "CCC",
        "passed": False,
        "sub_messages": [{
            "message": "ddd",
            "passed": True,
        },{
            "message": "eee",
            "passed": False,
        }]
    },]

    assert ExpectColumnValuesToEqualThree()._convert_checks_into_output_message(checks) == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
 ✔ AAA
   BBB
   CCC
    ✔ ddd
      eee
"""

def test__count_positive_and_negative_example_cases():
    assert ExpectColumnValuesToEqualThree()._count_positive_and_negative_example_cases() == (0,0)
    assert ExpectColumnValuesToEqualThree__SecondIteration()._count_positive_and_negative_example_cases() == (1,2)

def test_generate_diagnostic_checklist__first_iteration():
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
 ✔ Has a docstring, including a one-line short description
    ✔ "Expect values in this column to equal the number three."
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""

def test_generate_diagnostic_checklist__third_iteration():
    output_message = ExpectColumnValuesToEqualThree__ThirdIteration().generate_diagnostic_checklist()
    print(output_message)

    assert output_message == """\
Completeness checklist for ExpectColumnValuesToEqualThree__ThirdIteration:
 ✔ library_metadata object exists
   Has a docstring, including a one-line short description
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""
