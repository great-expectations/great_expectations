import json
from .fixtures.expect_column_values_to_only_contain_vowels import (
    ExpectColumnValuesToOnlyContainVowels,
)


def test_expectation_self_check():

    my_expectation = ExpectColumnValuesToOnlyContainVowels()
    expectation_diagnostic = my_expectation.run_diagnostics()
    print(json.dumps(expectation_diagnostic.to_dict(), indent=2))

    for test in expectation_diagnostic.tests:
        assert test.test_passed