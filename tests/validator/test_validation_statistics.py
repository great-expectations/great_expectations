import pytest

from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.validator.validation_statistics import (
    ValidationStatistics,
    calc_validation_statistics,
)


@pytest.mark.unit
def test_stats_no_expectations():
    expectation_results = []
    actual = calc_validation_statistics(expectation_results)

    # pay attention to these two
    assert None is actual.success_percent
    assert True is actual.success
    # the rest is boring
    assert actual.successful_expectations == 0
    assert actual.evaluated_expectations == 0
    assert actual.unsuccessful_expectations == 0


@pytest.mark.unit
def test_stats_no_successful_expectations():
    expectation_results = [ExpectationValidationResult(success=False)]
    actual = calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(1, 0, 1, 0.0, False)
    assert expected == actual

    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=False),
    ]
    actual = calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(3, 0, 3, 0.0, False)
    assert expected == actual


@pytest.mark.unit
def test_stats_all_successful_expectations():
    expectation_results = [
        ExpectationValidationResult(success=True),
    ]
    actual = calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(1, 1, 0, 100.0, True)
    assert expected == actual

    expectation_results = [
        ExpectationValidationResult(success=True),
        ExpectationValidationResult(success=True),
        ExpectationValidationResult(success=True),
    ]
    actual = calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(3, 3, 0, 100.0, True)
    assert expected == actual


@pytest.mark.unit
def test_stats_mixed_expectations():
    expectation_results = [
        ExpectationValidationResult(success=False),
        ExpectationValidationResult(success=True),
    ]
    actual = calc_validation_statistics(expectation_results)
    expected = ValidationStatistics(2, 1, 1, 50.0, False)
    assert expected == actual
