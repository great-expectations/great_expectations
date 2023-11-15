from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )


class ValidationStatistics(NamedTuple):
    evaluated_expectations: int
    successful_expectations: int
    unsuccessful_expectations: int
    success_percent: float | None
    success: bool


def calc_validation_statistics(
    validation_results: list[ExpectationValidationResult],
) -> ValidationStatistics:
    """
    Calculate summary statistics for the validation results and
    return ``ExpectationStatistics``.
    """
    # calc stats
    evaluated_expectations = len(validation_results)
    successful_expectations = len([exp for exp in validation_results if exp.success])
    unsuccessful_expectations = evaluated_expectations - successful_expectations
    success = successful_expectations == evaluated_expectations
    try:
        success_percent = successful_expectations / evaluated_expectations * 100
    except ZeroDivisionError:
        success_percent = None

    return ValidationStatistics(
        successful_expectations=successful_expectations,
        evaluated_expectations=evaluated_expectations,
        unsuccessful_expectations=unsuccessful_expectations,
        success=success,
        success_percent=success_percent,
    )
