import pytest
import json

from great_expectations.types import (
    Expectation,
    ExpectationSuite,
    ValidationResult,
    ValidationResultSuite,
)


def test_expectation_suite(titanic_profiled_expectations_1):
    suite = ExpectationSuite(
        coerce_types=True,
        **titanic_profiled_expectations_1
    )


def test_validation_result_suite(titanic_validation_results):
    # print(json.dumps(titanic_validation_results, indent=2))
    # titanic_validation_results["results"] = titanic_validation_results["results"][:2]
    suite = ValidationResultSuite(
        coerce_types=True,
        **titanic_validation_results
    )
    # print(json.dumps(suite, indent=2))
    # assert False
