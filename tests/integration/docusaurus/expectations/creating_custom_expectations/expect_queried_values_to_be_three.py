from typing import Dict, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import ExpectationValidationResult
from great_expectations.expectations.query_based_expectation import (
    QueryBasedExpectation,
    QueryMetricProvider,
)


class ExpectQueriedValuesToBeThree(QueryBasedExpectation):
    """TO-DO"""

    query_camel_name = "ValuesEqualThree"
    query_ = """
    SELECT CASE WHEN EXISTS (
    SELECT *
    FROM [data]
    WHERE threes = 3
    )
    THEN CAST(1 AS BIT)
    ELSE CAST(0 AS BIT) END
"""

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        breakpoint()

    examples = [
        {
            "data": {
                "threes": [3, 3, 3, 3, 3],
            },
            "tests": [
                {
                    "title": "positive_test",
                    "exact_match_out": False,
                    "in": {"column": "threes"},
                    "out": {
                        "success": True,
                    },
                    "include_in_gallery": True,
                },
            ],
            "only_for": ["postgresql", "pandas"],
        }
    ]

    metric = QueryBasedExpectation.register_metric(
        query_camel_name=query_camel_name,
        query_=query_,
    )

    library_metadata = {
        "tags": ["query"],
        "contributors": ["@joegargery"],
    }


# </snippet>
if __name__ == "__main__":
    ExpectQueriedValuesToBeThree().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectQueriedValuesToBeThree().run_diagnostics()
breakpoint()
for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    assert check["passed"] is True
