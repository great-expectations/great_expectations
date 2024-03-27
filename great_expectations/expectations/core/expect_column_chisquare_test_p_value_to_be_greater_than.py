from __future__ import annotations

from great_expectations.expectations.expectation import (
    BatchExpectation,
)


# NOTE: This Expectation is incomplete and not ready for use.
#       It should remain unexported until it meets the requirements set by our V1 API.
class ExpectColumnChiSquareTestPValueToBeGreaterThan(BatchExpectation):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    library_metadata = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "column aggregate expectation",
            "needs migration to modular expectations api",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    metric_dependencies = tuple()
    success_keys = ()
    args_keys = (
        "column",
        "partition_object",
        "p",
        "tail_weight_holdout",
    )
