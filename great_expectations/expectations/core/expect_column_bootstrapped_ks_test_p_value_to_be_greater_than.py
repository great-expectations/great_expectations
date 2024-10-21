from __future__ import annotations

# NOTE: This Expectation is incomplete and not ready for use.
#       It should remain unexported until it meets the requirements set by our V1 API.
from great_expectations._docs_decorators import public_api
from great_expectations.expectations.expectation import (
    BatchExpectation,
)


@public_api
class ExpectColumnBootstrappedKsTestPValueToBeGreaterThan(BatchExpectation):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
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
        "distribution",
        "p_value",
        "params",
    )
