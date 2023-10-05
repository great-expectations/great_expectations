from great_expectations.expectations.expectation import (
    BatchExpectation,
)


class ExpectColumnBootstrappedKsTestPValueToBeGreaterThan(BatchExpectation):
    # This expectation is a stub - it needs migration to the modular expectation API

    # This dictionary contains metadata for display in the public gallery
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
    default_kwarg_values = {}
    args_keys = (
        "column",
        "distribution",
        "p_value",
        "params",
    )
