from typing import Tuple

from moneyed import list_all_currencies

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.pyspark import types
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def generate_all_currency_codes() -> Tuple[str]:
    return [str(currency_code) for currency_code in list_all_currencies()]


def is_valid_currency_code(code: str, currency_codes: Tuple[str]) -> bool:
    return code in currency_codes


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesCurrencyCode(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.currency_code"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        currency_codes: Tuple[str] = generate_all_currency_codes()

        return column.apply(lambda x: is_valid_currency_code(x, currency_codes))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        currency_codes: Tuple[str] = generate_all_currency_codes()

        # Register the UDF
        is_valid_currency_code_udf = F.udf(is_valid_currency_code, types.BooleanType())

        # Apply the UDF to the column
        result_column = F.when(
            is_valid_currency_code_udf(column, F.lit(currency_codes)), True
        ).otherwise(False)
        return result_column


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidCurrencyCode(ColumnMapExpectation):
    """Expect values in this column to be valid currency codes (three capital letters).

    See ISO-4217 for more information.
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "valid_currency_codes": ["USD", "EUR", "BEL", "HKD", "GBP"],
                "invalid_currency_codes": [
                    "us dollars",
                    "pounds",
                    "Renminbi",
                    "$",
                    "EURO",
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_currency_codes",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_currency_codes"},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_currency_codes",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid_currency_codes"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.currency_code"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ["hackathon", "currency", "type-entities", "semantic-types"],
        "contributors": [
            "@lucasasmith" "@calvingdu",
        ],
        "requirements": ["py-moneyed"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidCurrencyCode().print_diagnostic_checklist()
