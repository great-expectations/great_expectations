from typing import Optional

from schwifty import BIC
from schwifty.exceptions import SchwiftyException

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
)

# SparkDFExecutionEngine,
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

# from great_expectations.compatibility.pyspark import functions as F
# from great_expectations.compatibility import pyspark


def is_valid_bic(bic_code: str) -> bool:
    try:
        BIC(bic_code)
        return True
    except SchwiftyException:
        return False


# @F.udf(pyspark.types.BooleanType())
# def is_valid_bic_udf(bic: str) -> bool:
#     return is_valid_bic(bic)


class ColumnValuesToBeValidBic(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_bic"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(is_valid_bic)

    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     return is_valid_bic_udf(column)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError


class ExpectColumnValuesToBeValidBic(ColumnMapExpectation):
    """Expect column values to be valid BIC (Business Identifier Code)."""

    map_metric = "column_values.valid_bic"

    success_keys = ("mostly",)

    default_kwarg_values = {}

    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "hackathon-22",
            "experimental",
            "typed-entities",
        ],
        "contributors": ["@szecsip", "@mkopec87"],
        "requirements": ["schwifty"],
    }

    examples = [
        {
            "data": {
                "all_valid": [
                    "GENODEM1GLS",
                    "BOHIUS77",
                    "OTPVHUHB",
                    "CAXBMNUB",
                    "SVBMMNUB",
                ],
                "some_other": [
                    "GENODEM1GLS",
                    "BOHIUS77",
                    "OTPVHUHB",
                    "CAXBMNUB",
                    "SVBXXXXX",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_valid"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "some_other", "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)


if __name__ == "__main__":
    ExpectColumnValuesToBeValidBic().print_diagnostic_checklist()
