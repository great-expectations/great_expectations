from functools import partial
from typing import Optional

import schwifty
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


def bic_belong_to_country(bic: str, country_code: str) -> bool:
    try:
        bic = schwifty.BIC(bic)
        if bic.country_code.upper() == country_code.upper():
            return True
        else:
            return False
    except SchwiftyException:
        return False


class ColumnValuesBicBelongToCountry(ColumnMapMetricProvider):
    condition_metric_name = "column_values.bic_belong_to_country"
    condition_value_keys = ("country_code",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, country_code, **kwargs):
        return column.apply(partial(bic_belong_to_country, country_code=country_code))

    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, country_code, **kwargs):
    #     @F.udf(pyspark.types.BooleanType())
    #     def bic_belong_to_country_udf(bic: str) -> bool:
    #         return bic_belong_to_country(bic, country_code)

    #     return bic_belong_to_country_udf(column)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError


class ExpectColumnValuesBicBelongToCountry(ColumnMapExpectation):
    """Expect the provided BIC (Business Identifier Code)
    in the country which code (alpha-2) passed in the parameters."""

    map_metric = "column_values.bic_belong_to_country"

    success_keys = (
        "mostly",
        "country_code",
    )

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
                "all_hu": [
                    "OTPVHUHB",
                    "GIBAHUHB",
                    "OKHBHUHB",
                    "GNBAHUHB",
                    "UBRTHUHB",
                ],
                "some_other": [
                    "OTPPPUHB",
                    "GIBAHUHB",
                    "OKHBHUHB",
                    "BOFAUS6S",
                    "BKCHCNBJ",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_hu",
                        "country_code": "hu",
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "some_other",
                        "country_code": "hu",
                        "mostly": 0.9,
                    },
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
    ExpectColumnValuesBicBelongToCountry().print_diagnostic_checklist()
