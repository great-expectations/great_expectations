from typing import Optional

from schwifty import IBAN
from schwifty.exceptions import SchwiftyException

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def is_valid_iban(iban: str) -> bool:
    try:
        IBAN(iban)
        return True
    except SchwiftyException:
        return False


@F.udf(pyspark.types.BooleanType())
def is_valid_iban_udf(iban: str) -> bool:
    return is_valid_iban(iban)


class ColumnValuesToBeValidIban(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_iban"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.apply(is_valid_iban)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return is_valid_iban_udf(column)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError


class ExpectColumnValuesToBeValidIban(ColumnMapExpectation):
    """Expect column values to be valid IBAN format."""

    map_metric = "column_values.valid_iban"

    success_keys = ("mostly",)

    default_kwarg_values = {}

    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "hackathon", "typed-entities"],
        "contributors": ["@voidforall", "@mkopec87"],
        "requirements": ["schwifty"],
    }

    examples = [
        {
            "data": {
                "well_formed_iban": [
                    "DE89 3704 0044 0532 0130 00",
                    "FR14 2004 1010 0505 0001 3M02 606",
                    "IT60 X054 2811 1010 0000 0123 456",
                    "CH93 0076 2011 6238 5295 7",
                    "GB29 NWBK 6016 1331 9268 19",
                ],
                "malformed_iban": [
                    # invalid length
                    "DE89 3704 0044 0532 0130",
                    "DE89 3704 0044 0532 0130 0000",
                    # wrong country code
                    "XX89 3704 0044 0532 0130 00",
                    # wrong format
                    "DE89 AA04 0044 0532 0130 00",
                    # wrong checksum digits
                    "GB01 BARC 2071 4583 6083 87",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "well_formed_iban"},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "malformed_iban"},
                    "out": {"success": False},
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
    ExpectColumnValuesToBeValidIban().print_diagnostic_checklist()
