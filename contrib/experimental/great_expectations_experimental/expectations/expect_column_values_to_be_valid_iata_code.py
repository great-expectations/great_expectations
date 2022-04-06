import json
from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.render.renderer.renderer import renderer

from pyairports.airports import Airports, AirportNotFoundException


def is_valid_iata_code(code: str) -> bool:
    airports = Airports()
    try:
        airports.lookup(code)
        return True
    except AirportNotFoundException:
        return False
    except Exception:
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesValidIATACode(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_iata_code"
    condition_value_keys = ("min_length",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_length, **kwargs):
        return column.apply(lambda x: is_valid_iata_code(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidIATACode(ColumnMapExpectation):
    """This Expectation validates data as conforming to the 3-letter IATA Airport Code"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "valid_iata_codes": ["BOS", "JFK", "EWR", "PVD", "BLR"],
                "invalid_iata_codes": [
                    "",
                    "AEM",
                    "AME",
                    "AMEIATATHISISFALSY",
                    "BLU",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_iata_codes"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid_iata_codes", "mostly": 0.8},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_iata_code"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "experimental",
            "hackathon",
            "typed-entities",
            "geospatial",
            "column map expectation",
        ],
        "contributors": [
            "@jasmcaus",
        ],
        "requirements": ["pyairports"],
    }

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
        if configuration is None:
            configuration = self.configuration

    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        # password = configuration.kwargs.get("password")
        mostly = "{:.2%}".format(float(configuration.kwargs.get("mostly", 1)))

        return f'Are at least {mostly} of all values in column "{column}" valid IATA codes?'

    @classmethod
    @renderer(renderer_type="renderer.answer")
    def _answer_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        # password = result.expectation_config.kwargs.get("password")
        mostly = "{:.2%}".format(float(configuration.kwargs.get("mostly", 1)))

        if result.success:
            return f'At least {mostly} of all values in column "{column}" are valid IATA codes.'
        else:
            return f'Less than {mostly} of all values in column "{column}" are valid IATA codes.'


if __name__ == "__main__":
    ExpectColumnValuesToBeValidIATACode().print_diagnostic_checklist()
