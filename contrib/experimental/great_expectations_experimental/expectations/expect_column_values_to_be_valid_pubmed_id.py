import re
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

PUBMED_ID_REGEX = r"^([1-3]\d{7}|[1-9]\d{0,6})$"


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesToBeValidPubmedId(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_pubmed_id"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def matches_pubmed_id_regex(x):
            if re.match(PUBMED_ID_REGEX, str(x)):
                return True
            return False

        return column.apply(lambda x: matches_pubmed_id_regex(x) if x else False)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidPubmedId(ColumnMapExpectation):
    """This Expectation validates data as conforming to the valid PubMed ID format."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "well_formed_pubmed_id": [
                    "823",
                    "4388",
                    "17246615",
                    "32768924",
                ],
                "malformed_pubmed_id": [
                    "172466159",
                    "01724661",
                    "10.103/nphys1170",
                    "This is not a valid PubMed ID",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "well_formed_pubmed_id"},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "malformed_pubmed_id"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_pubmed_id"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

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

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "hackathon", "typed-entities"],
        "contributors": [
            "@voidforall",
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidPubmedId().print_diagnostic_checklist()
