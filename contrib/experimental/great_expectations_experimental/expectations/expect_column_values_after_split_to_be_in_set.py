"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

from typing import List, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


def are_values_after_split_in_value_set(
    val: str, delimiter: str, value_set: List[str]
) -> bool:
    all_split_values = [v.strip() for v in val.split(delimiter)]

    for val in all_split_values:
        if val not in value_set:
            return False

    return True


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesAfterSplitInSet(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.values_after_split_in_set"
    condition_value_keys = (
        "delimiter",
        "value_set",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, delimiter, value_set, **kwargs):
        value_set = set(value_set)
        return column.apply(
            lambda x: are_values_after_split_in_value_set(x, delimiter, value_set)
        )

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesAfterSplitToBeInSet(ColumnMapExpectation):
    """Expect values in the column after splitting on a delimiter to be in a pre-defined set."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "allowed_sports": [
                    "hockey,football",
                    "cricket,tennis",
                    "tennis,badminton,hockey",
                ],
                "not_sports": ["cnn,hockey", "football", "badminton,judo,BBC"],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "allowed_sports",
                        "delimiter": ",",
                        "value_set": [
                            "hockey",
                            "football",
                            "tennis",
                            "badminton",
                            "cricket",
                        ],
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
                        "column": "not_sports",
                        "delimiter": ",",
                        "value_set": [
                            "hockey",
                            "football",
                            "tennis",
                            "badminton",
                            "cricket",
                        ],
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "postive_test_with_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "not_sports",
                        "delimiter": ",",
                        "value_set": [
                            "hockey",
                            "football",
                            "tennis",
                            "badminton",
                            "cricket",
                        ],
                        "mostly": 0.33,
                    },
                    "out": {
                        "success": True,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.values_after_split_in_set"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly", "value_set", "delimiter")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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
        configuration = configuration or self.configuration

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
        "tags": ["pandas"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@ace-racer",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesAfterSplitToBeInSet().print_diagnostic_checklist()
