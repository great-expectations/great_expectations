"""
This is a template for creating custom ColumnPairMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations
"""

from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnPairMapExpectations, the main business logic for calculation will live in this class.
# <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py ColumnPairValuesMatchSomeCriteria class_def">
class ColumnPairValuesMatchSomeCriteria(ColumnPairMapMetricProvider):
    # </snippet>
    # This is the id string that will be used to reference your metric.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py metric_name">
    condition_metric_name = "METRIC NAME GOES HERE"
    # </snippet>
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py pandas">
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        raise NotImplementedError

    # </snippet>

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_A, column_B, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_A, column_B, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py ExpectColumnPairValuesToMatchSomeCriteria class_def">
class ExpectColumnPairValuesToMatchSomeCriteria(ColumnPairMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py docstring">
    """TODO: Add a docstring here"""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = []

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py map_metric">
    map_metric = "METRIC NAME GOES HERE"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_A",
        "column_B",
        "mostly",
    )

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
    # <snippet name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet  name="tests/integration/docusaurus/expectations/examples/column_pair_map_expectation_template.py diagnostics">
    ExpectColumnPairValuesToMatchSomeCriteria().print_diagnostic_checklist()
#     </snippet>
