"""
This is a template for creating custom BatchExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.expectations.metrics.metric_provider import (
    MetricConfiguration,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)


# This class defines a Metric to support your Expectation.
# For most BatchExpectations, the main business logic for calculation will live in this class.
# <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py BatchMeetsSomeCriteria class_def">
class BatchMeetsSomeCriteria(TableMetricProvider):
    # </snippet>

    # This is the id string that will be used to reference your Metric.
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py metric_name">
    metric_name = "METRIC NAME GOES HERE"
    # </snippet>

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py pandas">
    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        raise NotImplementedError

    # </snippet>

    # @metric_value(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(
    #         cls,
    #         execution_engine,
    #         metric_domain_kwargs,
    #         metric_value_kwargs,
    #         metrics,
    #         runtime_configuration,
    # ):
    #    raise NotImplementedError

    # @metric_value(engine=SparkDFExecutionEngine)
    # def _spark(
    #         cls,
    #         execution_engine,
    #         metric_domain_kwargs,
    #         metric_value_kwargs,
    #         metrics,
    #         runtime_configuration,
    # ):
    #    raise NotImplementedError

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.columns": MetricConfiguration(
                "table.columns", metric.metric_domain_kwargs
            ),
        }


# This class defines the Expectation itself
# The main business logic for calculation lives here.
# <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py ExpectBatchToMeetSomeCriteria class_def">
class ExpectBatchToMeetSomeCriteria(BatchExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py docstring">
    """TODO: add a docstring here"""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py examples">
    examples = []
    # </snippet>

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py metric_dependencies">
    metric_dependencies = ("METRIC NAME GOES HERE",)
    # </snippet>

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ()

    # This dictionary contains default values for any parameters that should have default values.
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

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py validate">
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        # </snippet>
        raise NotImplementedError

    # This object contains metadata for display in the public Gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }


#     </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/batch_expectation_template.py diagnostics">
    ExpectBatchToMeetSomeCriteria().print_diagnostic_checklist()
#     </snippet>
