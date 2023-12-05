"""
This is a template for creating custom MulticolumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations
"""


from great_expectations.execution_engine import (
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most MulticolumnMapExpectations, the main business logic for calculation will live in this class.
# <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py MulticolumnValuesMatchSomeCriteria class_def">
class MulticolumnValuesMatchSomeCriteria(MulticolumnMapMetricProvider):
    # </snippet>
    # This is the id string that will be used to reference your metric.
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py metric_name">
    condition_metric_name = "METRIC NAME GOES HERE"
    # </snippet>
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py pandas">
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        raise NotImplementedError

    # </snippet>

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_list, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_list, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
# <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py ExpectMulticolumnValuesToMatchSomeCriteria class_def">
class ExpectMulticolumnValuesToMatchSomeCriteria(MulticolumnMapExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py docstring">
    """TODO: Add a docstring here"""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = []

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py map_metric">
    map_metric = "METRIC NAME GOES HERE"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_list",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py library_metadata">
    library_metadata = {
        "tags": [],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@your_name_here",  # Don't forget to add your github handle here!
        ],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="tests/integration/docusaurus/expectations/examples/multicolumn_map_expectation_template.py diagnostics">
    ExpectMulticolumnValuesToMatchSomeCriteria().print_diagnostic_checklist()
# </snippet>
