import json

import langid   # 1.1.6

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validator import Validator


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesAreEnglish(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.are_english"

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        # classify() outputs a tuple (label, probability)
        labels = column.apply(lambda x: langid.classify(x)[0])
        return labels == "en"

    # # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     return column.in_([3])

    #This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     return column.isin([3])

# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToBeEnglish(ColumnMapExpectation):
    """Defines an expectation such that column values are in the English language."""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [{
        "data": {
            "mostly_english": [
                "Twinkle, twinkle, little star. How I wonder what you are.",
                "Up above the world so high, Like a diamond in the sky.",
                "Twinkle, twinkle, little star. Up above the world so high.",
                "Brilla brilla pequeña estrella. Cómo me pregunto lo que eres.",
                None,
            ],
        },
        "tests": [
            {
                "title": "positive_test_with_mostly",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "mostly_english",
                    "mostly": 0.6
                 },
                "out": {
                    "success": True,
                    "unexpected_index_list": [3],
                    "unexpected_list": [4, 5],
                },
            }
        ],
    }]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "nlp",
            "hackathon"
        ],
        "contributors": [
            "@victorwyee"
        ],
        "package": "experimental_expectations",
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_english"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    #!!! This example renderer should render RenderedStringTemplateContent, not just a string


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesToBeEnglish().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
