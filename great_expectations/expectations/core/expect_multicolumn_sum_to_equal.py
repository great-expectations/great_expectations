from typing import Dict

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import num_to_str, substitute_none_for_missing


class ExpectMulticolumnSumToEqual(TableExpectation):
    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": [
            "core expectation",
            "column aggregate expectation",
            "needs migration to modular expectations api",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    #  Declaring "multicolumn_sum.equal.unexpected_count" is sufficient ("multicolumn_sum.equal.condition" is temporarily included for investigative purposes).
    # metric_dependencies = ("multicolumn_sum.equal.unexpected_count",)
    # Is specifying the dependency on a derived metric (i.e., the "condition_metric_name" + "." + suffix instead of only
    # "condition_metric_name") an acceptable coding pattern in expectation implementations (requires knowing suffixes)?
    # If affirmative, then a utility that can help to look up available metrics for an expectation type would be useful.
    condition_metric_suffixes = (
        "condition",
        "unexpected_count",
        # WARNING: Commented out suffixes result in unavailable metrics, due to dependency on the "column" domain key.
        # A plausible work-around: check whether or not the "column" domain key is present in "accessor_domain_kwargs".
        # "unexpected_index_list",
        # "unexpected_rows",
    )
    metric_dependencies = tuple(
        ["multicolumn_sum.equal" + "." + suffix for suffix in condition_metric_suffixes]
    )

    domain_keys = ("column_list",)
    success_keys = ("sum_total",)
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass

    """
    "get_validation_dependencies()" was not needed, because "TableExpectation.get_validation_dependencies()" with the
    appropriate class variables in the "condition provider" metric fulfill the requirements of this expectation.
    
    Which situations warrant the overriding of the "get_validation_dependencies()" method in the expectation itself?
    """

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        unexpected_count = metrics.get("multicolumn_sum.equal.unexpected_count")
        success = unexpected_count == 0
        return {
            "success": success,
            # What additional information would be useful to return as part of the "result" of this expectation?
            "result": {
                "unexpected_count": unexpected_count,
            },
        }
