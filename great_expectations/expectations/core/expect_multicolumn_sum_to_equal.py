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
    # This expectation is a stub - it needs migration to the modular expectation API

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

    # TODO: <Alex>ALEX</Alex>
    # metric_dependencies = ("multicolumn_sum_equal",)
    # TODO: <Alex>ALEX</Alex>
    metric_dependencies = ("multicolumn_sum.equal.condition",)
    # domain_keys = ("column_list",)
    # TODO: <Alex>ALEX</Alex>
    success_keys = (
        "column_list",
        "sum_total",
    )
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

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        # TODO: <Alex>ALEX</Alex>
        sum_total = self.get_success_kwargs(configuration).get("sum_total")
        # actual_sum_total = metrics.get("multicolumn_sum.equal.condition")
        success = metrics.get("multicolumn_sum.equal.condition")
        # TODO: <Alex>ALEX</Alex>
        return {
            "success": success,
            "result": {
                "observed_value": {
                    # TODO: <Alex>ALEX -- this is FAKE (for testing purposes)</Alex>
                    "something": 13,
                    # TODO: <Alex>ALEX -- this is FAKE (for testing purposes)</Alex>
                    "someother": 26,
                }
            },
        }
        # TODO: <Alex>ALEX</Alex>
