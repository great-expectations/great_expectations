import copy
from typing import Dict, Optional

import numpy as np

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
from great_expectations.validator.validation_graph import MetricConfiguration


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

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        column_list = configuration.kwargs.get("column_list")
        domain_kwargs = configuration.get_domain_kwargs()
        metric_domain_kwargs = copy.deepcopy(domain_kwargs)
        metric_domain_kwargs["columns"] = metric_domain_kwargs.pop("column_list")
        success_kwargs = configuration.get_success_kwargs()
        metric_value_kwargs = copy.deepcopy(success_kwargs)
        for domain_key in domain_kwargs:
            metric_value_kwargs.pop(domain_key, None)
        # TODO: <Alex>ALEX</Alex>
        ks = [
            "multicolumn_sum.equal.condition",
            "multicolumn_sum.equal.unexpected_count",
            # 'multicolumn_sum.equal.unexpected_index_list',
            # 'multicolumn_sum.equal.unexpected_rows',
        ]
        for k in ks:
            m = MetricConfiguration(
                metric_name=k,
                metric_domain_kwargs=metric_domain_kwargs,
                metric_value_kwargs=metric_value_kwargs,
                metric_dependencies=None,
            )
            dependencies["metrics"][k] = m
        # TODO: <Alex>ALEX</Alex>

        return dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        sum_total = self.get_success_kwargs(configuration).get("sum_total")
        # TODO: <Alex>ALEX</Alex>
        ks = [
            "multicolumn_sum.equal.condition",
            "multicolumn_sum.equal.unexpected_count",
            # 'multicolumn_sum.equal.unexpected_index_list',
            # 'multicolumn_sum.equal.unexpected_rows',
        ]
        for k in ks:
            v = metrics.get(k)
        # TODO: <Alex>ALEX</Alex>
        condition = metrics.get("multicolumn_sum.equal.condition")
        success = (
            np.count_nonzero([1 if v is False else 0 for v in condition[0].values]) == 0
        )
        # TODO: <Alex>ALEX</Alex>
        return {
            "success": success,
            "result": {
                "observed_value": {
                    "something": 13,
                    "someother": 26,
                }
            },
        }
        # TODO: <Alex>ALEX</Alex>
