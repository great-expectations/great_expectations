from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Tuple

from great_expectations.core import ExpectationConfiguration  # noqa: TCH001
from great_expectations.core._docs_decorators import public_api
from great_expectations.expectations.metrics.map_metric_provider import (
    MapMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class MulticolumnMapMetricProvider(MapMetricProvider):
    """Defines metrics that are evaluated for every row for a set of columns. All multi-column metrics require the
    domain key `column_list`.

    `expect_compound_columns_to_be_unique` is an example of an Expectation that uses this metric.
    """

    condition_domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    function_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: dict = {
            k: v
            for k, v in metric.metric_domain_kwargs.items()
            if k not in ["column_list", "ignore_row_if"]
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies
