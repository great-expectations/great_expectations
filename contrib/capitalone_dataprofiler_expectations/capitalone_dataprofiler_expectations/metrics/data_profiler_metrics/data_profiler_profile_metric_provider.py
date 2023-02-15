import logging
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.expectations.metrics.metric_provider import MetricProvider
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)


class DataProfilerProfileMetricProvider(MetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    value_keys = ("profile_path",)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return super()._get_evaluation_dependencies(
            metric, configuration, execution_engine, runtime_configuration
        )
