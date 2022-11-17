from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import ValidationGraph

logger = logging.getLogger(__name__)
logging.captureWarnings(True)

try:
    import pandas as pd
except ImportError:
    pd = None

    logger.debug(
        "Unable to load pandas; install optional pandas dependency for support."
    )

if TYPE_CHECKING:
    from great_expectations.expectations.metrics.metric_provider import MetricProvider


class MetricsCalculator:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        show_progress_bars: bool = False,
    ) -> None:
        """
        MetricsCalculator accepts and processes metrics calculation requests.

        Args:
            execution_engine: ExecutionEngine to perform metrics computation.
        """
        self._execution_engine: ExecutionEngine = execution_engine
        self._show_progress_bars: bool = show_progress_bars

    @property
    def show_progress_bars(self) -> bool:
        return self._show_progress_bars

    @show_progress_bars.setter
    def show_progress_bars(self, enable: bool) -> None:
        self._show_progress_bars = enable

    def columns(self, domain_kwargs: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Convenience method to run "table.columns" metric.
        """
        if domain_kwargs is None:
            domain_kwargs = {
                "batch_id": self._execution_engine.batch_manager.active_batch_data_id,
            }

        columns: List[str] = self.get_metric(
            metric=MetricConfiguration(
                metric_name="table.columns",
                metric_domain_kwargs=domain_kwargs,
            )
        )

        return columns

    def head(
        self,
        n_rows: int = 5,
        domain_kwargs: Optional[Dict[str, Any]] = None,
        fetch_all: bool = False,
    ) -> pd.DataFrame:
        """
        Convenience method to run "table.head" metric.
        """
        if domain_kwargs is None:
            domain_kwargs = {
                "batch_id": self._execution_engine.batch_manager.active_batch_data_id,
            }

        data: Any = self.get_metric(
            metric=MetricConfiguration(
                metric_name="table.head",
                metric_domain_kwargs=domain_kwargs,
                metric_value_kwargs={
                    "n_rows": n_rows,
                    "fetch_all": fetch_all,
                },
            )
        )

        if isinstance(
            self._execution_engine, (PandasExecutionEngine, SqlAlchemyExecutionEngine)
        ):
            df = pd.DataFrame(data=data)
        elif isinstance(self._execution_engine, SparkDFExecutionEngine):
            rows: List[Dict[str, Any]] = [datum.asDict() for datum in data]
            df = pd.DataFrame(data=rows)
        else:
            raise ge_exceptions.GreatExpectationsError(
                "Unsupported or unknown ExecutionEngine type encountered in Validator class."
            )

        return df.reset_index(drop=True, inplace=False)

    def get_metric(
        self,
        metric: MetricConfiguration,
    ) -> Any:
        """return the value of the requested metric."""
        return self.get_metrics(
            metrics={metric.metric_name: metric},
        )[metric.metric_name]

    def get_metrics(
        self,
        metrics: Dict[str, MetricConfiguration],
    ) -> Dict[str, Any]:
        """
        Args:
            metrics: Dictionary of desired metrics to be resolved; metric_name is key and MetricConfiguration is value.

        Returns:
            Return Dictionary with requested metrics resolved, with metric_name as key and computed metric as value.
        """
        resolved_metrics: Dict[
            Tuple[str, str, str], MetricValue
        ] = self.compute_metrics(
            metric_configurations=list(metrics.values()),
        )

        return {
            metric_configuration.metric_name: resolved_metrics[metric_configuration.id]
            for metric_configuration in metrics.values()
        }

    def compute_metrics(
        self,
        metric_configurations: List[MetricConfiguration],
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """
        Args:
            metric_configurations: List of desired MetricConfiguration objects to be resolved.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
        """
        graph: ValidationGraph = ValidationGraph(
            execution_engine=self._execution_engine
        )

        metric_configuration: MetricConfiguration
        for metric_configuration in metric_configurations:
            provider_cls, _ = get_metric_provider(
                metric_configuration.metric_name, self._execution_engine
            )

            self._get_default_domain_kwargs(
                metric_provider_cls=provider_cls,
                metric_configuration=metric_configuration,
            )
            self._get_default_value_kwargs(
                metric_provider_cls=provider_cls,
                metric_configuration=metric_configuration,
            )

            graph.build_metric_dependency_graph(
                metric_configuration=metric_configuration,
                runtime_configuration=None,
            )

        resolved_metrics: Dict[Tuple[str, str, str], MetricValue] = {}

        # updates graph with aborted metrics
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ] = graph.resolve_validation_graph(
            metrics=resolved_metrics,
            runtime_configuration=None,
        )

        if aborted_metrics_info:
            logger.warning(
                f"Exceptions\n{str(aborted_metrics_info)}\noccurred while resolving metrics."
            )

        return resolved_metrics

    @staticmethod
    def _get_default_domain_kwargs(
        metric_provider_cls: MetricProvider,
        metric_configuration: MetricConfiguration,
    ) -> None:
        for key in metric_provider_cls.domain_keys:
            if (
                key not in metric_configuration.metric_domain_kwargs
                and key in metric_provider_cls.default_kwarg_values
            ):
                metric_configuration.metric_domain_kwargs[
                    key
                ] = metric_provider_cls.default_kwarg_values[key]

    @staticmethod
    def _get_default_value_kwargs(
        metric_provider_cls: MetricProvider,
        metric_configuration: MetricConfiguration,
    ) -> None:
        key: str
        for key in metric_provider_cls.value_keys:
            if (
                key not in metric_configuration.metric_value_kwargs
                and key in metric_provider_cls.default_kwarg_values
            ):
                metric_configuration.metric_value_kwargs[
                    key
                ] = metric_provider_cls.default_kwarg_values[key]
