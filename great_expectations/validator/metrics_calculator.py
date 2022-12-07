import logging
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
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
            runtime_configuration=None,
            min_graph_edges_pbar_enable=0,
            show_progress_bars=True,
        )
        return {
            metric_configuration.metric_name: resolved_metrics[metric_configuration.id]
            for metric_configuration in metrics.values()
        }

    def compute_metrics(
        self,
        metric_configurations: List[MetricConfiguration],
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,
        # Set to low number (e.g., 3) to suppress progress bar for small graphs.
        show_progress_bars: bool = True,
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """
        Args:
            metric_configurations: List of desired MetricConfiguration objects to be resolved.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            min_graph_edges_pbar_enable: Minumum number of graph edges to warrant showing progress bars.
            show_progress_bars: Directive for whether or not to show progress bars.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
        """
        graph: ValidationGraph = self.build_metric_dependency_graph(
            metric_configurations=metric_configurations,
            runtime_configuration=runtime_configuration,
        )
        resolved_metrics: Dict[
            Tuple[str, str, str], MetricValue
        ] = self.resolve_validation_graph_and_handle_aborted_metrics_info(
            graph=graph,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=min_graph_edges_pbar_enable,
            show_progress_bars=show_progress_bars,
        )
        return resolved_metrics

    def build_metric_dependency_graph(
        self,
        metric_configurations: List[MetricConfiguration],
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationGraph:
        """
        Obtain domain and value keys for metrics and proceeds to add these metrics to the validation graph
        until all metrics have been added.

        Args:
            metric_configurations: List of "MetricConfiguration" objects, for which to build combined "ValidationGraph".
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").

        Returns:
            Resulting "ValidationGraph" object.
        """
        graph: ValidationGraph = ValidationGraph(
            execution_engine=self._execution_engine
        )

        metric_configuration: MetricConfiguration
        for metric_configuration in metric_configurations:
            graph.build_metric_dependency_graph(
                metric_configuration=metric_configuration,
                runtime_configuration=runtime_configuration,
            )

        return graph

    @staticmethod
    def resolve_validation_graph_and_handle_aborted_metrics_info(
        graph: ValidationGraph,
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,
        # Set to low number (e.g., 3) to suppress progress bar for small graphs.
        show_progress_bars: bool = True,
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """
        Args:
            graph: "ValidationGraph" object, containing "metric_edge" structures with "MetricConfiguration" objects.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            min_graph_edges_pbar_enable: Minumum number of graph edges to warrant showing progress bars.
            show_progress_bars: Directive for whether or not to show progress bars.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
        """
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ]
        (
            resolved_metrics,
            aborted_metrics_info,
        ) = MetricsCalculator.resolve_validation_graph(
            graph=graph,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=min_graph_edges_pbar_enable,
            show_progress_bars=show_progress_bars,
        )

        if aborted_metrics_info:
            logger.warning(
                f"Exceptions\n{str(aborted_metrics_info)}\noccurred while resolving metrics."
            )

        return resolved_metrics

    @staticmethod
    def resolve_validation_graph(
        graph: ValidationGraph,
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,
        # Set to low number (e.g., 3) to suppress progress bar for small graphs.
        show_progress_bars: bool = True,
    ) -> Tuple[
        Dict[Tuple[str, str, str], MetricValue],
        Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ],
    ]:
        """
        Calls "ValidationGraph.resolve()" method with supplied arguments.

        Args:
            graph: "ValidationGraph" object, containing "metric_edge" structures with "MetricConfiguration" objects.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            min_graph_edges_pbar_enable: Minumum number of graph edges to warrant showing progress bars.
            show_progress_bars: Directive for whether or not to show progress bars.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
            Aborted metrics information, with metric ID as key.
        """
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ]
        resolved_metrics, aborted_metrics_info = graph.resolve(
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=min_graph_edges_pbar_enable,
            show_progress_bars=show_progress_bars,
        )
        return resolved_metrics, aborted_metrics_info
