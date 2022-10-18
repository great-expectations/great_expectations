from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from tqdm.auto import tqdm

import great_expectations.exceptions as ge_exceptions
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import MetricEdge, ValidationGraph

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


MAX_METRIC_COMPUTATION_RETRIES: int = 3


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
        resolved_metrics: Dict[Tuple[str, str, str], Any] = self.compute_metrics(
            metric_configurations=list(metrics.values()),
        )

        return {
            metric_configuration.metric_name: resolved_metrics[metric_configuration.id]
            for metric_configuration in metrics.values()
        }

    def compute_metrics(
        self,
        metric_configurations: List[MetricConfiguration],
    ) -> Dict[Tuple[str, str, str], Any]:
        """
        Args:
            metric_configurations: List of desired MetricConfiguration objects to be resolved.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
        """
        graph: ValidationGraph = ValidationGraph()

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

            self.build_metric_dependency_graph(
                graph=graph,
                metric_configuration=metric_configuration,
            )

        resolved_metrics: Dict[Tuple[str, str, str], Any] = {}

        # updates graph with aborted metrics
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ] = self.resolve_validation_graph(
            graph=graph,
            metrics=resolved_metrics,
        )

        if aborted_metrics_info:
            logger.warning(
                f"Exceptions\n{str(aborted_metrics_info)}\noccurred while resolving metrics."
            )

        return resolved_metrics

    def build_metric_dependency_graph(
        self,
        graph: ValidationGraph,
        metric_configuration: MetricConfiguration,
        runtime_configuration: Optional[dict] = None,
    ) -> None:
        """Obtain domain and value keys for metrics and proceeds to add these metrics to the validation graph
        until all metrics have been added."""

        metric_impl = get_metric_provider(
            metric_configuration.metric_name, execution_engine=self._execution_engine
        )[0]
        metric_dependencies = metric_impl.get_evaluation_dependencies(
            metric=metric_configuration,
            execution_engine=self._execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if len(metric_dependencies) == 0:
            graph.add(
                MetricEdge(
                    left=metric_configuration,
                )
            )
        else:
            metric_configuration.metric_dependencies = metric_dependencies
            for metric_dependency in metric_dependencies.values():
                # TODO: <Alex>In the future, provide a more robust cycle detection mechanism.</Alex>
                if metric_dependency.id == metric_configuration.id:
                    logger.warning(
                        f"Metric {str(metric_configuration.id)} has created a circular dependency"
                    )
                    continue
                graph.add(
                    MetricEdge(
                        left=metric_configuration,
                        right=metric_dependency,
                    )
                )
                self.build_metric_dependency_graph(
                    graph=graph,
                    metric_configuration=metric_dependency,
                    runtime_configuration=runtime_configuration,
                )

    def resolve_validation_graph(  # noqa: C901 - complexity 16
        self,
        graph: ValidationGraph,
        metrics: Dict[Tuple[str, str, str], Any],
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,  # Set to low number (e.g., 3) to suppress progress bar for small graphs.
        show_progress_bars: bool = True,
    ) -> Dict[
        Tuple[str, str, str],
        Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
    ]:
        if runtime_configuration is None:
            runtime_configuration = {}

        if runtime_configuration.get("catch_exceptions", True):
            catch_exceptions = True
        else:
            catch_exceptions = False

        failed_metric_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ] = {}
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ] = {}

        ready_metrics: Set[MetricConfiguration]
        needed_metrics: Set[MetricConfiguration]

        exception_info: ExceptionInfo

        progress_bar: Optional[tqdm] = None

        done: bool = False
        while not done:
            ready_metrics, needed_metrics = graph.parse(metrics=metrics)

            # Check to see if the user has disabled progress bars
            disable = not show_progress_bars
            if len(graph.edges) < min_graph_edges_pbar_enable:
                disable = True

            if progress_bar is None:
                # noinspection PyProtectedMember,SpellCheckingInspection
                progress_bar = tqdm(
                    total=len(ready_metrics) + len(needed_metrics),
                    desc="Calculating Metrics",
                    disable=disable,
                )
            progress_bar.update(0)
            progress_bar.refresh()

            computable_metrics = set()

            for metric in ready_metrics:
                if metric.id in failed_metric_info and failed_metric_info[metric.id]["num_failures"] >= MAX_METRIC_COMPUTATION_RETRIES:  # type: ignore
                    aborted_metrics_info[metric.id] = failed_metric_info[metric.id]
                else:
                    computable_metrics.add(metric)

            try:
                metrics.update(
                    self._resolve_metrics(
                        metrics_to_resolve=computable_metrics,
                        metrics=metrics,
                        runtime_configuration=runtime_configuration,
                    )
                )
                progress_bar.update(len(computable_metrics))
                progress_bar.refresh()
            except ge_exceptions.MetricResolutionError as err:
                if catch_exceptions:
                    exception_traceback = traceback.format_exc()
                    exception_message = str(err)
                    exception_info = ExceptionInfo(
                        exception_traceback=exception_traceback,
                        exception_message=exception_message,
                    )
                    for failed_metric in err.failed_metrics:
                        if failed_metric.id in failed_metric_info:
                            failed_metric_info[failed_metric.id]["num_failures"] += 1  # type: ignore
                            failed_metric_info[failed_metric.id]["exception_info"].add(exception_info)  # type: ignore
                        else:
                            failed_metric_info[failed_metric.id] = {}
                            failed_metric_info[failed_metric.id][
                                "metric_configuration"
                            ] = failed_metric
                            failed_metric_info[failed_metric.id]["num_failures"] = 1  # type: ignore
                            failed_metric_info[failed_metric.id]["exception_info"] = {
                                exception_info
                            }
                else:
                    raise err
            except Exception as e:
                if catch_exceptions:
                    logger.error(
                        f"""Caught exception {str(e)} while trying to resolve a set of {len(ready_metrics)} metrics; aborting graph resolution."""
                    )
                    done = True
                else:
                    raise e

            if (len(ready_metrics) + len(needed_metrics) == 0) or (
                len(ready_metrics) == len(aborted_metrics_info)
            ):
                done = True

        progress_bar.close()  # type: ignore

        return aborted_metrics_info

    def _resolve_metrics(
        self,
        metrics_to_resolve: Iterable[MetricConfiguration],
        metrics: Dict[Tuple[str, str, str], Any] = None,
        runtime_configuration: dict = None,
    ) -> Dict[Tuple[str, str, str], MetricConfiguration]:
        """A means of accessing the Execution Engine's resolve_metrics method, where missing metric configurations are
        resolved"""
        return self._execution_engine.resolve_metrics(
            metrics_to_resolve=metrics_to_resolve,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
        )

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
