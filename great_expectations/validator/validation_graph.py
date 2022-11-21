import logging
import traceback
from typing import Dict, List, Optional, Set, Tuple, Union, cast

from tqdm.auto import tqdm

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)
logging.captureWarnings(True)

MAX_METRIC_COMPUTATION_RETRIES: int = 3


class MetricEdge:
    def __init__(
        self, left: MetricConfiguration, right: Optional[MetricConfiguration] = None
    ) -> None:
        self._left = left
        self._right = right

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    @property
    def id(self):
        if self.right:
            return self.left.id, self.right.id
        return self.left.id, None


class ValidationGraph:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        edges: Optional[List[MetricEdge]] = None,
    ) -> None:
        self._execution_engine = execution_engine

        if edges:
            self._edges = edges
        else:
            self._edges = []

        self._edge_ids = {edge.id for edge in self._edges}

    @property
    def edges(self):
        return self._edges

    @property
    def edge_ids(self):
        return {edge.id for edge in self._edges}

    def add(self, edge: MetricEdge) -> None:
        if edge.id not in self._edge_ids:
            self._edges.append(edge)
            self._edge_ids.add(edge.id)

    def build_metric_dependency_graph(
        self,
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
            self.add(
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
                self.add(
                    MetricEdge(
                        left=metric_configuration,
                        right=metric_dependency,
                    )
                )
                self.build_metric_dependency_graph(
                    metric_configuration=metric_dependency,
                    runtime_configuration=runtime_configuration,
                )

    def resolve_validation_graph(  # noqa: C901 - complexity 16
        self,
        metrics: Dict[Tuple[str, str, str], MetricValue],
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
            ready_metrics, needed_metrics = self._parse(metrics=metrics)

            # Check to see if the user has disabled progress bars
            disable = not show_progress_bars
            if len(self.edges) < min_graph_edges_pbar_enable:
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
                if metric.id in failed_metric_info and failed_metric_info[metric.id]["num_failures"] >= MAX_METRIC_COMPUTATION_RETRIES:  # type: ignore[operator]  # Incorrect flagging of 'Unsupported operand types for <= ("int" and "MetricConfiguration") and for >= ("Set[ExceptionInfo]" and "int")' in deep "Union" structure.
                    aborted_metrics_info[metric.id] = failed_metric_info[metric.id]
                else:
                    computable_metrics.add(metric)

            try:
                # Access "ExecutionEngine.resolve_metrics()" method, to resolve missing "MetricConfiguration" objects.
                metrics.update(
                    self._execution_engine.resolve_metrics(
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
                            failed_metric_info[failed_metric.id]["num_failures"] += 1  # type: ignore[operator]  # Incorrect flagging of 'Unsupported operand types for <= ("int" and "MetricConfiguration") and for >= ("Set[ExceptionInfo]" and "int")' in deep "Union" structure.
                            failed_metric_info[failed_metric.id]["exception_info"].add(exception_info)  # type: ignore[union-attr]  # Incorrect flagging of 'Item "MetricConfiguration" of "Union[MetricConfiguration, Set[ExceptionInfo], int]" has no attribute "add" and Item "int" of "Union[MetricConfiguration, Set[ExceptionInfo], int]" has no attribute "add"' in deep "Union" structure.
                        else:
                            failed_metric_info[failed_metric.id] = {}
                            failed_metric_info[failed_metric.id][
                                "metric_configuration"
                            ] = failed_metric
                            failed_metric_info[failed_metric.id]["num_failures"] = 1
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

        progress_bar.close()  # type: ignore[union-attr]  # Incorrect flagging of 'Item "None" of "Optional[Any]" has no attribute "close"' in external package.

        return aborted_metrics_info

    def _parse(
        self,
        metrics: Dict[Tuple[str, str, str], MetricValue],
    ) -> Tuple[Set[MetricConfiguration], Set[MetricConfiguration]]:
        """Given validation graph, returns the ready and needed metrics necessary for validation using a traversal of
        validation graph (a graph structure of metric ids) edges"""
        unmet_dependency_ids = set()
        unmet_dependency = set()
        maybe_ready_ids = set()
        maybe_ready = set()

        for edge in self.edges:
            if edge.left.id not in metrics:
                if edge.right is None or edge.right.id in metrics:
                    if edge.left.id not in maybe_ready_ids:
                        maybe_ready_ids.add(edge.left.id)
                        maybe_ready.add(edge.left)
                else:
                    if edge.left.id not in unmet_dependency_ids:
                        unmet_dependency_ids.add(edge.left.id)
                        unmet_dependency.add(edge.left)

        return maybe_ready - unmet_dependency, unmet_dependency


class ExpectationValidationGraph:
    def __init__(
        self,
        execution_engine: ExecutionEngine,
        configuration: ExpectationConfiguration,
    ) -> None:
        self._configuration = configuration
        self._graph = ValidationGraph(execution_engine=execution_engine)

    @property
    def configuration(self) -> ExpectationConfiguration:
        return self._configuration

    @property
    def graph(self) -> ValidationGraph:
        return self._graph

    def update(self, graph: ValidationGraph) -> None:
        edge: MetricEdge
        for edge in graph.edges:
            self.graph.add(edge=edge)

    def get_exception_info(
        self,
        metric_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ],
    ) -> Set[ExceptionInfo]:
        metric_info = self._filter_metric_info_in_graph(metric_info=metric_info)
        metric_exception_info: Set[ExceptionInfo] = set()
        metric_id: Tuple[str, str, str]
        metric_info_item: Union[MetricConfiguration, Set[ExceptionInfo], int]
        for metric_id, metric_info_item in metric_info.items():  # type: ignore[assignment]  # Incorrect flagging of 'Incompatible types in assignment (expression has type "Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]]", variable has type "Union[MetricConfiguration, Set[ExceptionInfo], int]")' in deep "Union" structure.
            # noinspection PyUnresolvedReferences
            metric_exception_info.update(
                cast(Set[ExceptionInfo], metric_info_item["exception_info"])  # type: ignore[index]  # Incorrect flagging of 'Value of type "Union[MetricConfiguration, Set[ExceptionInfo], int]" is not indexable' in deep "Union" structure.
            )

        return metric_exception_info

    def _filter_metric_info_in_graph(
        self,
        metric_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ],
    ) -> Dict[
        Tuple[str, str, str],
        Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
    ]:
        graph_metric_ids: List[Tuple[str, str, str]] = []
        edge: MetricEdge
        vertex: MetricConfiguration
        for edge in self.graph.edges:
            for vertex in [edge.left, edge.right]:
                if vertex is not None:
                    graph_metric_ids.append(vertex.id)

        metric_id: Tuple[str, str, str]
        metric_info_item: Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]]
        return {
            metric_id: metric_info_item
            for metric_id, metric_info_item in metric_info.items()
            if metric_id in graph_metric_ids
        }
