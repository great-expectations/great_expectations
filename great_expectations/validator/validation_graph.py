from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration


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
    def __init__(self, edges: Optional[List[MetricEdge]] = None) -> None:
        if edges:
            self._edges = edges
        else:
            self._edges = []

        self._edge_ids = {edge.id for edge in self._edges}

    def add(self, edge: MetricEdge) -> None:
        if edge.id not in self._edge_ids:
            self._edges.append(edge)
            self._edge_ids.add(edge.id)

    def parse(
        self,
        metrics: Dict[Tuple[str, str, str], Any],
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

    @property
    def edges(self):
        return self._edges

    @property
    def edge_ids(self):
        return {edge.id for edge in self._edges}


class ExpectationValidationGraph:
    def __init__(self, configuration: ExpectationConfiguration) -> None:
        self._configuration = configuration
        self._graph = ValidationGraph()

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
        for metric_id, metric_info_item in metric_info.items():
            metric_exception_info.update(
                cast(Set[ExceptionInfo], metric_info_item["exception_info"])
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

    @property
    def configuration(self) -> ExpectationConfiguration:
        return self._configuration

    @property
    def graph(self) -> ValidationGraph:
        return self._graph
