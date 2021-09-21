import copy
from typing import Dict, List, Optional, Set, Tuple, Union, cast

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration


class MetricEdge:
    def __init__(
        self, left: MetricConfiguration, right: Optional[MetricConfiguration] = None
    ):
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
    def __init__(self, edges: Optional[List[MetricEdge]] = None):
        if edges:
            self._edges = edges
        else:
            self._edges = []

        self._edge_ids = {edge.id for edge in self._edges}

    def add(self, edge: MetricEdge):
        if edge.id not in self._edge_ids:
            self._edges.append(edge)
            self._edge_ids.add(edge.id)

    @property
    def edges(self):
        return copy.deepcopy(self._edges)

    @property
    def edge_ids(self):
        return {edge.id for edge in self.edges}


class ExpectationValidationGraph:
    def __init__(self, configuration: ExpectationConfiguration):
        self._configuration = configuration
        self._graph = ValidationGraph()

    def update(self, graph: ValidationGraph):
        edge: MetricEdge
        for edge in graph.edges:
            self.graph.add(edge=edge)

    def get_exception_info(
        self,
        metric_infos: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ],
    ) -> Set[ExceptionInfo]:
        metric_infos = self._filter_metric_infos_in_graph(metric_infos=metric_infos)
        metric_exception_info: Set[ExceptionInfo] = set()
        metric_id: str
        metric_info: Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]]
        exception_info: ExceptionInfo
        for metric_id, metric_info in metric_infos.items():
            metric_exception_info.update(
                cast(Set[ExceptionInfo], metric_info["exception_info"])
            )

        return metric_exception_info

    def _filter_metric_infos_in_graph(
        self,
        metric_infos: Dict[
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

        return {
            metric_id: metric_info
            for metric_id, metric_info in metric_infos.items()
            if metric_id in graph_metric_ids
        }

    @property
    def configuration(self) -> ExpectationConfiguration:
        return self._configuration

    @property
    def graph(self) -> ValidationGraph:
        return self._graph
