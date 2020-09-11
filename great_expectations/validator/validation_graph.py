from typing import Optional, Set, Union

from great_expectations.core.id_dict import IDDict


class MetricEdgeKey(object):
    def __init__(
        self, metric_name: str, metric_domain_kwargs: dict, metric_value_kwargs: dict
    ):
        self._metric_name = metric_name
        if not isinstance(metric_domain_kwargs, IDDict):
            metric_domain_kwargs = IDDict(metric_domain_kwargs)
        self._metric_domain_kwargs = metric_domain_kwargs
        if not isinstance(metric_value_kwargs, IDDict):
            metric_value_kwargs = IDDict(metric_value_kwargs)
        self._metric_value_kwargs = metric_value_kwargs

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_domain_kwargs(self):
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self):
        return self._metric_value_kwargs

    @property
    def metric_domain_kwargs_id(self):
        return self._metric_domain_kwargs.to_id()

    @property
    def metric_value_kwargs_id(self):
        return self._metric_value_kwargs.to_id()

    @property
    def id(self):
        return (
            self.metric_name,
            self.metric_domain_kwargs_id,
            self.metric_value_kwargs_id,
        )

    # def __hash__(self):
    #     return self.id.__hash__()


class MetricEdge(object):
    def __init__(self, left: MetricEdgeKey, right: Union[MetricEdgeKey, None]):
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
        return self.left.id, self.right.id

    # def __hash__(self):
    #     return self.id


class ValidationGraph(object):
    def __init__(self, domain: dict, edges: Optional[Set[MetricEdge]] = None):
        self._domain = IDDict(domain)
        if edges:
            self.edges = edges
        else:
            self.edges = set()

    @property
    def domain(self):
        return self._domain

    @property
    def domain_id(self):
        return self._domain.to_id()
