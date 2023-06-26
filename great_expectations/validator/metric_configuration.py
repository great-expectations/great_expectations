import json
from typing import Optional, Tuple

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.domain import Domain
from great_expectations.core.id_dict import IDDict
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable


@public_api
class MetricConfiguration:
    """An interface for configuring Metrics.

    MetricConfiguration allows the configuration of domain information, dependencies and additional metric-specific
    configurations.  Metrics are computed attributes of data, and are derived from one or more Batches that can then
    be used to evaluate Expectations or to summarize the result of the Validation.

    Args:
        metric_name (str): name of the Metric defined by the current MetricConfiguration.
        metric_domain_kwargs (dict): provides information on where the Metric can be calculated. For instance, a
            MapCondition metric can include the name of the column that the Metric is going to be run on.
        metric_value_kwargs (optional[dict]): Optional kwargs that define values specific to each Metric.  For instance,
            a Metric that partitions a column can define the method of partitioning (`uniform` bins) and the number of
            bins (`n_bins`) as `metric_value_kwargs`.
    """

    def __init__(
        self,
        metric_name: str,
        metric_domain_kwargs: dict,
        metric_value_kwargs: Optional[dict] = None,
    ) -> None:
        self._metric_name = metric_name

        if not isinstance(metric_domain_kwargs, IDDict):
            metric_domain_kwargs = IDDict(metric_domain_kwargs)

        self._metric_domain_kwargs: IDDict = metric_domain_kwargs

        if not isinstance(metric_value_kwargs, IDDict):
            if metric_value_kwargs is None:
                metric_value_kwargs = {}
            metric_value_kwargs = IDDict(metric_value_kwargs)

        self._metric_value_kwargs: IDDict = metric_value_kwargs

        self._metric_dependencies: IDDict = IDDict({})

    def __repr__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    def __str__(self):
        return self.__repr__()

    @property
    def metric_name(self) -> str:
        return self._metric_name

    @property
    def metric_domain_kwargs(self) -> IDDict:
        return self._metric_domain_kwargs

    @property
    def metric_value_kwargs(self) -> IDDict:
        return self._metric_value_kwargs

    @property
    def metric_domain_kwargs_id(self) -> str:
        return self.metric_domain_kwargs.to_id()

    @property
    def metric_value_kwargs_id(self) -> str:
        return self.metric_value_kwargs.to_id()

    @property
    def metric_dependencies(self) -> IDDict:
        return self._metric_dependencies

    @metric_dependencies.setter
    def metric_dependencies(self, metric_dependencies: dict) -> None:
        if metric_dependencies is None:
            metric_dependencies = IDDict({})

        if not isinstance(metric_dependencies, IDDict):
            metric_dependencies = IDDict(metric_dependencies)

        self._metric_dependencies = metric_dependencies

    def get_domain(self) -> Domain:
        """Return "Domain" object, constructed from this "MetricConfiguration" object."""
        domain_type: MetricDomainTypes = self.get_domain_type()

        if domain_type == MetricDomainTypes.TABLE:
            other_table_name: Optional[str] = self._metric_domain_kwargs.get("table")
            if other_table_name:
                return Domain(
                    domain_type=domain_type,
                    domain_kwargs={
                        "table": other_table_name,
                    },
                )

            return Domain(
                domain_type=domain_type,
            )

        if domain_type == MetricDomainTypes.COLUMN:
            return Domain(
                domain_type=domain_type,
                domain_kwargs={
                    "column": self._metric_domain_kwargs["column"],
                },
            )

        if domain_type == MetricDomainTypes.COLUMN_PAIR:
            return Domain(
                domain_type=domain_type,
                domain_kwargs={
                    "column_A": self._metric_domain_kwargs["column_A"],
                    "column_B": self._metric_domain_kwargs["column_B"],
                },
            )

        if domain_type == MetricDomainTypes.MULTICOLUMN:
            return Domain(
                domain_type=domain_type,
                domain_kwargs={
                    "column_list": self._metric_domain_kwargs["column_list"],
                },
            )

        raise ValueError(f"""Domain type "{domain_type}" is not recognized.""")

    def get_domain_type(self) -> MetricDomainTypes:
        """Return "domain_type" of this "MetricConfiguration" object."""
        if "column" in self._metric_domain_kwargs:
            return MetricDomainTypes.COLUMN

        if (
            "column_A" in self._metric_domain_kwargs
            and "column_B" in self._metric_domain_kwargs
        ):
            return MetricDomainTypes.COLUMN_PAIR

        if "column_list" in self._metric_domain_kwargs:
            return MetricDomainTypes.MULTICOLUMN

        # TODO: <Alex>Determining "domain_type" of "MetricConfiguration" using heuristics defaults to "TABLE".</Alex>
        return MetricDomainTypes.TABLE

    @property
    def id(self) -> Tuple[str, str, str]:
        return (
            self.metric_name,
            self.metric_domain_kwargs_id,
            self.metric_value_kwargs_id,
        )

    @public_api
    def to_json_dict(self) -> dict:
        """Returns a JSON-serializable dict representation of this MetricConfiguration.

        Returns:
            A JSON-serializable dict representation of this MetricConfiguration.

        """
        json_dict: dict = convert_to_json_serializable(
            data={
                "metric_name": self.metric_name,
                "metric_domain_kwargs": self.metric_domain_kwargs,
                "metric_domain_kwargs_id": self.metric_domain_kwargs_id,
                "metric_value_kwargs": self.metric_value_kwargs,
                "metric_value_kwargs_id": self.metric_value_kwargs_id,
                "id": self.id,
            }
        )
        return json_dict
