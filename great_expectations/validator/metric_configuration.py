import json
from typing import Optional, Tuple

from great_expectations.core.domain import Domain
from great_expectations.core.id_dict import IDDict
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable


class MetricConfiguration:
    def __init__(
        self,
        metric_name: str,
        metric_domain_kwargs: dict,
        metric_value_kwargs: Optional[dict] = None,
        metric_dependencies: Optional[dict] = None,
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
        if metric_dependencies is not None:
            self._metric_dependencies = IDDict(metric_dependencies)

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
    def metric_dependencies(self, metric_dependencies) -> None:
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

    def to_json_dict(self) -> dict:
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
