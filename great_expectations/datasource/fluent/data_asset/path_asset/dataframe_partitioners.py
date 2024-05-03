from typing import TYPE_CHECKING, Any, Dict, List, Literal

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.interfaces import Batch

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import BatchParameters


class _PartitionerDatetime(FluentBaseModel):
    column_name: str
    method_name: str
    sort_ascending: bool = True

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        """Validates all the datetime parameters for this partitioner exist in `options`."""
        identifiers: Dict = {}
        for part in self.param_names:
            if part in options:
                identifiers[part] = options[part]
        return {self.column_name: identifiers}

    def _get_concrete_values_from_batch(self, batch: Batch) -> tuple[int]:
        return tuple(batch.metadata[param] for param in self.param_names)

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError


class DataframePartitionerYearly(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year"] = "partition_on_year"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class DataframePartitionerMonthly(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class DataframePartitionerDaily(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month_and_day"] = (
        "partition_on_year_and_month_and_day"
    )

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month", "day"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}
