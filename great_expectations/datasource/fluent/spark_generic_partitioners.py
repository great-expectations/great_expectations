from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Protocol,
    Union,
)

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
)
from great_expectations.datasource.fluent.interfaces import PartitionerProtocol
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import BatchParameters
    from great_expectations.datasource.fluent.interfaces import Batch


class _Partitioner(PartitionerProtocol, Protocol): ...  # noqa: PYI046


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


class SparkPartitionerYear(_PartitionerDatetime):
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


class SparkPartitionerYearAndMonth(_PartitionerDatetime):
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


class SparkPartitionerYearAndMonthAndDay(_PartitionerDatetime):
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


class SparkPartitionerDatetimePart(_PartitionerDatetime):
    datetime_parts: List[str]
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"

    @property
    @override
    def param_names(self) -> List[str]:
        return self.datetime_parts

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "date_parts": self.param_names}

    @pydantic.validator("datetime_parts", each_item=True)
    def _check_param_name_allowed(cls, v: str):
        allowed_date_parts = [part.value for part in DatePart]
        assert (
            v in allowed_date_parts
        ), f"Only the following param_names are allowed: {allowed_date_parts}"
        return v


class _PartitionerOneColumnOneParam(FluentBaseModel):
    column_name: str
    method_name: str
    sort_ascending: bool = True

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        raise NotImplementedError


class SparkPartitionerDividedInteger(_PartitionerOneColumnOneParam):
    divisor: int
    column_name: str
    method_name: Literal["partition_on_divided_integer"] = "partition_on_divided_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["quotient"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "divisor": self.divisor}

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "quotient" not in options:
            raise ValueError("'quotient' must be specified in the batch parameters")  # noqa: TRY003
        return {self.column_name: options["quotient"]}


class SparkPartitionerModInteger(_PartitionerOneColumnOneParam):
    mod: int
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["remainder"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "mod": self.mod}

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "remainder" not in options:
            raise ValueError("'remainder' must be specified in the batch parameters")  # noqa: TRY003
        return {self.column_name: options["remainder"]}


class SparkPartitionerColumnValue(_PartitionerOneColumnOneParam):
    column_name: str
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"

    @property
    @override
    def param_names(self) -> List[str]:
        return [self.column_name]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if self.column_name not in options:
            raise ValueError(f"'{self.column_name}' must be specified in the batch parameters")  # noqa: TRY003
        return {self.column_name: options[self.column_name]}


class SparkPartitionerMultiColumnValue(FluentBaseModel):
    column_names: List[str]
    sort_ascending: bool = True
    method_name: Literal["partition_on_multi_column_values"] = "partition_on_multi_column_values"

    @property
    def columns(self):
        return self.column_names

    @property
    def param_names(self) -> List[str]:
        return self.column_names

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_names": self.column_names}

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if not (set(self.column_names) <= set(options.keys())):
            raise ValueError(  # noqa: TRY003
                f"All column names, {self.column_names}, must be specified in the batch parameters. "  # noqa: E501
                f" The options provided were f{options}."
            )
        return {col: options[col] for col in self.column_names}


# We create this type instead of using _Partitioner so pydantic can use to this to
# coerce the partitioner to the right type during deserialization from config.
SparkPartitioner = Union[
    SparkPartitionerColumnValue,
    SparkPartitionerMultiColumnValue,
    SparkPartitionerDividedInteger,
    SparkPartitionerModInteger,
    SparkPartitionerYear,
    SparkPartitionerYearAndMonth,
    SparkPartitionerYearAndMonthAndDay,
    SparkPartitionerDatetimePart,
]
