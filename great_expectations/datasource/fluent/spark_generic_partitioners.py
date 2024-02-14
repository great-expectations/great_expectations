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
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import BatchRequestOptions


class _Partitioner(Protocol):  # noqa: PYI046
    @property
    def columns(self) -> list[str]:
        """The names of the column used to partition the data"""

    @property
    def method_name(self) -> str:
        """Returns a partitioner method name.

        The possible values of partitioner method names are defined in the enum,
        great_expectations.execution_engine.partition_and_sample.data_partitioner.PartitionerMethod
        """

    @property
    def param_names(self) -> List[str]:
        """Returns the parameter names that specify a batch derived from this partitioner

        For example, for PartitionerYearMonth this returns ["year", "month"]. For more
        examples, please see Partitioner* classes below.
        """

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        """A shim to our spark execution engine partitioner methods

        We translate any internal _Partitioner state and what is passed in from
        a batch_request to the partitioner_kwargs required by our execution engine
        data partitioners defined in:
        great_expectations.execution_engine.partition_and_sample.sparkdf_data_partitioner

        Look at Partitioner* classes for concrete examples.
        """

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        """Translates `options` to the execution engine batch spec kwarg identifiers

        Arguments:
            options: A BatchRequest.options dictionary that specifies ALL the fields necessary
                     to specify a batch with respect to this partitioner.

        Returns:
            A dictionary that can be added to batch_spec_kwargs["batch_identifiers"].
            This has one of 2 forms:
              1. This category has many parameters are derived from 1 column.
                 These only are datetime partitioners and the batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name: {datepart_1: value, datepart_2: value, ...}
                 where datepart_* are strings like "year", "month", "day". The exact
                 fields depend on the partitioner.

              2. This category has only 1 parameter for each column.
                 This is used for all other partitioners and the batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name_1: value, column_name_2: value, ...}
                 where value is the value of the column after being processed by the partitioner.
                 For example, for the PartitionerModInteger where mod = 3,
                 {"passenger_count": 2}, means the raw passenger count value is in the set:
                 {2, 5, 8, ...} = {2*n + 1 | n is a nonnegative integer }
                 This category was only 1 parameter per column.
        """


class _PartitionerDatetime(FluentBaseModel):
    column_name: str
    method_name: str

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        """Validates all the datetime parameters for this partitioner exist in `options`."""
        identifiers: Dict = {}
        for part in self.param_names:
            if part not in options:
                raise ValueError(
                    f"'{part}' must be specified in the batch request options"
                )
            identifiers[part] = options[part]
        return {self.column_name: identifiers}

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError


class PartitionerYear(_PartitionerDatetime):
    column_name: str
    method_name: Literal["partition_on_year"] = "partition_on_year"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class PartitionerYearAndMonth(_PartitionerDatetime):
    column_name: str
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class PartitionerYearAndMonthAndDay(_PartitionerDatetime):
    column_name: str
    method_name: Literal[
        "partition_on_year_and_month_and_day"
    ] = "partition_on_year_and_month_and_day"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month", "day"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class PartitionerDatetimePart(_PartitionerDatetime):
    datetime_parts: List[str]
    column_name: str
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

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        raise NotImplementedError


class PartitionerDividedInteger(_PartitionerOneColumnOneParam):
    divisor: int
    column_name: str
    method_name: Literal[
        "partition_on_divided_integer"
    ] = "partition_on_divided_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["quotient"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "divisor": self.divisor}

    @override
    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "quotient" not in options:
            raise ValueError(
                "'quotient' must be specified in the batch request options"
            )
        return {self.column_name: options["quotient"]}


class PartitionerModInteger(_PartitionerOneColumnOneParam):
    mod: int
    column_name: str
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["remainder"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "mod": self.mod}

    @override
    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "remainder" not in options:
            raise ValueError(
                "'remainder' must be specified in the batch request options"
            )
        return {self.column_name: options["remainder"]}


class PartitionerColumnValue(_PartitionerOneColumnOneParam):
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
    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if self.column_name not in options:
            raise ValueError(
                f"'{self.column_name}' must be specified in the batch request options"
            )
        return {self.column_name: options[self.column_name]}


class PartitionerMultiColumnValue(FluentBaseModel):
    column_names: List[str]
    method_name: Literal[
        "partition_on_multi_column_values"
    ] = "partition_on_multi_column_values"

    @property
    def columns(self):
        return self.column_names

    @property
    def param_names(self) -> List[str]:
        return self.column_names

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_names": self.column_names}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if not (set(self.column_names) <= set(options.keys())):
            raise ValueError(
                f"All column names, {self.column_names}, must be specified in the batch request options. "
                f" The options provided were f{options}."
            )
        return {col: options[col] for col in self.column_names}


# We create this type instead of using _Partitioner so pydantic can use to this to
# coerce the partitioner to the right type during deserialization from config.
Partitioner = Union[
    PartitionerColumnValue,
    PartitionerMultiColumnValue,
    PartitionerDividedInteger,
    PartitionerModInteger,
    PartitionerYear,
    PartitionerYearAndMonth,
    PartitionerYearAndMonthAndDay,
    PartitionerDatetimePart,
]
