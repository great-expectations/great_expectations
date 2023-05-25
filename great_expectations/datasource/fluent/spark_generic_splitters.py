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

import pydantic

from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
)
from great_expectations.execution_engine.split_and_sample.data_splitter import DatePart

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import BatchRequestOptions


class _Splitter(Protocol):
    @property
    def columns(self) -> list[str]:
        """The names of the column used to split the data"""

    @property
    def method_name(self) -> str:
        """Returns a splitter method name.

        The possible values of splitter method names are defined in the enum,
        great_expectations.execution_engine.split_and_sample.data_splitter.SplitterMethod
        """

    @property
    def param_names(self) -> List[str]:
        """Returns the parameter names that specify a batch derived from this splitter

        For example, for SplitterYearMonth this returns ["year", "month"]. For more
        examples, please see Splitter* classes below.
        """

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        """A shim to our spark execution engine splitter methods

        We translate any internal _Splitter state and what is passed in from
        a batch_request to the splitter_kwargs required by our execution engine
        data splitters defined in:
        great_expectations.execution_engine.split_and_sample.sparkdf_data_splitter

        Look at Splitter* classes for concrete examples.
        """

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        """Translates `options` to the execution engine batch spec kwarg identifiers

        Arguments:
            options: A BatchRequest.options dictionary that specifies ALL the fields necessary
                     to specify a batch with respect to this splitter.

        Returns:
            A dictionary that can be added to batch_spec_kwargs["batch_identifiers"].
            This has one of 2 forms:
              1. This category has many parameters are derived from 1 column.
                 These only are datetime splitters and the batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name: {datepart_1: value, datepart_2: value, ...}
                 where datepart_* are strings like "year", "month", "day". The exact
                 fields depend on the splitter.

              2. This category has only 1 parameter for each column.
                 This is used for all other splitters and the batch_spec_kwargs["batch_identifiers"]
                 look like:
                   {column_name_1: value, column_name_2: value, ...}
                 where value is the value of the column after being processed by the splitter.
                 For example, for the SplitterModInteger where mod = 3,
                 {"passenger_count": 2}, means the raw passenger count value is in the set:
                 {2, 5, 8, ...} = {2*n + 1 | n is a nonnegative integer }
                 This category was only 1 parameter per column.
        """


class _SplitterDatetime(FluentBaseModel):
    column_name: str
    method_name: str

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        """Validates all the datetime parameters for this splitter exist in `options`."""
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

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError


class SplitterYear(_SplitterDatetime):
    column_name: str
    method_name: Literal["split_on_year"] = "split_on_year"

    @property
    def param_names(self) -> List[str]:
        return ["year"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SplitterYearAndMonth(_SplitterDatetime):
    column_name: str
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"

    @property
    def param_names(self) -> List[str]:
        return ["year", "month"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SplitterYearAndMonthAndDay(_SplitterDatetime):
    column_name: str
    method_name: Literal[
        "split_on_year_and_month_and_day"
    ] = "split_on_year_and_month_and_day"

    @property
    def param_names(self) -> List[str]:
        return ["year", "month", "day"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SplitterDatetimePart(_SplitterDatetime):
    datetime_parts: List[str]
    column_name: str
    method_name: Literal["split_on_date_parts"] = "split_on_date_parts"

    @property
    def param_names(self) -> List[str]:
        return self.datetime_parts

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "date_parts": self.param_names}

    @pydantic.validator("datetime_parts", each_item=True)
    def _check_param_name_allowed(cls, v: str):
        allowed_date_parts = [part.value for part in DatePart]
        assert (
            v in allowed_date_parts
        ), f"Only the following param_names are allowed: {allowed_date_parts}"
        return v


class _SplitterOneColumnOneParam(FluentBaseModel):
    column_name: str
    method_name: str

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        raise NotImplementedError


class SplitterDividedInteger(_SplitterOneColumnOneParam):
    divisor: int
    column_name: str
    method_name: Literal["split_on_divided_integer"] = "split_on_divided_integer"

    @property
    def param_names(self) -> List[str]:
        return ["quotient"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "divisor": self.divisor}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "quotient" not in options:
            raise ValueError(
                "'quotient' must be specified in the batch request options"
            )
        return {self.column_name: options["quotient"]}


class SplitterModInteger(_SplitterOneColumnOneParam):
    mod: int
    column_name: str
    method_name: Literal["split_on_mod_integer"] = "split_on_mod_integer"

    @property
    def param_names(self) -> List[str]:
        return ["remainder"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "mod": self.mod}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "remainder" not in options:
            raise ValueError(
                "'remainder' must be specified in the batch request options"
            )
        return {self.column_name: options["remainder"]}


class SplitterColumnValue(_SplitterOneColumnOneParam):
    column_name: str
    method_name: Literal["split_on_column_value"] = "split_on_column_value"

    @property
    def param_names(self) -> List[str]:
        return [self.column_name]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if self.column_name not in options:
            raise ValueError(
                f"'{self.column_name}' must be specified in the batch request options"
            )
        return {self.column_name: options[self.column_name]}


class SplitterMultiColumnValue(FluentBaseModel):
    column_names: List[str]
    method_name: Literal[
        "split_on_multi_column_values"
    ] = "split_on_multi_column_values"

    @property
    def columns(self):
        return self.column_names

    @property
    def param_names(self) -> List[str]:
        return self.column_names

    def splitter_method_kwargs(self) -> Dict[str, Any]:
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


# We create this type instead of using _Splitter so pydantic can use to this to
# coerce the splitter to the right type during deserialization from config.
Splitter = Union[
    SplitterColumnValue,
    SplitterMultiColumnValue,
    SplitterDividedInteger,
    SplitterModInteger,
    SplitterYear,
    SplitterYearAndMonth,
    SplitterYearAndMonthAndDay,
    SplitterDatetimePart,
]
