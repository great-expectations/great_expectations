from __future__ import annotations

import copy
import logging
import warnings
from datetime import date, datetime
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Collection,
    Dict,
    Final,
    Generic,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Annotated, Never, Self

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import deprecated_argument, public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.batch_spec import (
    BatchSpec,
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    PartitionerColumnValue,
    PartitionerConvertedDatetime,
    PartitionerDatetimePart,
    PartitionerDividedInteger,
    PartitionerModInteger,
    PartitionerMultiColumnValue,
)
from great_expectations.datasource.fluent.batch_parameter_normalization import (
    is_digit_string,
    normalize_batch_parameters,
    numeric_parameter_names_of,
)
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest,
)
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME
from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.interfaces import (
    Batch,
    DataAsset,
    Datasource,
    DatasourceT,
    GxDatasourceWarning,
    PartitionerProtocol,
    TestConnectionError,
)
from great_expectations.exceptions.exceptions import (
    NoAvailableBatchesError,
    SqlAddBatchDefinitionError,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner import (
    SqlAlchemyDataPartitioner,
)

if TYPE_CHECKING:
    # We re-import sqlalchemy here to make type-checking and our compatability layer
    # play nice with one another
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.fluent import BatchParameters
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class Missing:
    """Sentinel used to distinguish "not provided" from an explicit None.

    Implemented as a singleton with custom copy/deepcopy behavior so that
    Pydantic V1's deepcopy of field defaults preserves identity checks (is).
    """

    _instance: Missing | None = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance  # type: ignore[return-value] # singleton

    def __copy__(self) -> Self:
        return self

    def __deepcopy__(self, memo: dict) -> Self:
        return self

    @classmethod
    def __get_validators__(cls) -> Iterator[Callable[..., Any]]:
        yield cls._validate

    @classmethod
    def _validate(cls, v: Any) -> Missing:
        if isinstance(v, cls):
            return v
        raise ValueError("Expected Missing sentinel")  # noqa: TRY003  # not very re-usable


MISSING: Final[Missing] = Missing()

DEFAULT_INITIAL_QUOTE_CHARACTERS: Final[Tuple[str, str, str, str]] = ('"', "'", "`", "[")
DEFAULT_FINAL_QUOTE_CHARACTERS: Final[Mapping[str, str]] = {
    '"': '"',
    "'": "'",
    "`": "`",
    "[": "]",
}


@overload
def to_lower_if_not_quoted(value: str, quote_characters: Sequence[str] = ...) -> str: ...


@overload
def to_lower_if_not_quoted(value: None, quote_characters: Sequence[str] = ...) -> None: ...


def to_lower_if_not_quoted(
    value: str | None,
    quote_characters: Sequence[str] = DEFAULT_INITIAL_QUOTE_CHARACTERS,
) -> str | None:
    """
    Convert a string to lowercase if it is not enclosed in quotes.
    """
    if not value:
        return value
    for char in quote_characters:
        if value.startswith(char) and value.endswith(DEFAULT_FINAL_QUOTE_CHARACTERS[char]):
            LOGGER.warning(
                f"The {value} string is bracketed by quotes,"
                " so it will not be converted to lowercase."
                " May cause sqlalchemy case-sensitivity issues."
            )
            return value
    LOGGER.info(f"Setting {value} to lowercase to ensure sqlalchemy case-insensitivity.")
    return value.lower()


class SQLDatasourceError(Exception):
    pass


class SQLAlchemyCreateEngineError(SQLDatasourceError):
    """
    An error creating a SQLAlchemy `Engine` object.

    Not to be confused with the GX `SQLAlchemyExecutionEngine`.
    """

    @overload
    def __init__(self, addendum: str | None = ..., cause: Exception = ...): ...

    @overload
    def __init__(self, addendum: str = ..., cause: Exception | None = ...): ...

    def __init__(
        self,
        addendum: str | None = None,
        cause: Exception | None = None,
    ):
        """Must provide a `cause`, `addendum`, or both."""
        message = "Unable to create SQLAlchemy Engine"
        if cause:
            message += f": due to {cause!r}"
        if addendum:
            message += f": {addendum}"
        super().__init__(message)


class _Partitioner(PartitionerProtocol, Protocol):
    def param_defaults(self, sql_asset: _SQLAsset) -> List[Dict]:
        """Creates all valid batch requests options for sql_asset

        This can be implemented by querying the data defined in the sql_asset to generate
        all the possible parameter values for the BatchRequest.options that will return data.
        For example for a YearMonth partitioner, we can query the underlying data to return the
        set of distinct (year, month) pairs. We would then return a list of BatchRequest.options,
        ie dictionaries, of the form {"year": year, "month": month} that contain all these distinct
        pairs.
        """
        ...


def _partitioner_and_sql_asset_to_batch_identifier_data(
    partitioner: _Partitioner, asset: _SQLAsset
) -> list[dict]:
    execution_engine = asset.datasource.get_execution_engine()
    sqlalchemy_data_partitioner = SqlAlchemyDataPartitioner(execution_engine.dialect_name)
    return sqlalchemy_data_partitioner.get_data_for_batch_identifiers(
        execution_engine=execution_engine,
        selectable=asset.as_selectable(),
        partitioner_method_name=partitioner.method_name,
        partitioner_kwargs=partitioner.partitioner_method_kwargs(),
    )


class _PartitionerDatetime(FluentBaseModel):
    column_name: str
    method_name: str
    sort_ascending: bool = True

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    def param_defaults(self, sql_asset: _SQLAsset) -> list[dict]:
        batch_identifier_data = _partitioner_and_sql_asset_to_batch_identifier_data(
            partitioner=self, asset=sql_asset
        )
        params: list[dict] = []
        for identifer_data in batch_identifier_data:
            params.append(identifer_data[self.column_name])
        return params

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        """Validates all the datetime parameters for this partitioner exist in `options`."""
        identifiers: Dict = {}
        for part in self.param_names:
            if part not in options:
                raise ValueError(f"'{part}' must be specified in the batch parameters")  # noqa: TRY003 # FIXME CoP
            identifiers[part] = options[part]
        return {self.column_name: identifiers}

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError


class SqlPartitionerYear(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year"] = "partition_on_year"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year"]

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SqlPartitionerYearAndMonth(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month"] = "partition_on_year_and_month"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month"]

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SqlPartitionerYearAndMonthAndDay(_PartitionerDatetime):
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_year_and_month_and_day"] = (
        "partition_on_year_and_month_and_day"
    )

    @property
    @override
    def param_names(self) -> List[str]:
        return ["year", "month", "day"]

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}


class SqlPartitionerDatetimePart(_PartitionerDatetime):
    datetime_parts: List[str]
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_date_parts"] = "partition_on_date_parts"

    @property
    @override
    def param_names(self) -> List[str]:
        return self.datetime_parts

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "date_parts": self.param_names}

    @pydantic.validator("datetime_parts", each_item=True)
    def _check_param_name_allowed(cls, v: str):
        allowed_date_parts = [part.value for part in DatePart]
        assert v in allowed_date_parts, (
            f"Only the following param_names are allowed: {allowed_date_parts}"
        )
        return v


class _PartitionerOneColumnOneParam(FluentBaseModel):
    column_name: str
    method_name: str
    sort_ascending: bool = True

    @property
    def columns(self) -> list[str]:
        return [self.column_name]

    def param_defaults(self, sql_asset: _SQLAsset) -> list[dict]:
        batch_identifier_data = _partitioner_and_sql_asset_to_batch_identifier_data(
            partitioner=self, asset=sql_asset
        )
        params: list[dict] = []
        for identifer_data in batch_identifier_data:
            params.append({self.param_names[0]: identifer_data[self.column_name]})
        return params

    @property
    def param_names(self) -> list[str]:
        raise NotImplementedError

    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError

    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        raise NotImplementedError


class SqlPartitionerDividedInteger(_PartitionerOneColumnOneParam):
    divisor: int
    column_name: str
    method_name: Literal["partition_on_divided_integer"] = "partition_on_divided_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["quotient"]

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "divisor": self.divisor}

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "quotient" not in options:
            raise ValueError("'quotient' must be specified in the batch parameters")  # noqa: TRY003 # FIXME CoP
        return {self.column_name: options["quotient"]}


class SqlPartitionerModInteger(_PartitionerOneColumnOneParam):
    mod: int
    column_name: str
    method_name: Literal["partition_on_mod_integer"] = "partition_on_mod_integer"

    @property
    @override
    def param_names(self) -> List[str]:
        return ["remainder"]

    @property
    def numeric_param_names(self) -> List[str]:
        return self.param_names

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "mod": self.mod}

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "remainder" not in options:
            raise ValueError("'remainder' must be specified in the batch parameters")  # noqa: TRY003 # FIXME CoP
        return {self.column_name: options["remainder"]}


class SqlPartitionerColumnValue(_PartitionerOneColumnOneParam):
    column_name: str
    method_name: Literal["partition_on_column_value"] = "partition_on_column_value"

    # Deliberately does not declare numeric_param_names: the parameter name is the
    # column name itself, so a string column literally named "year" would produce a
    # "year" key. Declaring nothing here keeps that value from ever being coerced.

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
            raise ValueError(f"'{self.column_name}' must be specified in the batch parameters")  # noqa: TRY003 # FIXME CoP
        return {self.column_name: options[self.column_name]}

    @override
    def param_defaults(self, sql_asset: _SQLAsset) -> list[dict]:
        # The superclass version of param_defaults is correct, but here we leverage that
        # the parameter name is the same as the column name to make this much faster.
        return _partitioner_and_sql_asset_to_batch_identifier_data(
            partitioner=self, asset=sql_asset
        )


class SqlPartitionerMultiColumnValue(FluentBaseModel):
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
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                f"All column names, {self.column_names}, must be specified in the batch parameters. "  # noqa: E501 # FIXME CoP
                f" The options provided were f{options}."
            )
        return {col: options[col] for col in self.column_names}

    def param_defaults(self, sql_asset: _SQLAsset) -> list[dict]:
        return _partitioner_and_sql_asset_to_batch_identifier_data(
            partitioner=self, asset=sql_asset
        )


class SqlitePartitionerConvertedDateTime(_PartitionerOneColumnOneParam):
    """A partitioner than can be used for sql engines that represents datetimes as strings.

    The SQL engine that this currently supports is SQLite since it stores its datetimes as
    strings.
    The DatetimePartitioner will also work for SQLite and may be more intuitive.
    """

    # date_format_strings syntax is documented here:
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    # It allows for arbitrary strings so can't be validated until conversion time.
    date_format_string: str
    column_name: str
    sort_ascending: bool = True
    method_name: Literal["partition_on_converted_datetime"] = "partition_on_converted_datetime"

    # Deliberately does not declare numeric_param_names: "datetime" carries a
    # string-formatted datetime value, not a bare integer, so it must never be coerced.

    @property
    @override
    def param_names(self) -> List[str]:
        # The datetime parameter will be a string representing a datetime in the format
        # given by self.date_format_string.
        return ["datetime"]

    @override
    def partitioner_method_kwargs(self) -> Dict[str, Any]:
        return {
            "column_name": self.column_name,
            "date_format_string": self.date_format_string,
        }

    @override
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "datetime" not in options:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "'datetime' must be specified in the batch parameters to create a batch identifier"
            )
        return {self.column_name: options["datetime"]}


# We create this type instead of using _Partitioner so pydantic can use to this to
# coerce the partitioner to the right type during deserialization from config.
SqlPartitioner = Union[
    SqlPartitionerColumnValue,
    SqlPartitionerMultiColumnValue,
    SqlPartitionerDividedInteger,
    SqlPartitionerModInteger,
    SqlPartitionerYear,
    SqlPartitionerYearAndMonth,
    SqlPartitionerYearAndMonthAndDay,
    SqlPartitionerDatetimePart,
    SqlitePartitionerConvertedDateTime,
]


class _NoMatchDiagnostics(NamedTuple):
    """What the single candidate pass in `_fully_specified_batch_requests` learned,
    carried forward so a no-match error can be composed without re-running the
    (live, DB-backed) candidate query at the raise site."""

    candidate_count: int
    offending_param: Optional[Tuple[str, Any]]


def _first_numerically_uninterpretable_param(
    options: Optional[BatchParameters], numeric_param_names: Collection[str]
) -> Optional[Tuple[str, Any]]:
    """The first requested numeric-parameter value that is a string but not a digit
    string (e.g. "202O"), so it cannot be interpreted as an integer. None when every
    requested numeric parameter is either absent or already numerically interpretable.
    """
    if not options:
        return None
    for name in sorted(numeric_param_names):
        if name not in options:
            continue
        value = options[name]
        if isinstance(value, str) and not is_digit_string(value):
            return name, value
    return None


def _compose_no_available_batches_message(
    diagnostics: _NoMatchDiagnostics, options: Optional[BatchParameters]
) -> str:
    """Diagnostic text for a no-match raise.

    Zero candidates (an empty table or column) is distinguished from candidates
    existing but none matching: the latter always names the candidate count and the
    requested options, since a bare "no batches found" leaves a user unable to tell
    whether the data is absent or their parameters were malformed. When a numeric
    parameter's value is a string that cannot be interpreted as an integer (e.g. a
    typo like "202O"), that is very likely the actual explanation, so it's named
    on top of the candidate-count text.
    """
    if diagnostics.candidate_count == 0:
        return (
            "No available batches found: no candidate batches exist for this asset "
            "(e.g. an empty table or column)."
        )
    message = (
        f"No available batches found: {diagnostics.candidate_count} candidate batch(es) "
        f"were checked against the requested options {options!r}, but none matched."
    )
    if diagnostics.offending_param is not None:
        name, value = diagnostics.offending_param
        message += (
            f" Parameter {name!r} has value {value!r}, which cannot be interpreted "
            "as an integer; pass an integer instead."
        )
    return message


@public_api
class _SQLAsset(DataAsset[DatasourceT, ColumnPartitioner], Generic[DatasourceT]):
    """A _SQLAsset Mixin

    This is used as a mixin for _SQLAsset subclasses to give them the TableAsset functionality
    that can be used by different SQL datasource subclasses.

    For example see TableAsset defined in this module and SqliteTableAsset defined in
    sqlite_datasource.py
    """

    # Instance fields
    type: str = pydantic.Field("_sql_asset")
    name: str
    _partitioner_implementation_map: Dict[
        Type[ColumnPartitioner], Optional[Type[SqlPartitioner]]
    ] = pydantic.PrivateAttr(
        default={
            ColumnPartitionerYearly: SqlPartitionerYear,
            ColumnPartitionerMonthly: SqlPartitionerYearAndMonth,
            ColumnPartitionerDaily: SqlPartitionerYearAndMonthAndDay,
            PartitionerColumnValue: SqlPartitionerColumnValue,
            PartitionerDatetimePart: SqlPartitionerDatetimePart,
            PartitionerDividedInteger: SqlPartitionerDividedInteger,
            PartitionerModInteger: SqlPartitionerModInteger,
            PartitionerMultiColumnValue: SqlPartitionerMultiColumnValue,
            PartitionerConvertedDatetime: None,  # only implemented for sqlite backend
        }
    )

    def get_partitioner_implementation(
        self, abstract_partitioner: ColumnPartitioner
    ) -> SqlPartitioner:
        PartitionerClass = self._partitioner_implementation_map.get(type(abstract_partitioner))
        if not PartitionerClass:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                f"Requested Partitioner `{abstract_partitioner.method_name}` is not implemented for this DataAsset. "  # noqa: E501 # FIXME CoP
            )
        assert PartitionerClass is not None
        return PartitionerClass(**abstract_partitioner.dict())

    @override
    def get_batch_parameters_keys(
        self,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> tuple[str, ...]:
        option_keys: Tuple[str, ...] = tuple()
        if partitioner:
            sql_partitioner = self.get_partitioner_implementation(partitioner)
            option_keys += tuple(sql_partitioner.param_names)
        return option_keys

    @override
    def test_connection(self) -> None:
        pass

    @staticmethod
    def _matches_request_options(candidate: Dict, requested_options: BatchParameters) -> bool:
        for k, v in requested_options.items():
            if isinstance(candidate[k], (datetime, date)):
                candidate[k] = str(candidate[k])

            if v is not None and candidate[k] != v:
                return False
        return True

    def _fully_specified_batch_requests(
        self, batch_request: BatchRequest
    ) -> Tuple[List[BatchRequest], _NoMatchDiagnostics]:
        """Populates a batch requests unspecified params producing a list of batch requests.

        Also returns diagnostics describing the single candidate pass below, so a caller
        that ends up with no fully-specified requests can compose a no-match error
        without re-running the (live, DB-backed) candidate query.
        """

        if batch_request.partitioner is None:
            # Currently batch_request.options is complete determined by the presence of a
            # partitioner. If partitioner is None, then there are no specifiable options
            # so we return early. Since the passed in batch_request is verified, it must be the
            # empty, ie {}.
            # In the future, if there are options that are not determined by the partitioner
            # this check will have to be generalized.
            # The request itself is the only candidate, and it always matches, so this
            # count describes the returned list rather than standing in for one.
            return [batch_request], _NoMatchDiagnostics(
                candidate_count=len([batch_request]), offending_param=None
            )

        sql_partitioner = self.get_partitioner_implementation(batch_request.partitioner)
        numeric_param_names = numeric_parameter_names_of(sql_partitioner)

        candidates = list(sql_partitioner.param_defaults(self))

        batch_requests: List[BatchRequest] = []
        # We iterate through all possible batches as determined by the partitioner
        for params in candidates:
            # If the params from the partitioner don't match the batch parameters
            # we don't create this batch.
            if not _SQLAsset._matches_request_options(params, batch_request.options):
                continue
            options = copy.deepcopy(batch_request.options)
            options.update(params)
            batch_requests.append(
                BatchRequest(
                    datasource_name=batch_request.datasource_name,
                    data_asset_name=batch_request.data_asset_name,
                    options=options,
                    partitioner=batch_request.partitioner,
                )
            )

        diagnostics = _NoMatchDiagnostics(
            candidate_count=len(candidates),
            offending_param=_first_numerically_uninterpretable_param(
                batch_request.options, numeric_param_names
            ),
        )
        return batch_requests, diagnostics

    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        self._validate_batch_request(batch_request)
        if batch_request.partitioner:
            sql_partitioner = self.get_partitioner_implementation(batch_request.partitioner)
        else:
            sql_partitioner = None

        requests, _diagnostics = self._fully_specified_batch_requests(batch_request)
        metadata_dicts = [self._get_batch_metadata_from_batch_request(r) for r in requests]

        if sql_partitioner:
            metadata_dicts = self.sort_batch_identifiers_list(metadata_dicts, sql_partitioner)

        return metadata_dicts[batch_request.batch_slice]

    @override
    def get_batch(self, batch_request: BatchRequest) -> Batch:
        """Batch that matches the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                build_batch_request on the asset.

        Returns:
            A list Batch that matches the options specified in the batch request.
        """
        self._validate_batch_request(batch_request)

        if batch_request.partitioner:
            sql_partitioner = self.get_partitioner_implementation(batch_request.partitioner)
        else:
            sql_partitioner = None

        batch_spec_kwargs: Dict[str, str | dict | None]
        requests, diagnostics = self._fully_specified_batch_requests(batch_request)
        unsorted_metadata_dicts = [self._get_batch_metadata_from_batch_request(r) for r in requests]

        if not unsorted_metadata_dicts:
            raise NoAvailableBatchesError(
                _compose_no_available_batches_message(diagnostics, batch_request.options)
            )

        if sql_partitioner:
            sorted_metadata_dicts = self.sort_batch_identifiers_list(
                unsorted_metadata_dicts, sql_partitioner
            )
        else:
            sorted_metadata_dicts = unsorted_metadata_dicts

        sorted_metadata_dicts = sorted_metadata_dicts[batch_request.batch_slice]
        batch_metadata = sorted_metadata_dicts[-1]

        # we've sorted the metadata, but not the requests, so we need the index of our
        # batch_metadata from the original unsorted list so that we get the right request
        request_index = unsorted_metadata_dicts.index(batch_metadata)

        request = requests[request_index]
        batch_spec_kwargs = self._create_batch_spec_kwargs()
        if sql_partitioner:
            batch_spec_kwargs["partitioner_method"] = sql_partitioner.method_name
            batch_spec_kwargs["partitioner_kwargs"] = sql_partitioner.partitioner_method_kwargs()
            # mypy infers that batch_spec_kwargs["batch_identifiers"] is a collection, but
            # it is hardcoded to a dict above, so we cast it here.
            cast("Dict", batch_spec_kwargs["batch_identifiers"]).update(
                sql_partitioner.batch_parameters_to_batch_spec_kwarg_identifiers(request.options)
            )
        # Creating the batch_spec is our hook into the execution engine.
        batch_spec = self._create_batch_spec(batch_spec_kwargs)
        execution_engine: SqlAlchemyExecutionEngine = self.datasource.get_execution_engine()
        data, markers = execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)

        batch_definition = LegacyBatchDefinition(
            datasource_name=self.datasource.name,
            data_connector_name=_DATA_CONNECTOR_NAME,
            data_asset_name=self.name,
            batch_identifiers=IDDict(batch_spec["batch_identifiers"]),
            batch_spec_passthrough=None,
        )

        return Batch(
            datasource=self.datasource,
            data_asset=self,
            batch_request=request,
            data=data,
            metadata=batch_metadata,
            batch_markers=markers,
            batch_spec=batch_spec,
            batch_definition=batch_definition,
        )

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to filter the batch groups returned from the asset.
                The dict structure depends on the asset type. The available keys for dict can be obtained by
                calling batch_parameters. Numeric batch parameters (e.g. year, month, day) take integer
                values; digit-string values are still accepted but are deprecated and emit a warning,
                with support for them removed in 2.0.
            batch_slice: A python slice that can be used to limit the sorted batches by index.
                e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.
            partitioner: A Partitioner used to narrow the data returned from the asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch from an Asset by calling the
            get_batch method.
        """  # noqa: E501 # FIXME CoP
        # Resolving the partitioner is what reports an unimplemented kind, so it stays
        # behind the same guard the rest of this method uses: with no values to coerce
        # there is nothing to classify, and building a request has never been the step
        # that rejects a partitioner the asset cannot implement.
        if options and partitioner is not None:
            sql_partitioner = self.get_partitioner_implementation(partitioner)
            numeric_param_names = numeric_parameter_names_of(sql_partitioner)
            options = normalize_batch_parameters(options, numeric_param_names)

        if options is not None and not self._batch_parameters_are_valid(
            options=options, partitioner=partitioner
        ):
            allowed_keys = set(self.get_batch_parameters_keys(partitioner=partitioner))
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003 # FIXME CoP
                "batch parameters should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
            batch_slice=batch_slice,
            partitioner=partitioner,
        )

    @override
    def add_batch_definition(
        self,
        name: str,
        partitioner: Optional[ColumnPartitioner] = None,
        validate_partitioner: bool = True,
    ) -> BatchDefinition[ColumnPartitioner]:
        if validate_partitioner and partitioner:
            self.validate_batch_definition(partitioner)
        return super().add_batch_definition(name, partitioner)

    @public_api
    def validate_batch_definition(self, partitioner: ColumnPartitioner) -> None:
        """Validates that the Batch Definition column is of a permissible type

        This isn't meant to be called directly. This is called internally when a Batch Definition
         is added. Data asset implementers can override this for their specific data asset.

        Raises:
            SqlAddBatchDefinitionError: The specified column to partition on is not of
                a permissible type for batching (ie date or datetime) or no data is
                present in this column.
        """
        # We only support certain partitioners for using as batch definitions.
        assert isinstance(
            partitioner,
            (
                ColumnPartitionerYearly,
                ColumnPartitionerMonthly,
                ColumnPartitionerDaily,
            ),
        )
        # A _SQLAsset must have a SQLDatasource
        assert isinstance(self.datasource, SQLDatasource)
        engine: sqlalchemy.Engine = self.datasource.get_engine()

        # It would be better to introspect the database types and see which ones map to date or
        # datetime. However, 3rd party types, such as Snowflakes TIMESTAMP_NTZ haven't implemented
        # all the sqlalchemy abstract methods such as `python_type`.
        # To make this more concrete I would have liked to do something like:
        # insp = sqlalchemy.inspect(self.datasource.get_engine())
        # cols = insp.get_columns(self.table_name, self.schema_name)
        # for col in cols:
        #     pytype = col['type'].python_type
        #
        # Instead we query the db for a non-null value to see if sqlalchemy converts
        # this value to a python date or datetime. This means we REQUIRE that data is
        # present for this validation to work.
        with engine.connect() as connection:
            selectable: sqlalchemy.Selectable = self.as_selectable()
            column: sqlalchemy.ColumnClause[Never] = sa.sql.column(partitioner.column_name)
            try:
                row = connection.execute(
                    sa.select(column, selectable).limit(1)  # type: ignore[call-overload]  # sqlalchemy typing is missing variants
                )
            except Exception as query_error:
                raise SqlAddBatchDefinitionError(
                    msg=f"Attempt to read an example non-null '{column}' value from '{selectable}'"
                    " failed so column type can't be verified to be a date or datetime."
                ) from query_error

            r = row.first()
            if not r or not isinstance(getattr(r, partitioner.column_name), (datetime, date)):
                raise SqlAddBatchDefinitionError(
                    msg=f"'{column}' column from '{selectable}' is not a date or datetime type."
                )

    @public_api
    def add_batch_definition_whole_table(self, name: str) -> BatchDefinition:
        """Adds a whole table Batch Definition to this Data Asset

        Args:
            name: The name of the Batch Definition to be added

        Returns:
            The added BatchDefinition object.
        """
        return self.add_batch_definition(
            name=name,
            partitioner=None,
        )

    @public_api
    def add_batch_definition_yearly(
        self,
        name: str,
        column: str,
        sort_ascending: bool = True,
        validate_batchable: bool = True,
    ) -> BatchDefinition:
        """Adds a yearly Batch Definition to this Data Asset

        Args:
            name: The name of the Batch Definition to be added.
            column: The column name on which to partition the asset by year.
            sort_ascending: Boolean to indicate whether to sort ascending (default) or descending.
                When running a validation, we default to running the last Batch Definition
                if one is not explicitly specified.

        Returns:
            The added BatchDefinition object.
        """

        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerYearly(
                method_name="partition_on_year", column_name=column, sort_ascending=sort_ascending
            ),
            validate_partitioner=validate_batchable,
        )

    @public_api
    def add_batch_definition_monthly(
        self,
        name: str,
        column: str,
        sort_ascending: bool = True,
        validate_batchable: bool = True,
    ) -> BatchDefinition:
        """Adds a monthly Batch Definition to this Data Asset

        Args:
            name: The name of the Batch Definition to be added
            column: The column name on which to partition the asset by month
            sort_ascending: Boolean to indicate whether to sort ascending (default) or descending.
                When running a validation, we default to running the last Batch Definition
                if one is not explicitly specified.

        Returns:
            The added BatchDefinition object.
        """

        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerMonthly(
                method_name="partition_on_year_and_month",
                column_name=column,
                sort_ascending=sort_ascending,
            ),
            validate_partitioner=validate_batchable,
        )

    @public_api
    def add_batch_definition_daily(
        self,
        name: str,
        column: str,
        sort_ascending: bool = True,
        validate_batchable: bool = True,
    ) -> BatchDefinition:
        """Adds a daily Batch Definition to this Data Asset

        Args:
            name: The name of the Batch Definition to be added
            column: The column name on which to partition the asset by day
            sort_ascending: Boolean to indicate whether to sort ascending (default) or descending.
                When running a validation, we default to running the last Batch Definition
                if one is not explicitly specified.

        Returns:
            The added BatchDefinition object.
        """

        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerDaily(
                method_name="partition_on_year_and_month_and_day",
                column_name=column,
                sort_ascending=sort_ascending,
            ),
            validate_partitioner=validate_batchable,
        )

    @override
    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and self._batch_parameters_are_valid(
                options=batch_request.options,
                partitioner=batch_request.partitioner,
            )
        ):
            options = dict.fromkeys(
                self.get_batch_parameters_keys(partitioner=batch_request.partitioner)
            )
            expect_batch_request_form = BatchRequest[ColumnPartitioner](
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=options,
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined] # FIXME CoP
                partitioner=batch_request.partitioner,
            )
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003 # FIXME CoP
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )

    def _create_batch_spec_kwargs(self) -> Dict[str, Any]:
        """Creates batch_spec_kwargs used to instantiate a SqlAlchemyDatasourceBatchSpec or RuntimeQueryBatchSpec

        This is called by get_batch to generate the batch.

        Returns:
            A dictionary that will be passed to self._create_batch_spec(**returned_dict)
        """  # noqa: E501 # FIXME CoP
        raise NotImplementedError

    def _create_batch_spec(self, batch_spec_kwargs: dict) -> BatchSpec:
        """
        Instantiates a SqlAlchemyDatasourceBatchSpec or RuntimeQueryBatchSpec.
        """
        raise NotImplementedError

    def as_selectable(self) -> sqlalchemy.Selectable:
        """Returns a Selectable that can be used to query this data

        Returns:
            A Selectable that can be used in a from clause to query this data
        """
        raise NotImplementedError


@public_api
class QueryAsset(_SQLAsset):
    """An asset made from a SQL query

    Args:
        query: The query to be used to construct the underlying Data Asset
    """

    # Instance fields
    type: Literal["query"] = "query"
    query: str

    @pydantic.validator("query")
    def query_must_start_with_select(cls, v: str):
        query = v.lstrip()
        if not (query.upper().startswith("SELECT") and query[6].isspace()):
            raise ValueError("query must start with 'SELECT' followed by a whitespace.")  # noqa: TRY003 # FIXME CoP
        return v

    @override
    def as_selectable(self) -> sqlalchemy.Selectable:
        """Returns the Selectable that is used to retrieve the data.

        This can be used in a subselect FROM clause for queries against this data.
        """
        return sa.select(sa.text(self.query.lstrip()[6:])).subquery()

    @override
    def _create_batch_spec_kwargs(self) -> Dict[str, Any]:
        return {
            "data_asset_name": self.name,
            "query": self.query,
            "temp_table_schema_name": None,
            "batch_identifiers": {},
        }

    @override
    def _create_batch_spec(self, batch_spec_kwargs: dict) -> RuntimeQueryBatchSpec:
        return RuntimeQueryBatchSpec(**batch_spec_kwargs)


@deprecated_argument(
    argument_name="schema_name",
    version="1.14.0",
    message="Pass the schema in your datasource's connection configuration instead.",
)
@public_api
class TableAsset(_SQLAsset):
    """A class representing a table from a SQL database

    Args:
        table_name: The name of the database table to be added
        schema_name: The name of the schema containing the database table to be added.
    """

    # Instance fields
    type: Literal["table"] = "table"
    # TODO: quoted_name or str
    table_name: str = pydantic.Field(
        "",
        description="Name of the SQL table. Will default to the value of `name` if not provided.",
    )
    schema_name: Union[str, Missing, None] = MISSING

    _quote_character: Optional[str] = None

    @pydantic.validator("schema_name", pre=True, always=True)
    @classmethod
    def _schema_name_deprecation_warning(cls, v: str | Missing | None) -> str | Missing | None:
        if v is MISSING:
            return v
        # deprecated-v1.14.0
        warnings.warn(
            "`schema_name` is deprecated."
            " Pass the schema in your datasource's connection configuration instead.",
            category=DeprecationWarning,
        )
        return v

    @property
    def _effective_schema_name(self) -> str | None:
        """Returns the schema to use based on schema_name (Union[str, Missing, None]):

        - MISSING: not provided, fall back to the datasource schema
        - str: explicitly provided, normalize and return
        - None: explicitly set to None, meaning no schema
        """
        if self.schema_name is MISSING:
            try:
                datasource: SQLDatasource = self.datasource
            except AttributeError:
                # _datasource is unset during deserialization before the asset is attached
                return None
            schema = datasource.schema_
            if schema is not None:
                schema = self._to_lower_if_not_bracketed_by_quotes(schema)
            return schema
        if isinstance(self.schema_name, str):
            return self._to_lower_if_not_bracketed_by_quotes(self.schema_name)
        return None

    @property
    def qualified_name(self) -> str:
        schema = self._effective_schema_name
        return f"{schema}.{self.table_name}" if schema else self.table_name

    @pydantic.validator("table_name", pre=True, always=True)
    def _default_table_name(cls, table_name: str, values: dict, **kwargs) -> str:
        if not (validated_table_name := table_name or values.get("name")):
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "table_name cannot be empty and should default to name if not provided"
            )

        return validated_table_name

    @pydantic.validator("table_name")
    def _resolve_quoted_name(cls, table_name: str, values: Dict[str, Any]) -> str:
        # We reimport sqlalchemy from our compatability layer because we make
        # quoted_name a top level import there.
        from great_expectations.compatibility import sqlalchemy

        if sqlalchemy.quoted_name:  # type: ignore[truthy-function] # FIXME CoP
            if isinstance(table_name, sqlalchemy.quoted_name):
                return table_name

            quote: bool = cls._is_bracketed_by_quotes(table_name)

            if quote:
                # https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.quoted_name.quote
                # Remove the quotes and add them back using the sqlalchemy.quoted_name function
                # TODO: We need to handle nested quotes
                values["_quote_character"] = table_name[0]
                quote = True
                table_name = table_name.lstrip("".join(DEFAULT_INITIAL_QUOTE_CHARACTERS)).rstrip(
                    "".join(DEFAULT_FINAL_QUOTE_CHARACTERS.values())
                )

            return sqlalchemy.quoted_name(
                value=table_name,
                quote=quote,
            )

        return table_name

    @override
    def dict(self, **kwargs) -> Dict[str, Any]:
        original_dict = super().dict(**kwargs)

        # we need to ensure we retain the quotes when serializing quoted names
        qc = self._quote_character
        if qc is not None:
            original_dict["table_name"] = (
                f"{qc}{self.table_name}{DEFAULT_FINAL_QUOTE_CHARACTERS[qc]}"
            )

        # Exclude schema_name from serialization when it wasn't explicitly provided
        # or was set to None, so stored configs stop including it before the field
        # is removed.
        schema = original_dict.get("schema_name")
        if schema is None or schema is MISSING:
            original_dict.pop("schema_name", None)

        return original_dict

    @override
    def test_connection(self) -> None:
        """Test the connection for the TableAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: SQLDatasource = self.datasource
        engine: sqlalchemy.Engine = datasource.get_engine()
        effective_schema = self._effective_schema_name

        try:
            with engine.connect() as connection:
                table = sa.table(self.table_name, schema=effective_schema)
                # don't need to fetch any data, just want to make sure the table is accessible
                connection.execute(sa.select(1, table).limit(1))
        except Exception as query_error:
            LOGGER.info(f"{self.name} `.test_connection()` query failed: {query_error!r}")
            # A missing schema is a common cause of this failure and deserves a more
            # specific message, but determining it requires listing every schema on the
            # server. On some backends that listing is dramatically more expensive than
            # the probe query itself -- it can be a server-wide metadata scan whose cost
            # scales with the whole instance rather than with this table -- so it is only
            # worth paying for once we already know the probe failed.
            if effective_schema and not self._schema_exists(engine, effective_schema):
                raise TestConnectionError(  # noqa: TRY003 # FIXME CoP
                    f'Attempt to connect to table: "{self.qualified_name}" failed because '
                    f'the schema "{effective_schema}" does not exist.'
                ) from query_error
            raise TestConnectionError(  # noqa: TRY003 # FIXME CoP
                f"Attempt to connect to table: {self.qualified_name} failed because the test query "
                f"failed. Ensure the table exists and the user has access to select data from the table: {query_error}"  # noqa: E501 # FIXME CoP
            ) from query_error

    def _schema_exists(self, engine: sqlalchemy.Engine, effective_schema: str) -> bool:
        """Whether ``effective_schema`` is visible on the server.

        Only ever called after the connection probe has already failed, to decide
        between two error messages. If the listing itself fails we cannot tell, so
        report the schema as present and let the caller fall back to the generic
        message rather than masking the original error with this one.
        """
        try:
            inspector: sqlalchemy.Inspector = sa.inspect(engine)
            schema_names = inspector.get_schema_names() or []
        except Exception as inspect_error:
            LOGGER.info(
                f"{self.name} `.test_connection()` could not list schemas: {inspect_error!r}"
            )
            return True
        return effective_schema in [
            self._to_lower_if_not_bracketed_by_quotes(name) for name in schema_names
        ]

    @override
    def as_selectable(self) -> sqlalchemy.Selectable:
        """Returns the table as a sqlalchemy Selectable.

        This can be used in a from clause for a query against this data.
        """
        return sa.table(self.table_name, schema=self._effective_schema_name)

    @override
    def _create_batch_spec_kwargs(self) -> Dict[str, Any]:
        return {
            "type": "table",
            "data_asset_name": self.name,
            "table_name": self.table_name,
            "schema_name": self._effective_schema_name,
            "batch_identifiers": {},
        }

    @override
    def _create_batch_spec(self, batch_spec_kwargs: Dict) -> SqlAlchemyDatasourceBatchSpec:
        return SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)

    @staticmethod
    def _is_bracketed_by_quotes(target: str) -> bool:
        """
        Returns True if the target string is bracketed by quotes.

        Override this method if the quote characters are different than `'` or `"` in the
        target database, such as backticks in Databricks SQL.

        Arguments:
            target: A string to check if it is bracketed by quotes.

        Returns:
            True if the target string is bracketed by quotes.
        """
        return any(
            target.startswith(quote) and target.endswith(DEFAULT_FINAL_QUOTE_CHARACTERS[quote])
            for quote in DEFAULT_INITIAL_QUOTE_CHARACTERS
        )

    @classmethod
    def _to_lower_if_not_bracketed_by_quotes(cls, target: str) -> str:
        """Returns the target string in lowercase if it is not bracketed by quotes.
        This is used to ensure case-insensitivity in sqlalchemy queries.

        Arguments:
            target: A string to convert to lowercase if it is not bracketed by quotes.

        Returns:
            The target string in lowercase if it is not bracketed by quotes.
        """
        return to_lower_if_not_quoted(target, quote_characters=DEFAULT_INITIAL_QUOTE_CHARACTERS)


def _warn_for_more_specific_datasource_type(connection_string: str) -> None:
    """
    Warns if a more specific datasource type may be more appropriate based on the connection string connector prefix.
    """  # noqa: E501 # FIXME CoP
    from great_expectations.datasource.fluent.sources import DataSourceManager

    connector: str = connection_string.split("://", maxsplit=1)[0].split("+", maxsplit=1)[0]

    type_lookup_plus: Dict[str, str] = {
        n: DataSourceManager.type_lookup[n].__name__
        for n in DataSourceManager.type_lookup.type_names()
    }
    # type names are not always exact match to connector strings
    type_lookup_plus.update(
        {
            "postgresql": type_lookup_plus["postgres"],
            "databricks": type_lookup_plus["databricks_sql"],
        }
    )

    more_specific_datasource: str | None = type_lookup_plus.get(connector)
    if more_specific_datasource:
        warnings.warn(
            f"You are using a generic SQLDatasource but a more specific {more_specific_datasource} "
            "may be more appropriate"
            " https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data",
            category=GxDatasourceWarning,
        )


# This improves our error messages by providing a more specific type for pydantic to validate against  # noqa: E501 # FIXME CoP
# It also ensure the generated jsonschema has a oneOf instead of anyOf field for assets
# https://docs.pydantic.dev/1.10/usage/types/#discriminated-unions-aka-tagged-unions
AssetTypes = Annotated[Union[TableAsset, QueryAsset], Field(discriminator="type")]


@public_api
class SQLDatasource(Datasource):
    """Adds a generic SQL datasource to the data context.

    Args:
        name: The name of this datasource.
        connection_string: The SQLAlchemy connection string used to connect to the database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        create_temp_table: Whether to leverage temporary tables during metric computation.
        kwargs: Extra SQLAlchemy keyword arguments to pass to `create_engine()`. Note, only python
            primitive types will be serializable to config.
        assets: An optional dictionary whose keys are SQL DataAsset names and whose values
            are SQL DataAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset, QueryAsset]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sql"] = "sql"
    connection_string: Union[ConfigStr, str]
    create_temp_table: bool = False
    kwargs: Dict[str, Union[ConfigStr, Any]] = pydantic.Field(
        default={},
        description="Optional dictionary of `kwargs` will be passed to the SQLAlchemy Engine"
        " as part of `create_engine(connection_string, **kwargs)`",
    )
    # We need to explicitly add each asset type to the Union due to how
    # deserialization is implemented in our pydantic base model.
    assets: List[AssetTypes] = []

    # private attrs
    _cached_connection_string: Union[str, ConfigStr] = pydantic.PrivateAttr("")
    _engine: Union[sqlalchemy.Engine, None] = pydantic.PrivateAttr(None)

    # These are instance var because ClassVars can't contain Type variables. See
    # https://peps.python.org/pep-0526/#class-and-instance-variable-annotations
    _TableAsset: Type[TableAsset] = pydantic.PrivateAttr(TableAsset)
    _QueryAsset: Type[QueryAsset] = pydantic.PrivateAttr(QueryAsset)

    class Config:
        validate_assignment = True

    @property
    def schema_(self) -> str | None:
        """The schema for this datasource, if available.

        There is no standard way to encode schema in a database connection URL,
        so the base implementation returns ``None``.  Subclasses with structured
        connection details (e.g. SnowflakeDatasource, SQLServerDatasource)
        override this to expose the schema.
        """
        return None

    @property
    @override
    def execution_engine_type(self) -> Type[SqlAlchemyExecutionEngine]:
        """Returns the default execution engine type."""
        return SqlAlchemyExecutionEngine

    def get_engine(self) -> sqlalchemy.Engine:
        if self.connection_string != self._cached_connection_string or not self._engine:
            try:
                self._engine = self._create_engine()
            except Exception as e:
                # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine  # noqa: E501 # FIXME CoP
                # one possible case is a missing plugin (e.g. psycopg2)
                raise SQLAlchemyCreateEngineError(cause=e) from e
            self._cached_connection_string = self.connection_string
        return self._engine

    def _create_engine(self) -> sqlalchemy.Engine:
        model_dict = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
        )
        _check_config_substitutions_needed(
            self, model_dict, raise_warning_if_provider_not_present=True
        )
        # the connection_string has had config substitutions applied
        connection_string = model_dict.pop("connection_string")
        if self.__class__.__name__ == "SQLDatasource":
            _warn_for_more_specific_datasource_type(connection_string)
        kwargs = model_dict.pop("kwargs", {})
        return sa.create_engine(connection_string, **kwargs)

    @override
    def get_execution_engine(self) -> SqlAlchemyExecutionEngine:
        # Overrides get_execution_engine in Datasource
        # because we need to pass the kwargs as keyvalue args to the execution engine
        # when then passes them to the engine.
        current_execution_engine_kwargs = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
            # by default we exclude unset values to prevent lots of extra values in the yaml files
            # but we want to include them here
            exclude_unset=False,
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            # Copy before the pop below, which mutates the dict it is taken from. The
            # cached copy must keep the "kwargs" key so that it compares equal to the
            # next freshly computed dict. Caching the same object left the cache
            # permanently missing that key, so the comparison never matched and every
            # call built a new execution engine and SQLAlchemy engine.
            cached_execution_engine_kwargs = dict(current_execution_engine_kwargs)
            engine_kwargs = current_execution_engine_kwargs.pop("kwargs", {})
            self._execution_engine = self._execution_engine_type()(
                **current_execution_engine_kwargs,
                **engine_kwargs,
            )
            # Cache only once the engine exists. Caching first would leave a failed
            # rebuild holding kwargs no engine was ever built from, so the next call
            # would compare equal and return the engine from the previous configuration
            # instead of retrying.
            self._cached_execution_engine_kwargs = cached_execution_engine_kwargs
        return self._execution_engine

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SQLDatasource.

        Args:
            test_assets: If assets have been passed to the SQLDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """  # noqa: E501 # FIXME CoP
        try:
            engine: sqlalchemy.Engine = self.get_engine()
            with engine.connect():
                pass
        except Exception as e:
            raise TestConnectionError(cause=e) from e
        if self.assets and test_assets:
            for asset in self.assets:
                asset._datasource = self
                asset.test_connection()

    @deprecated_argument(
        argument_name="schema_name",
        version="1.14.0",
        message="Pass the schema in your datasource's connection configuration instead.",
    )
    @public_api
    def add_table_asset(
        self,
        name: str,
        table_name: str = "",
        schema_name: str | Missing | None = MISSING,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table. Will use the datasource schema if not
                provided.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches
                derived from it.

        Returns:
            The table asset that is added to the datasource.
            The type of this object will match the necessary type for this datasource.
            eg, it could be a TableAsset or a SqliteTableAsset.
        """
        asset = self._TableAsset(
            name=name,
            table_name=table_name,
            schema_name=schema_name,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)

    @public_api
    def add_query_asset(
        self,
        name: str,
        query: str,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> QueryAsset:
        """Adds a query asset to this datasource.

        Args:
            name: The name of this query asset.
            query: The SELECT query to selects the data to validate. It must begin with the "SELECT".
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The query asset that is added to the datasource.
            The type of this object will match the necessary type for this datasource.
            eg, it could be a QueryAsset or a SqliteQueryAsset.
        """  # noqa: E501 # FIXME CoP
        asset = self._QueryAsset(
            name=name,
            query=query,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)
