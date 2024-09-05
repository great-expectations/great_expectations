from __future__ import annotations

import copy
import logging
import warnings
from datetime import date, datetime
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Final,
    Generic,
    List,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import Annotated

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
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
from great_expectations.exceptions.exceptions import NoAvailableBatchesError
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.partition_and_sample.data_partitioner import (
    DatePart,
)
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner import (
    SqlAlchemyDataPartitioner,
)

if TYPE_CHECKING:
    from sqlalchemy.sql import quoted_name  # noqa: TID251 # type-checking only

    from great_expectations.compatibility import sqlalchemy
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.fluent import BatchParameters
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchSlice,
    )

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

DEFAULT_QUOTE_CHARACTERS: Final[Tuple[str, str]] = ('"', "'")


@overload
def to_lower_if_not_quoted(value: str, quote_characters: Sequence[str] = ...) -> str: ...


@overload
def to_lower_if_not_quoted(value: None, quote_characters: Sequence[str] = ...) -> None: ...


def to_lower_if_not_quoted(
    value: str | None,
    quote_characters: Sequence[str] = DEFAULT_QUOTE_CHARACTERS,
) -> str | None:
    """
    Convert a string to lowercase if it is not enclosed in quotes.
    """
    if not value:
        return value
    for char in quote_characters:
        if value.startswith(char) and value.endswith(char):
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
                raise ValueError(f"'{part}' must be specified in the batch parameters")  # noqa: TRY003
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


class SqlPartitionerModInteger(_PartitionerOneColumnOneParam):
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
    def batch_parameters_to_batch_spec_kwarg_identifiers(
        self, options: BatchParameters
    ) -> Dict[str, Any]:
        if "remainder" not in options:
            raise ValueError("'remainder' must be specified in the batch parameters")  # noqa: TRY003
        return {self.column_name: options["remainder"]}


class SqlPartitionerColumnValue(_PartitionerOneColumnOneParam):
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
            raise ValueError(  # noqa: TRY003
                f"All column names, {self.column_names}, must be specified in the batch parameters. "  # noqa: E501
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
            raise ValueError(  # noqa: TRY003
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
            raise ValueError(  # noqa: TRY003
                f"Requested Partitioner `{abstract_partitioner.method_name}` is not implemented for this DataAsset. "  # noqa: E501
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

    def _fully_specified_batch_requests(self, batch_request: BatchRequest) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests."""

        if batch_request.partitioner is None:
            # Currently batch_request.options is complete determined by the presence of a
            # partitioner. If partitioner is None, then there are no specifiable options
            # so we return early. Since the passed in batch_request is verified, it must be the
            # empty, ie {}.
            # In the future, if there are options that are not determined by the partitioner
            # this check will have to be generalized.
            return [batch_request]

        sql_partitioner = self.get_partitioner_implementation(batch_request.partitioner)

        batch_requests: List[BatchRequest] = []
        # We iterate through all possible batches as determined by the partitioner
        for params in sql_partitioner.param_defaults(self):
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
        return batch_requests

    @override
    def get_batch_identifiers_list(self, batch_request: BatchRequest) -> List[dict]:
        self._validate_batch_request(batch_request)
        if batch_request.partitioner:
            sql_partitioner = self.get_partitioner_implementation(batch_request.partitioner)
        else:
            sql_partitioner = None

        requests = self._fully_specified_batch_requests(batch_request)
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

        batch_spec_kwargs: dict[str, str | dict | None]
        requests = self._fully_specified_batch_requests(batch_request)
        unsorted_metadata_dicts = [self._get_batch_metadata_from_batch_request(r) for r in requests]

        if not unsorted_metadata_dicts:
            raise NoAvailableBatchesError()

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
            cast(Dict, batch_spec_kwargs["batch_identifiers"]).update(
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
                calling batch_parameters.
            batch_slice: A python slice that can be used to limit the sorted batches by index.
                e.g. `batch_slice = "[-5:]"` will request only the last 5 batches after the options filter is applied.
            partitioner: A Partitioner used to narrow the data returned from the asset.

        Returns:
            A BatchRequest object that can be used to obtain a batch from an Asset by calling the
            get_batch method.
        """  # noqa: E501
        if options is not None and not self._batch_parameters_are_valid(
            options=options, partitioner=partitioner
        ):
            allowed_keys = set(self.get_batch_parameters_keys(partitioner=partitioner))
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
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

    @public_api
    def add_batch_definition_whole_table(self, name: str) -> BatchDefinition:
        return self.add_batch_definition(
            name=name,
            partitioner=None,
        )

    @public_api
    def add_batch_definition_yearly(
        self, name: str, column: str, sort_ascending: bool = True
    ) -> BatchDefinition:
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerYearly(
                method_name="partition_on_year", column_name=column, sort_ascending=sort_ascending
            ),
        )

    @public_api
    def add_batch_definition_monthly(
        self, name: str, column: str, sort_ascending: bool = True
    ) -> BatchDefinition:
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerMonthly(
                method_name="partition_on_year_and_month",
                column_name=column,
                sort_ascending=sort_ascending,
            ),
        )

    @public_api
    def add_batch_definition_daily(
        self, name: str, column: str, sort_ascending: bool = True
    ) -> BatchDefinition:
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerDaily(
                method_name="partition_on_year_and_month_and_day",
                column_name=column,
                sort_ascending=sort_ascending,
            ),
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
            options = {
                option: None
                for option in self.get_batch_parameters_keys(partitioner=batch_request.partitioner)
            }
            expect_batch_request_form = BatchRequest[ColumnPartitioner](
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=options,
                batch_slice=batch_request._batch_slice_input,  # type: ignore[attr-defined]
                partitioner=batch_request.partitioner,
            )
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "BatchRequest should have form:\n"
                f"{pf(expect_batch_request_form.dict())}\n"
                f"but actually has form:\n{pf(batch_request.dict())}\n"
            )

    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        """Creates batch_spec_kwargs used to instantiate a SqlAlchemyDatasourceBatchSpec or RuntimeQueryBatchSpec

        This is called by get_batch to generate the batch.

        Returns:
            A dictionary that will be passed to self._create_batch_spec(**returned_dict)
        """  # noqa: E501
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
    # Instance fields
    type: Literal["query"] = "query"
    query: str

    @pydantic.validator("query")
    def query_must_start_with_select(cls, v: str):
        query = v.lstrip()
        if not (query.upper().startswith("SELECT") and query[6].isspace()):
            raise ValueError("query must start with 'SELECT' followed by a whitespace.")  # noqa: TRY003
        return v

    @override
    def as_selectable(self) -> sqlalchemy.Selectable:
        """Returns the Selectable that is used to retrieve the data.

        This can be used in a subselect FROM clause for queries against this data.
        """
        return sa.select(sa.text(self.query.lstrip()[6:])).subquery()

    @override
    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        return {
            "data_asset_name": self.name,
            "query": self.query,
            "temp_table_schema_name": None,
            "batch_identifiers": {},
        }

    @override
    def _create_batch_spec(self, batch_spec_kwargs: dict) -> RuntimeQueryBatchSpec:
        return RuntimeQueryBatchSpec(**batch_spec_kwargs)


@public_api
class TableAsset(_SQLAsset):
    # Instance fields
    type: Literal["table"] = "table"
    # TODO: quoted_name or str
    table_name: str = pydantic.Field(
        "",
        description="Name of the SQL table. Will default to the value of `name` if not provided.",
    )
    schema_name: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}" if self.schema_name else self.table_name

    @pydantic.validator("table_name", pre=True, always=True)
    def _default_table_name(cls, table_name: str, values: dict, **kwargs) -> str:
        if not (validated_table_name := table_name or values.get("name")):
            raise ValueError(  # noqa: TRY003
                "table_name cannot be empty and should default to name if not provided"
            )

        return validated_table_name

    @pydantic.validator("table_name")
    def _resolve_quoted_name(cls, table_name: str) -> str | quoted_name:
        table_name_is_quoted: bool = cls._is_bracketed_by_quotes(table_name)

        from great_expectations.compatibility import sqlalchemy

        if sqlalchemy.quoted_name:  # type: ignore[truthy-function]
            if isinstance(table_name, sqlalchemy.quoted_name):
                return table_name

            if table_name_is_quoted:
                # https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.quoted_name.quote
                # Remove the quotes and add them back using the sqlalchemy.quoted_name function
                # TODO: We need to handle nested quotes
                table_name = table_name.strip("'").strip('"')

            return sqlalchemy.quoted_name(
                value=table_name,
                quote=table_name_is_quoted,
            )

        return table_name

    @override
    def test_connection(self) -> None:
        """Test the connection for the TableAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: SQLDatasource = self.datasource
        engine: sqlalchemy.Engine = datasource.get_engine()
        inspector: sqlalchemy.Inspector = sa.inspect(engine)

        if self.schema_name and self.schema_name not in inspector.get_schema_names():
            raise TestConnectionError(  # noqa: TRY003
                f'Attempt to connect to table: "{self.qualified_name}" failed because the schema '
                f'"{self.schema_name}" does not exist.'
            )

        try:
            with engine.connect() as connection:
                table = sa.table(self.table_name, schema=self.schema_name)
                # don't need to fetch any data, just want to make sure the table is accessible
                connection.execute(sa.select(1, table).limit(1))
        except Exception as query_error:
            LOGGER.info(f"{self.name} `.test_connection()` query failed: {query_error!r}")
            raise TestConnectionError(  # noqa: TRY003
                f"Attempt to connect to table: {self.qualified_name} failed because the test query "
                f"failed. Ensure the table exists and the user has access to select data from the table: {query_error}"  # noqa: E501
            ) from query_error

    @override
    def as_selectable(self) -> sqlalchemy.Selectable:
        """Returns the table as a sqlalchemy Selectable.

        This can be used in a from clause for a query against this data.
        """
        return sa.text(self.qualified_name)  # type: ignore[return-value]

    @override
    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        return {
            "type": "table",
            "data_asset_name": self.name,
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "batch_identifiers": {},
        }

    @override
    def _create_batch_spec(self, batch_spec_kwargs: dict) -> SqlAlchemyDatasourceBatchSpec:
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
            target.startswith(quote) and target.endswith(quote)
            for quote in DEFAULT_QUOTE_CHARACTERS
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
        return to_lower_if_not_quoted(target, quote_characters=DEFAULT_QUOTE_CHARACTERS)


def _warn_for_more_specific_datasource_type(connection_string: str) -> None:
    """
    Warns if a more specific datasource type may be more appropriate based on the connection string connector prefix.
    """  # noqa: E501
    from great_expectations.datasource.fluent.sources import DataSourceManager

    connector: str = connection_string.split("://")[0].split("+")[0]

    type_lookup_plus: dict[str, str] = {
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


# This improves our error messages by providing a more specific type for pydantic to validate against  # noqa: E501
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
                # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine  # noqa: E501
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
            self._cached_execution_engine_kwargs = current_execution_engine_kwargs
            engine_kwargs = current_execution_engine_kwargs.pop("kwargs", {})
            self._execution_engine = self._execution_engine_type()(
                **current_execution_engine_kwargs,
                **engine_kwargs,
            )
        return self._execution_engine

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SQLDatasource.

        Args:
            test_assets: If assets have been passed to the SQLDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """  # noqa: E501
        try:
            engine: sqlalchemy.Engine = self.get_engine()
            engine.connect()
        except Exception as e:
            raise TestConnectionError(cause=e) from e
        if self.assets and test_assets:
            for asset in self.assets:
                asset._datasource = self
                asset.test_connection()

    @public_api
    def add_table_asset(
        self,
        name: str,
        table_name: str = "",
        schema_name: Optional[str] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The table asset that is added to the datasource.
            The type of this object will match the necessary type for this datasource.
            eg, it could be a TableAsset or a SqliteTableAsset.
        """  # noqa: E501
        if schema_name:
            schema_name = self._TableAsset._to_lower_if_not_bracketed_by_quotes(schema_name)
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
            name: The name of this table asset.
            query: The SELECT query to selects the data to validate. It must begin with the "SELECT".
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The query asset that is added to the datasource.
            The type of this object will match the necessary type for this datasource.
            eg, it could be a QueryAsset or a SqliteQueryAsset.
        """  # noqa: E501
        asset = self._QueryAsset(
            name=name,
            query=query,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)
