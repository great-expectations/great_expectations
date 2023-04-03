from __future__ import annotations

import copy
import dataclasses
import functools
import logging
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

import pandas as pd
import pydantic
from pydantic import Field, StrictBool, StrictInt, root_validator, validate_arguments
from typing_extensions import TypeAlias

from great_expectations.core.config_substitutor import _ConfigurationSubstitutor
from great_expectations.core.id_dict import BatchSpec  # noqa: TCH001
from great_expectations.datasource.fluent.datasource import (
    Datasource,
    Sorter,
    _sorter_from_list,
)
from great_expectations.datasource.fluent.fluent_base_model import (
    FluentBaseModel,
)
from great_expectations.validator.metrics_calculator import MetricsCalculator

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # TODO: We should try to import the annotations from core.batch so we no longer need to call
    #  Batch.update_forward_refs() before instantiation.
    from great_expectations.core.batch import (
        BatchData,
        BatchDefinition,
        BatchMarkers,
    )
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )
    from great_expectations.datasource.fluent.datasource import SortersDefinition

try:
    import pyspark
    from pyspark.sql import Row as pyspark_sql_Row
except ImportError:
    pyspark = None  # type: ignore[assignment]
    pyspark_sql_Row = None  # type: ignore[assignment,misc]
    logger.debug("No spark sql dataframe module available.")


class TestConnectionError(Exception):
    pass


# BatchRequestOptions is a dict that is composed into a BatchRequest that specifies the
# Batches one wants as returned. The keys represent dimensions one can slice the data along
# and the values are the realized. If a value is None or unspecified, the batch_request
# will capture all data along this dimension. For example, if we have a year and month
# splitter, and we want to query all months in the year 2020, the batch request options
# would look like:
#   options = { "year": 2020 }
BatchRequestOptions: TypeAlias = Dict[str, Any]


BatchMetadata: TypeAlias = Dict[str, Any]


@dataclasses.dataclass(frozen=True)
class BatchRequest:
    datasource_name: str
    data_asset_name: str
    options: BatchRequestOptions


_DatasourceT = TypeVar("_DatasourceT", bound=Datasource)


class DataAsset(FluentBaseModel, Generic[_DatasourceT]):
    # To subclass a DataAsset one must define `type` as a Class literal explicitly on the sublass
    # as well as implementing the methods in the `Abstract Methods` section below.
    # Some examples:
    # * type: Literal["MyAssetTypeID"] = "MyAssetTypeID",
    # * type: Literal["table"] = "table"
    # * type: Literal["csv"] = "csv"
    name: str
    type: str
    id: Optional[uuid.UUID] = Field(default=None, description="DataAsset id")

    order_by: List[Sorter] = Field(default_factory=list)
    batch_metadata: BatchMetadata = pydantic.Field(default_factory=dict)

    # non-field private attributes
    _datasource: _DatasourceT = pydantic.PrivateAttr()
    _data_connector: Optional[DataConnector] = pydantic.PrivateAttr(default=None)
    _test_connection_error_message: Optional[str] = pydantic.PrivateAttr(default=None)

    @property
    def datasource(self) -> _DatasourceT:
        return self._datasource

    def test_connection(self) -> None:
        """Test the connection for the DataAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a DataAsset subclass."""
        )

    # Abstract Methods
    @property
    def batch_request_options(self) -> tuple[str, ...]:
        """The potential keys for BatchRequestOptions.

        Example:
        ```python
        >>> print(asset.batch_request_options)
        ("day", "month", "year")
        >>> options = {"year": "2023"}
        >>> batch_request = asset.build_batch_request(options=options)
        ```

        Returns:
            A tuple of keys that can be used in a BatchRequestOptions dictionary.
        """
        raise NotImplementedError(
            """One needs to implement "batch_request_options" on a DataAsset subclass."""
        )

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        raise NotImplementedError

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to limit the number of batches returned from the asset.
                The dict structure depends on the asset type. The available keys for dict can be obtained by
                calling batch_request_options.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        raise NotImplementedError(
            """One must implement "build_batch_request" on a DataAsset subclass."""
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        raise NotImplementedError(
            """One must implement "_validate_batch_request" on a DataAsset subclass."""
        )

    # End Abstract Methods

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(set(self.batch_request_options))

    def _get_batch_metadata_from_batch_request(
        self, batch_request: BatchRequest
    ) -> BatchMetadata:
        """Performs config variable substitution and populates batch request options for
        Batch.metadata at runtime.
        """
        batch_metadata = copy.deepcopy(self.batch_metadata)
        config_variables = self._datasource._data_context.config_variables
        batch_metadata = _ConfigurationSubstitutor().substitute_all_config_variables(
            data=batch_metadata, replace_variables_dict=config_variables
        )
        batch_metadata.update(copy.deepcopy(batch_request.options))
        return batch_metadata

    # Sorter methods
    @pydantic.validator("order_by", pre=True)
    def _parse_order_by_sorters(
        cls, order_by: Optional[List[Union[Sorter, str, dict]]] = None
    ) -> List[Sorter]:
        return Datasource.parse_order_by_sorters(order_by=order_by)

    def add_sorters(self: _DataAssetT, sorters: SortersDefinition) -> _DataAssetT:
        """Associates a sorter to this DataAsset

        The passed in sorters will replace any previously associated sorters.
        Batches returned from this DataAsset will be sorted on the batch's
        metadata in the order specified by `sorters`. Sorters work left to right.
        That is, batches will be sorted first by sorters[0].key, then
        sorters[1].key, and so on. If sorter[i].reverse is True, that key will
        sort the batches in descending, as opposed to ascending, order.

        Args:
            sorters: A list of either Sorter objects or strings. The strings
              are a shorthand for Sorter objects and are parsed as follows:
              r'[+-]?.*'
              An optional prefix of '+' or '-' sets Sorter.reverse to
              'False' or 'True' respectively. It is 'False' if no prefix is present.
              The rest of the string gets assigned to the Sorter.key.
              For example:
              ["key1", "-key2", "key3"]
              is equivalent to:
              [
                  Sorter(key="key1", reverse=False),
                  Sorter(key="key2", reverse=True),
                  Sorter(key="key3", reverse=False),
              ]

        Returns:
            This DataAsset with the passed in sorters accessible via self.order_by
        """
        # NOTE: (kilo59) we could use pydantic `validate_assignment` for this
        # https://docs.pydantic.dev/usage/model_config/#options
        self.order_by = _sorter_from_list(sorters)
        return self

    def sort_batches(self, batch_list: List[Batch]) -> None:
        """Sorts batch_list in place in the order configured in this DataAsset.

        Args:
            batch_list: The list of batches to sort in place.
        """
        for sorter in reversed(self.order_by):
            try:
                batch_list.sort(
                    key=functools.cmp_to_key(
                        _sort_batches_with_none_metadata_values(sorter.key)
                    ),
                    reverse=sorter.reverse,
                )
            except KeyError as e:
                raise KeyError(
                    f"Trying to sort {self.name} table asset batches on key {sorter.key} "
                    "which isn't available on all batches."
                ) from e


def _sort_batches_with_none_metadata_values(
    key: str,
) -> Callable[[Batch, Batch], int]:
    def _compare_function(a: Batch, b: Batch) -> int:
        if a.metadata[key] is not None and b.metadata[key] is not None:
            if a.metadata[key] < b.metadata[key]:
                return -1

            if a.metadata[key] > b.metadata[key]:
                return 1

            return 0

        if a.metadata[key] is None and b.metadata[key] is None:
            return 0

        if a.metadata[key] is None:  # b.metadata[key] is not None
            return -1

        if a.metadata[key] is not None:  # b.metadata[key] is None
            return 1

        # This line should never be reached; hence, "ValueError" with corresponding error message is raised.
        raise ValueError(
            f'Unexpected Batch metadata key combination, "{a.metadata[key]}" and "{b.metadata[key]}", was encountered.'
        )

    return _compare_function


_DataAssetT = TypeVar("_DataAssetT", bound=DataAsset)


@dataclasses.dataclass(frozen=True)
class HeadData:
    """
    An immutable wrapper around pd.DataFrame for .head() methods which
        are intended to be used for visual inspection of BatchData.
    """

    data: pd.DataFrame

    def __repr__(self) -> str:
        return self.data.__repr__()


class Batch(FluentBaseModel):
    """This represents a batch of data.

    This is usually not the data itself but a hook to the data on an external datastore such as
    a spark or a sql database. An exception exists for pandas or any in-memory datastore.
    """

    datasource: Datasource
    data_asset: DataAsset
    batch_request: BatchRequest
    data: BatchData
    id: str = ""
    # metadata is any arbitrary data one wants to associate with a batch. GX will add arbitrary metadata
    # to a batch so developers may want to namespace any custom metadata they add.
    metadata: Dict[str, Any] = {}

    # TODO: These legacy fields are currently required. They are only used in usage stats so we
    #       should figure out a better way to anonymize and delete them.
    batch_markers: BatchMarkers = Field(..., alias="legacy_batch_markers")
    batch_spec: BatchSpec = Field(..., alias="legacy_batch_spec")
    batch_definition: BatchDefinition = Field(..., alias="legacy_batch_definition")

    class Config:
        allow_mutation = False
        arbitrary_types_allowed = True

    @root_validator(pre=True)
    def _set_id(cls, values: dict) -> dict:
        # We need a unique identifier. This will likely change as we get more input.
        options_list = []
        for key, value in values["batch_request"].options.items():
            if key != "path":
                options_list.append(f"{key}_{value}")

        values["id"] = "-".join(
            [values["datasource"].name, values["data_asset"].name, *options_list]
        )

        return values

    @classmethod
    def update_forward_refs(cls):
        from great_expectations.core.batch import (
            BatchData,
            BatchDefinition,
            BatchMarkers,
        )

        super().update_forward_refs(
            BatchData=BatchData,
            BatchDefinition=BatchDefinition,
            BatchMarkers=BatchMarkers,
        )

    @validate_arguments
    def head(
        self,
        n_rows: StrictInt = 5,
        fetch_all: StrictBool = False,
    ) -> HeadData:
        """Return the first n rows of this Batch.

        This method returns the first n rows for the Batch based on position.

        For negative values of n_rows, this method returns all rows except the last n rows.

        If n_rows is larger than the number of rows, this method returns all rows.

        Parameters
            n_rows: The number of rows to return from the Batch.
            fetch_all: If True, ignore n_rows and return the entire Batch.

        Returns
            HeadData
        """
        self.data.execution_engine.batch_manager.load_batch_list(batch_list=[self])
        metrics_calculator = MetricsCalculator(
            execution_engine=self.data.execution_engine,
            show_progress_bars=True,
        )
        table_head_df: pd.DataFrame = metrics_calculator.head(
            n_rows=n_rows,
            domain_kwargs={"batch_id": self.id},
            fetch_all=fetch_all,
        )
        return HeadData(data=table_head_df.reset_index(drop=True, inplace=False))
