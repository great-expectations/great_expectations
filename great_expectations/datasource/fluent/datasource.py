from __future__ import annotations

import logging
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
)

import pydantic
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import TypeAlias, TypeGuard

from great_expectations.datasource.fluent.fluent_base_model import FluentBaseModel
from great_expectations.datasource.fluent.metadatasource import MetaDatasource

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context import AbstractDataContext as GXDataContext
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )
    from great_expectations.datasource.fluent.interfaces import (
        Batch,
        BatchRequest,
        DataAsset,
    )
    from great_expectations.datasource.fluent.type_lookup import TypeLookup


@pydantic_dc.dataclass(frozen=True)
class Sorter:
    key: str
    reverse: bool = False


SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]


def _is_sorter_list(
    sorters: SortersDefinition,
) -> TypeGuard[list[Sorter]]:
    if len(sorters) == 0 or isinstance(sorters[0], Sorter):
        return True
    return False


def _is_str_sorter_list(sorters: SortersDefinition) -> TypeGuard[list[str]]:
    if len(sorters) > 0 and isinstance(sorters[0], str):
        return True
    return False


def _sorter_from_list(sorters: SortersDefinition) -> list[Sorter]:
    if _is_sorter_list(sorters):
        return sorters

    # mypy doesn't successfully type-narrow sorters to a list[str] here, so we use
    # another TypeGuard. We could cast instead which may be slightly faster.
    sring_valued_sorter: str
    if _is_str_sorter_list(sorters):
        return [
            _sorter_from_str(sring_valued_sorter) for sring_valued_sorter in sorters
        ]

    # This should never be reached because of static typing but is necessary because
    # mypy doesn't know of the if conditions must evaluate to True.
    raise ValueError(f"sorters is a not a SortersDefinition but is a {type(sorters)}")


def _sorter_from_str(sort_key: str) -> Sorter:
    """Convert a list of strings to Sorter objects

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting.  If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return Sorter(key=sort_key[1:], reverse=True)

    if sort_key[0] == "+":
        return Sorter(key=sort_key[1:], reverse=False)

    return Sorter(key=sort_key, reverse=False)


# It would be best to bind this to ExecutionEngine, but we can't now due to circular imports
_ExecutionEngineT = TypeVar("_ExecutionEngineT")


class Datasource(
    FluentBaseModel,
    Generic[_ExecutionEngineT],
    metaclass=MetaDatasource,
):
    # To subclass Datasource one needs to define:
    # asset_types
    # type
    # assets
    #
    # The important part of defining `assets` is setting the Dict type correctly.
    # In addition, one must define the methods in the `Abstract Methods` section below.
    # If one writes a class level docstring, this will become the documentation for the
    # data context method `data_context.sources.add_my_datasource` method.

    # class attrs
    asset_types: ClassVar[Sequence[Type[DataAsset]]] = []
    # Not all Datasources require a DataConnector
    data_connector_type: ClassVar[Optional[Type[DataConnector]]] = None
    # Datasource instance attrs but these will be fed into the `execution_engine` constructor
    _EXCLUDED_EXEC_ENG_ARGS: ClassVar[Set[str]] = {
        "name",
        "type",
        "id",
        "execution_engine",
        "assets",
        "base_directory",  # filesystem argument
        "glob_directive",  # filesystem argument
        "data_context_root_directory",  # filesystem argument
        "bucket",  # s3 argument
        "boto3_options",  # s3 argument
        "prefix",  # s3 argument and gcs argument
        "delimiter",  # s3 argument and gcs argument
        "max_keys",  # s3 argument
        "bucket_or_name",  # gcs argument
        "gcs_options",  # gcs argument
        "max_results",  # gcs argument
    }
    _type_lookup: ClassVar[  # This attribute is set in `MetaDatasource.__new__`
        TypeLookup
    ]
    # Setting this in a Datasource subclass will override the execution engine type.
    # The primary use case is to inject an execution engine for testing.
    execution_engine_override: ClassVar[Optional[Type[_ExecutionEngineT]]] = None  # type: ignore[misc]  # ClassVar cannot contain type variables

    # instance attrs
    type: str
    name: str
    id: Optional[uuid.UUID] = Field(default=None, description="Datasource id")
    assets: MutableMapping[str, DataAsset] = {}

    # private attrs
    _data_context: GXDataContext = pydantic.PrivateAttr()
    _cached_execution_engine_kwargs: Dict[str, Any] = pydantic.PrivateAttr({})
    _execution_engine: Union[_ExecutionEngineT, None] = pydantic.PrivateAttr(None)
    _config_provider: Union[_ConfigurationProvider, None] = pydantic.PrivateAttr(None)

    @pydantic.validator("assets", each_item=True)
    @classmethod
    def _load_asset_subtype(
        cls: Type[Datasource[_ExecutionEngineT]], data_asset: DataAsset
    ) -> DataAsset:
        """
        Some `data_asset` may be loaded as a less specific asset subtype different than
        what was intended.
        If a more specific subtype is needed the `data_asset` will be converted to a
        more specific `DataAsset`.
        """
        logger.debug(f"Loading '{data_asset.name}' asset ->\n{pf(data_asset, depth=4)}")
        asset_type_name: str = data_asset.type
        asset_type: Type[DataAsset] = cls._type_lookup[asset_type_name]

        if asset_type is type(data_asset):
            # asset is already the intended type
            return data_asset

        # strip out asset default kwargs
        kwargs = data_asset.dict(exclude_unset=True)
        logger.debug(f"{asset_type_name} - kwargs\n{pf(kwargs)}")

        asset_of_intended_type = asset_type(**kwargs)
        logger.debug(f"{asset_type_name} - {repr(asset_of_intended_type)}")
        return asset_of_intended_type

    def _execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Returns the execution engine to be used"""
        return self.execution_engine_override or self.execution_engine_type

    def get_execution_engine(self) -> _ExecutionEngineT:
        current_execution_engine_kwargs = self.dict(
            exclude=self._EXCLUDED_EXEC_ENG_ARGS, config_provider=self._config_provider
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            self._execution_engine = self._execution_engine_type()(
                **current_execution_engine_kwargs
            )
            self._cached_execution_engine_kwargs = current_execution_engine_kwargs
        return self._execution_engine

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that correspond to the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                build_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch_list_from_batch_request(batch_request)

    def get_asset(self, asset_name: str) -> DataAsset:
        """Returns the DataAsset referred to by name"""
        # This default implementation will be used if protocol is inherited
        try:
            self.assets[asset_name]._datasource = self
            return self.assets[asset_name]
        except KeyError as exc:
            raise LookupError(
                f"'{asset_name}' not found. Available assets are {list(self.assets.keys())}"
            ) from exc

    def _add_asset(
        self, asset: DataAsset, connect_options: dict | None = None
    ) -> DataAsset:
        """Adds an asset to a datasource

        Args:
            asset: The DataAsset to be added to this datasource.
        """
        # The setter for datasource is non-functional, so we access _datasource directly.
        # See the comment in DataAsset for more information.
        asset._datasource = self

        if not connect_options:
            connect_options = {}
        self._build_data_connector(asset, **connect_options)

        asset.test_connection()

        self.assets[asset.name] = asset

        return asset

    @staticmethod
    def parse_order_by_sorters(
        order_by: Optional[List[Union[Sorter, str, dict]]] = None
    ) -> List[Sorter]:
        order_by_sorters: list[Sorter] = []
        if order_by:
            for idx, sorter in enumerate(order_by):
                if isinstance(sorter, str):
                    if not sorter:
                        raise ValueError(
                            '"order_by" list cannot contain an empty string'
                        )
                    order_by_sorters.append(_sorter_from_str(sorter))
                elif isinstance(sorter, dict):
                    key: Optional[Any] = sorter.get("key")
                    reverse: Optional[Any] = sorter.get("reverse")
                    if key and reverse:
                        order_by_sorters.append(Sorter(key=key, reverse=reverse))
                    elif key:
                        order_by_sorters.append(Sorter(key=key))
                    else:
                        raise ValueError(
                            '"order_by" list dict must have a key named "key"'
                        )
                else:
                    order_by_sorters.append(sorter)
        return order_by_sorters

    # Abstract Methods
    @property
    def execution_engine_type(self) -> Type[_ExecutionEngineT]:
        """Return the ExecutionEngine type use for this Datasource"""
        raise NotImplementedError(
            """One needs to implement "execution_engine_type" on a Datasource subclass."""
        )

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the Datasource.

        Args:
            test_assets: If assets have been passed to the Datasource, an attempt can be made to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        raise NotImplementedError(
            """One needs to implement "test_connection" on a Datasource subclass."""
        )

    def _build_data_connector(self, data_asset: DataAsset, **kwargs) -> None:
        """Any Datasource subclass that utilizes DataConnector should overwrite this method.

        Specific implementations instantiate appropriate DataConnector class and set "self._data_connector" to it.

        Args:
            data_asset: DataAsset using this DataConnector instance
            kwargs: Extra keyword arguments allow specification of arguments used by particular DataConnector subclasses
        """
        pass

    # End Abstract Methods
