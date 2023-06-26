from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition, RuntimeBatchRequest
from great_expectations.core.batch_spec import (
    AzureBatchSpec,
    BatchMarkers,
    BatchSpec,
    PathBatchSpec,
    RuntimeDataBatchSpec,
    RuntimeQueryBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.datasource.data_connector.util import _build_asset_from_config

if TYPE_CHECKING:
    from great_expectations.datasource.data_connector.asset import Asset
    from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

DEFAULT_DELIMITER: str = "-"


@public_api
class RuntimeDataConnector(DataConnector):
    """A Data Connector that allows users to specify a Batch's data directly using a Runtime Batch Request.

    A Runtime Batch Request contains either an in-memory Pandas or Spark DataFrame, a filesystem or S3 path,
    or an arbitrary SQL query.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        batch_identifiers: A list of keys that must be defined in the batch identifiers dict of the Runtime Batch
            Request.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        execution_engine: ExecutionEngine,
        batch_identifiers: Optional[list] = None,
        batch_spec_passthrough: Optional[dict] = None,
        assets: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing RuntimeDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        if assets is None:
            assets = {}

        self._assets = assets
        self._batch_identifiers: dict = {}

        self._build_assets_from_config(config=assets)

        # add batch_identifiers defined at the DataConnector level.
        self._add_batch_identifiers(
            batch_identifiers=batch_identifiers, data_asset_name=None  # type: ignore[arg-type]
        )
        self._refresh_data_references_cache()

    @property
    def assets(self):
        return self._assets

    def _build_assets_from_config(self, config: Dict[str, dict]) -> None:
        """
        Read in asset configurations from RuntimeDataConnector. Build and load into assets property, and load
        batch_identifiers.

        Args:
            config (dict): Asset configurations at the DataConnector-level
        """

        name: str
        asset_config: dict

        for name, asset_config in config.items():
            if asset_config is None:
                asset_config = {}  # noqa: PLW2901

            asset_config.update({"name": name})
            new_asset: Asset = _build_asset_from_config(
                runtime_environment=self,
                config=asset_config,
            )
            self.assets[name] = new_asset
            self._add_batch_identifiers(
                batch_identifiers=new_asset.batch_identifiers,  # type: ignore[arg-type]
                data_asset_name=new_asset.name,
            )

    def _add_batch_identifiers(
        self,
        batch_identifiers: List[str],
        data_asset_name: Optional[str] = None,
    ) -> None:
        """
        Handles batch_identifiers that are configured at the DataConnector or Asset-level.
        batch_identifiers are added to the `self._batch_identifiers` cache.

            - Asset-level batch_identifiers are keyed by data_asset_name
            - DataConnector-level batch_identifiers are keyed by DataConnector-name

        Using DataConnector-level batch_identifiers also comes with a Deprecation warning.

        Args:
            batch_identifiers:  batch_identifiers from either DataConnector or Asset-level
            data_asset_name: if this value is not None, then we know the batch_identifiers are Asset-level
        """
        if data_asset_name:
            if not batch_identifiers:
                raise gx_exceptions.DataConnectorError(
                    f"""RuntimeDataConnector "{self.name}" requires batch_identifiers to be configured when specifying Assets."""
                )
            self._batch_identifiers[data_asset_name] = batch_identifiers
        else:
            if not batch_identifiers and len(self.assets) == 0:
                raise gx_exceptions.DataConnectorError(
                    f"""RuntimeDataConnector "{self.name}" requires batch_identifiers to be configured, either at the DataConnector or Asset-level."""
                )
            if batch_identifiers:
                self._batch_identifiers[self.name] = batch_identifiers

    def _refresh_data_references_cache(self) -> None:
        self._data_references_cache: dict = {}

    def _get_data_reference_list(
        self, data_asset_name: Optional[str] = None
    ) -> List[str]:
        """
        List objects in the cache to create a list of data_references. If data_asset_name is passed in, method will
        return all data_references for the named data_asset. If no data_asset_name is passed in, will return a list of
        all data_references for all data_assets in the cache.
        """
        if data_asset_name:
            return self._get_data_reference_list_from_cache_by_data_asset_name(
                data_asset_name
            )
        else:
            data_reference_list = [
                self._get_data_reference_list_from_cache_by_data_asset_name(
                    data_asset_name
                )
                for data_asset_name in self.get_available_data_asset_names()
            ]
            return data_reference_list  # type: ignore[return-value] # could be list of lists

    def get_data_reference_count(self) -> int:
        """
        Get number of data_references corresponding to all data_asset_names in cache. In cases where the
        RuntimeDataConnector has been passed a BatchRequest with the same data_asset_name but different
        batch_identifiers, it is possible to have more than one data_reference for a data_asset.
        """
        return sum(
            len(data_reference_dict)
            for key, data_reference_dict in self._data_references_cache.items()
        )

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[str]:
        """Fetch data_references corresponding to data_asset_name from the cache."""
        data_references_for_data_asset_name = self._data_references_cache.get(
            data_asset_name
        )
        if data_references_for_data_asset_name is not None:
            return list(data_references_for_data_asset_name.keys())
        else:
            return []

    def get_unmatched_data_references(self) -> List[str]:
        return []

    @public_api
    def get_available_data_asset_names(self) -> List[str]:
        """Returns a list of data_assets that are both defined at runtime, and defined in DataConnector configuration"""
        defined_assets: List[str] = list(self.assets.keys())
        data_reference_keys: List[str] = list(self._data_references_cache.keys())
        available_assets: List[str] = list(set(defined_assets + data_reference_keys))

        sorted_available_assets: List[str] = sorted(available_assets)
        return sorted_available_assets

    # noinspection PyMethodOverriding
    def get_batch_data_and_metadata(  # type: ignore[override]
        self,
        batch_definition: BatchDefinition,
        runtime_parameters: dict,
    ) -> Tuple[Any, BatchSpec, BatchMarkers]:  # batch_data
        batch_spec: RuntimeDataBatchSpec = self.build_batch_spec(  # type: ignore[assignment]
            batch_definition=batch_definition,
            runtime_parameters=runtime_parameters,
        )
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        self._execution_engine.load_batch_data(batch_definition.id, batch_data)  # type: ignore[arg-type] # got ExecutionEngine
        return (
            batch_data,  # type: ignore[return-value]
            batch_spec,
            batch_markers,
        )

    def get_batch_definition_list_from_batch_request(  # type: ignore[override] # BatchRequestBase
        self,
        batch_request: RuntimeBatchRequest,
    ) -> List[BatchDefinition]:
        return self._get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    def _get_batch_definition_list_from_batch_request(
        self,
        batch_request: RuntimeBatchRequest,
    ) -> List[BatchDefinition]:
        """
        <Will> 202103. The following behavior of the _data_references_cache follows a pattern that we are using for
        other data_connectors, including variations of FilePathDataConnector. When BatchRequest contains batch_data
        that is passed in as a in-memory dataframe, the cache will contain the names of all data_assets
        (and data_references) that have been passed into the RuntimeDataConnector in this session, even though technically
        only the most recent batch_data is available. This can be misleading. However, allowing the RuntimeDataConnector
        to keep a record of all data_assets (and data_references) that have been passed in will allow for the proposed
        behavior of RuntimeBatchRequest which will allow for paths and queries to be passed in as part of the BatchRequest.
        Therefore this behavior will be revisited when the design of RuntimeBatchRequest and related classes are complete.
        """
        self._validate_batch_request(batch_request=batch_request)

        batch_identifiers: Optional[dict] = None
        if batch_request.batch_identifiers:
            self._validate_batch_identifiers(
                data_asset_name=batch_request.data_asset_name,
                batch_identifiers=batch_request.batch_identifiers,
            )
            batch_identifiers = batch_request.batch_identifiers

        if not batch_identifiers:
            raise gx_exceptions.DataConnectorError(
                "Passed in a RuntimeBatchRequest with no batch_identifiers"
            )

        batch_definition_list: List[BatchDefinition]
        batch_definition = BatchDefinition(
            datasource_name=self.datasource_name,
            data_connector_name=self.name,
            data_asset_name=batch_request.data_asset_name,
            batch_identifiers=IDDict(batch_identifiers),
            batch_spec_passthrough=batch_request.batch_spec_passthrough,
        )
        batch_definition_list = [batch_definition]
        self._update_data_references_cache(
            batch_request.data_asset_name,
            batch_definition_list,
            IDDict(batch_identifiers),
        )
        return batch_definition_list

    def _update_data_references_cache(
        self,
        data_asset_name: str,
        batch_definition_list: List,
        batch_identifiers: IDDict,
    ) -> None:
        data_reference = self._get_data_reference_name(batch_identifiers)
        if data_asset_name not in self._data_references_cache:
            # add
            self._data_references_cache[data_asset_name] = {
                data_reference: batch_definition_list
            }
            # or replace
        else:
            self._data_references_cache[data_asset_name][
                data_reference
            ] = batch_definition_list

    def _self_check_fetch_batch(
        self,
        pretty_print,
        example_data_reference,
        data_asset_name,
    ):
        return {}

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        data_asset_name: str = batch_definition.data_asset_name
        return {"data_asset_name": data_asset_name}

    # This method is currently called called only in tests.
    # noinspection PyMethodOverriding
    def build_batch_spec(  # type: ignore[return,override]
        self,
        batch_definition: BatchDefinition,
        runtime_parameters: dict,
    ) -> Union[RuntimeDataBatchSpec, RuntimeQueryBatchSpec, PathBatchSpec]:
        self._validate_runtime_parameters(runtime_parameters=runtime_parameters)
        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        if "batch_data" in runtime_parameters:
            batch_spec["batch_data"] = runtime_parameters.get("batch_data")
            return RuntimeDataBatchSpec(batch_spec)
        elif "query" in runtime_parameters:
            batch_spec["query"] = runtime_parameters.get("query")
            batch_spec["temp_table_schema_name"] = runtime_parameters.get(
                "temp_table_schema_name"
            )
            return RuntimeQueryBatchSpec(batch_spec)
        elif "path" in runtime_parameters:
            path: str = runtime_parameters["path"]
            batch_spec["path"] = path
            if "s3" in path:
                return S3BatchSpec(batch_spec)
            elif "blob.core.windows.net" in path:
                return AzureBatchSpec(batch_spec)
            else:
                return PathBatchSpec(batch_spec)

    @staticmethod
    def _get_data_reference_name(
        batch_identifiers: IDDict,
    ) -> str:
        if batch_identifiers is None:
            batch_identifiers = IDDict({})
        data_reference_name = DEFAULT_DELIMITER.join(
            [str(value) for value in batch_identifiers.values()]
        )
        return data_reference_name

    @staticmethod
    def _validate_runtime_parameters(runtime_parameters: Union[dict, None]) -> None:
        if not isinstance(runtime_parameters, dict):
            raise TypeError(
                f"""The type of runtime_parameters must be a dict object. The type given is
        "{str(type(runtime_parameters))}", which is illegal.
                        """
            )
        keys_present: List[str] = [
            key
            for key, val in runtime_parameters.items()
            if val is not None and key in ["batch_data", "query", "path"]
        ]
        if len(keys_present) != 1:
            raise gx_exceptions.InvalidBatchRequestError(
                "The runtime_parameters dict must have one (and only one) of the following keys: 'batch_data', "
                "'query', 'path'."
            )

    def _validate_batch_request(self, batch_request: RuntimeBatchRequest) -> None:  # type: ignore[override]
        super()._validate_batch_request(batch_request=batch_request)

        runtime_parameters = batch_request.runtime_parameters
        batch_identifiers = batch_request.batch_identifiers

        if not (
            (not runtime_parameters and not batch_identifiers)
            or (runtime_parameters and batch_identifiers)
        ):
            raise gx_exceptions.DataConnectorError(
                f"""RuntimeDataConnector "{self.name}" requires runtime_parameters and batch_identifiers to be both
                present and non-empty or both absent in the batch_request parameter."""
            )

        if runtime_parameters:
            self._validate_runtime_parameters(runtime_parameters=runtime_parameters)

    def _validate_batch_identifiers(
        self, data_asset_name: str, batch_identifiers: dict
    ) -> None:
        """
        Called by _get_batch_definition_list_from_batch_request() ie, when a RuntimeBatchRequest is passed in.

        The batch_identifiers in the RuntimeBatchRequest will need to validated against two types of batch_identifiers.
            1. Defined at the Asset-level
            2. Defined at the DataConnector-level

        - Asset-level batch_identifiers should only be accessible from the assets that are configured as part of the DataConnector config.
        - DataConnector-level batch_identifiers should only be accessible from data assets built at runtime, named by the RuntimeBatchRequest.

        This method will validate the two types of batch_identifiers by calling
            - _validate_asset_level_batch_identifiers()
            - _validate_data_connector_level_batch_identifiers()

        Args:
            data_asset_name: name specified by RuntimeBatchRequest
            batch_identifiers: identifiers to validate
        """
        configured_asset_names: List[str] = list(self.assets.keys())
        if data_asset_name in configured_asset_names:
            self._validate_asset_level_batch_identifiers(
                data_asset_name=data_asset_name, batch_identifiers=batch_identifiers
            )
        else:
            self._validate_data_connector_level_batch_identifiers(
                batch_identifiers=batch_identifiers
            )

    def _validate_asset_level_batch_identifiers(
        self, data_asset_name: str, batch_identifiers: dict
    ) -> None:
        """
        Check that batch_identifiers passed in are an exact match to the ones configured at the Asset-level
        """
        asset: Asset = self.assets[data_asset_name]
        batch_identifiers_keys: List[str] = list(batch_identifiers.keys())
        if not set(batch_identifiers_keys) == set(asset.batch_identifiers):  # type: ignore[arg-type]
            raise gx_exceptions.DataConnectorError(
                f"""
                Data Asset {data_asset_name} was invoked with one or more batch_identifiers
                that were not configured for the asset.

                The Data Asset was configured with : {asset.batch_identifiers}
                It was invoked with : {batch_identifiers_keys}
                """
            )

    def _validate_data_connector_level_batch_identifiers(
        self, batch_identifiers: dict
    ) -> None:
        """
        Check that batch_identifiers passed in are a subset of the ones configured at the DataConnector-level
        """
        batch_identifiers_keys: List[str] = list(batch_identifiers.keys())
        if not set(batch_identifiers_keys) <= set(self._batch_identifiers[self.name]):
            raise gx_exceptions.DataConnectorError(
                f"""RuntimeDataConnector {self.name} was invoked with one or more batch identifiers that do not
        appear among the configured batch identifiers.

                The RuntimeDataConnector was configured with : {self._batch_identifiers[self.name]}
                It was invoked with : {batch_identifiers_keys}
                """
            )

    def self_check(self, pretty_print=True, max_examples=3):
        """
        Overrides the self_check method for RuntimeDataConnector. This method currently supports 2 modes of usage:
            1. user has configured Assets at the RuntimeDataConnector-level (preferred).
            2. user has not configured Assets and will pass in `data_asset_name` with the RuntimeBatchRequest.

        In the case of #1, the get_available_data_asset_names() will return the list of configured Assets and base
        self_check() will be called.

        In the case of #2  there are no example data_asset_names until the data is passed in through the
        RuntimeBatchRequest. Therefore, there will be a note displayed to the user saying that RuntimeDataConnector
        will not have data_asset_names until they are passed in through RuntimeBatchRequest.

        Args:
            pretty_print (bool): should the output be printed?
            max_examples (int): how many data_references should be printed?
        Returns:
            report_obj (dict): dictionary containing self_check output
        """
        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        if pretty_print:
            print(f"\t{self.name}:{self.__class__.__name__}\n")
        asset_names: List[str] = self.get_available_data_asset_names()
        len_asset_names: int = len(asset_names)

        if len_asset_names > 0:
            return super().self_check()
        else:
            report_obj: dict = {
                "class_name": self.__class__.__name__,
                "data_asset_count": len_asset_names,
                "example_data_asset_names": asset_names[:max_examples],
                "data_assets": {},
                "note": "RuntimeDataConnector will not have data_asset_names until they are passed in through RuntimeBatchRequest",
            }
            if pretty_print:
                print(
                    f"\tAvailable data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):"
                )
                print(
                    "\t\t"
                    + "Note : RuntimeDataConnector will not have data_asset_names until they are passed in through RuntimeBatchRequest"
                )

            unmatched_data_references: List[str] = self.get_unmatched_data_references()
            len_unmatched_data_references: int = len(unmatched_data_references)

            if pretty_print:
                if pretty_print:
                    print(
                        f"\n\tUnmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}): {unmatched_data_references[:max_examples]}\n"
                    )

            report_obj["unmatched_data_reference_count"] = len_unmatched_data_references
            report_obj["example_unmatched_data_references"] = unmatched_data_references[
                :max_examples
            ]
            return report_obj
