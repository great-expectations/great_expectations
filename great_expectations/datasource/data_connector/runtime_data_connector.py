import logging
from typing import Any, List, Optional, Tuple, Union, cast
from urllib.parse import urlparse

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
)
from great_expectations.core.batch_spec import (
    BatchMarkers,
    BatchSpec,
    PathBatchSpec,
    RuntimeDataBatchSpec,
    RuntimeQueryBatchSpec,
    S3BatchSpec,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.data_connector.data_connector import DataConnector
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

DEFAULT_DELIMITER: str = "-"


class RuntimeDataConnector(DataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        batch_identifiers: Optional[list] = None,
    ):
        logger.debug(f'Constructing RuntimeDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
        )

        self._batch_identifiers = batch_identifiers
        self._refresh_data_references_cache()

    def _refresh_data_references_cache(self):
        self._data_references_cache = {}

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
            return data_reference_list

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

    def get_data_reference_list_count(self) -> int:
        """
        Get number of data_references corresponding to all data_asset_names in cache. In cases where the
        RuntimeDataConnector has been passed a BatchRequest with the same data_asset_name but different
        batch_identifiers, it is possible to have more than one data_reference for a data_asset.
        """
        return sum(
            len(data_reference_dict)
            for key, data_reference_dict in self._data_references_cache.items()
        )

    def get_unmatched_data_references(self) -> List[str]:
        return []

    def get_available_data_asset_names(self) -> List[str]:
        """Please see note in : _get_batch_definition_list_from_batch_request()"""
        return list(self._data_references_cache.keys())

    # noinspection PyMethodOverriding
    def get_batch_data_and_metadata(
        self,
        batch_definition: BatchDefinition,
        runtime_parameters: dict,
    ) -> Tuple[Any, BatchSpec, BatchMarkers,]:  # batch_data
        batch_spec: RuntimeDataBatchSpec = self.build_batch_spec(
            batch_definition=batch_definition,
            runtime_parameters=runtime_parameters,
        )
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )
        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: RuntimeBatchRequest,
    ) -> List[BatchDefinition]:
        return self._get_batch_definition_list_from_batch_request(
            batch_request=batch_request
        )

    def _get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
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
                batch_identifiers=batch_request.batch_identifiers
            )
            batch_identifiers = batch_request.batch_identifiers
        if not batch_identifiers:
            batch_identifiers = {}

        batch_definition_list: List[BatchDefinition]
        batch_definition: BatchDefinition = BatchDefinition(
            datasource_name=self.datasource_name,
            data_connector_name=self.name,
            data_asset_name=batch_request.data_asset_name,
            batch_identifiers=IDDict(batch_identifiers),
            batch_spec_passthrough=batch_request.batch_spec_passthrough,
        )
        batch_definition_list = [batch_definition]
        self._update_data_references_cache(
            batch_request.data_asset_name, batch_definition_list, batch_identifiers
        )
        return batch_definition_list

    def _update_data_references_cache(
        self,
        data_asset_name: str,
        batch_definition_list: List,
        batch_identifiers: IDDict,
    ):
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

    # This method is currently called called only in tests.
    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        return {}

    # This method is currently called called only in tests.
    # noinspection PyMethodOverriding
    def build_batch_spec(
        self,
        batch_definition: BatchDefinition,
        runtime_parameters: dict,
    ) -> Union[RuntimeDataBatchSpec, RuntimeQueryBatchSpec, PathBatchSpec]:
        self._validate_runtime_parameters(runtime_parameters=runtime_parameters)
        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        if runtime_parameters.get("batch_data") is not None:
            batch_spec["batch_data"] = runtime_parameters.get("batch_data")
            return RuntimeDataBatchSpec(batch_spec)
        elif runtime_parameters.get("query"):
            batch_spec["query"] = runtime_parameters.get("query")
            return RuntimeQueryBatchSpec(batch_spec)
        elif runtime_parameters.get("path"):
            path = runtime_parameters.get("path")
            batch_spec["path"] = path
            parsed_url = urlparse(path)
            if "s3" in parsed_url.scheme:
                return S3BatchSpec(batch_spec)
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
    def _validate_runtime_parameters(runtime_parameters: Union[dict, type(None)]):
        if not isinstance(runtime_parameters, dict):
            raise TypeError(
                f"""The type of runtime_parameters must be a dict object. The type given is
        "{str(type(runtime_parameters))}", which is illegal.
                        """
            )
        keys_present = [
            key
            for key, val in runtime_parameters.items()
            if val is not None and key in ["batch_data", "query", "path"]
        ]
        if len(keys_present) != 1:
            raise ge_exceptions.InvalidBatchRequestError(
                "The runtime_parameters dict must have one (and only one) of the following keys: 'batch_data', "
                "'query', 'path'."
            )

    def _validate_batch_request(self, batch_request: BatchRequestBase):
        super()._validate_batch_request(batch_request=batch_request)

        runtime_parameters = batch_request.runtime_parameters
        batch_identifiers = batch_request.batch_identifiers
        if not (
            (not runtime_parameters and not batch_identifiers)
            or (runtime_parameters and batch_identifiers)
        ):
            raise ge_exceptions.DataConnectorError(
                f"""RuntimeDataConnector "{self.name}" requires runtime_parameters and batch_identifiers to be both 
                present and non-empty or
                both absent in the batch_request parameter.
                """
            )
        if runtime_parameters:
            self._validate_runtime_parameters(runtime_parameters=runtime_parameters)

    def _validate_batch_identifiers(self, batch_identifiers: dict):
        if batch_identifiers is None:
            batch_identifiers = {}
        self._validate_batch_identifiers_configuration(
            batch_identifiers=list(batch_identifiers.keys())
        )

    def _validate_batch_identifiers_configuration(self, batch_identifiers: List[str]):
        if batch_identifiers and len(batch_identifiers) > 0:
            if not (
                self._batch_identifiers
                and set(batch_identifiers) <= set(self._batch_identifiers)
            ):
                raise ge_exceptions.DataConnectorError(
                    f"""RuntimeDataConnector "{self.name}" was invoked with one or more batch identifiers that do not
appear among the configured batch identifiers.
                    """
                )
