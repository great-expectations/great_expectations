import os
from typing import List, Optional
from pathlib import Path
import copy

import logging

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    map_data_reference_string_to_batch_definition_list_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
)

logger = logging.getLogger(__name__)


class SinglePartitionDataConnector(DataConnector):
    """SinglePartitionDataConnector is a base class for DataConnectors that require exactly one Partitioner be configured in the declaration.

    Instead, its data_references are stored in a data_reference_dictionary : {
        "pretend/path/A-100.csv" : pandas_df_A_100,
        "pretend/path/A-101.csv" : pandas_df_A_101,
        "pretend/directory/B-1.csv" : pandas_df_B_1,
        "pretend/directory/B-2.csv" : pandas_df_B_2,
        ...
    }
    """
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        default_regex: dict = None,
        base_directory: str = None,
        glob_directive: str = "*",
    ):
        logger.debug(f'Constructing SinglePartitionDataConnector "{name}".')

        self.base_directory = base_directory
        self.glob_directive = glob_directive
        if default_regex is None:
            default_regex = {}
        self._default_regex = default_regex

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=None,
        )

    def refresh_data_references_cache(self):
        """
        """
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list: List[BatchDefinition] = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference,
                data_asset_name=None
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[str]:
        """Fetch data_references corresponding to data_asset_name from the cache.
        """
        # TODO: <Alex>This needs to be looked at.</Alex>
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
            )
        )

        regex_config: dict = copy.deepcopy(self._default_regex)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]

        path_list: List[str] = [
            map_batch_definition_to_data_reference_string_using_regex(
                batch_definition=batch_definition,
                regex_pattern=pattern,
                group_names=group_names
            )
            for batch_definition in batch_definition_list
        ]

        # TODO: Sort with a real sorter here
        path_list.sort()

        return path_list

    # TODO: <Alex>This method should be implemented in every subclass.</Alex>
    # def _get_data_reference_list(self) -> List[str]:
    #     pass

    def get_data_reference_list_count(self) -> int:
        return len(self._data_references_cache)

    def get_unmatched_data_references(self) -> List[str]:
        if self._data_references_cache is None:
            raise ValueError('_data_references_cache is None.  Have you called "refresh_data_references_cache()" yet?')

        return [k for k, v in self._data_references_cache.items() if v is None]

    def get_available_data_asset_names(self) -> List[str]:
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        # This will fetch ALL batch_definitions in the cache
        batch_definition_list: List[BatchDefinition] = self.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
            )
        )

        data_asset_names: set = set()
        for batch_definition in batch_definition_list:
            data_asset_names.add(batch_definition.data_asset_name)

        return list(data_asset_names)

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        if batch_request.data_connector_name != self.name:
            raise ValueError(
                f'data_connector_name "{batch_request.data_connector_name}" does not match name "{self.name}".'
            )

        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        batch_definition_list: List[BatchDefinition] = []
        # TODO: <Alex>A cleaner implementation would be a filter on sub_cache.values() with "batch_definition_matches_batch_request()" as condition, since "data_reference" is not involved.</Alex>
        for data_reference, batch_definition in self._data_references_cache.items():
            if batch_definition is not None:
                if batch_definition_matches_batch_request(
                    batch_definition=batch_definition[0],
                    batch_request=batch_request
                ):
                    batch_definition_list.extend(batch_definition)

        return batch_definition_list

    # TODO: <Alex>Should this method be moved to SinglePartitionFileDataConnector?</Alex>
    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: str,
        data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        regex_config: dict = copy.deepcopy(self._default_regex)
        pattern: str = regex_config["pattern"]
        group_names: List[str] = regex_config["group_names"]

        return map_data_reference_string_to_batch_definition_list_using_regex(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
            data_asset_name=data_asset_name,
            data_reference=data_reference,
            regex_pattern=pattern,
            group_names=group_names
        )

    # TODO: <Alex>This method should be implemented in every subclass.</Alex>
    # def _map_batch_definition_to_data_reference(self, batch_definition: BatchDefinition) -> str:
    #     pass

    # TODO: <Alex>This method should be implemented in every subclass.</Alex>
    # def _generate_batch_spec_parameters_from_batch_definition(
    #     self,
    #     batch_definition: BatchDefinition
    # ) -> dict:
    #     pass


class SinglePartitionDictDataConnector(SinglePartitionDataConnector):
    def __init__(
        self,
        name: str,
        data_reference_dict: dict = None,
        # TODO: <Alex>Are these "kwargs" needed here?</Alex>
        **kwargs,
    ):
        if data_reference_dict is None:
            data_reference_dict = {}
        logger.debug(f'Constructing SinglePartitionDictDataConnector "{name}".')
        super().__init__(
            name,
            # TODO: <Alex>Are these "kwargs" needed here?</Alex>
            **kwargs
        )

        # This simulates the underlying filesystem
        self.data_reference_dict = data_reference_dict

    def _get_data_reference_list(self):
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        data_reference_keys = list(self.data_reference_dict.keys())
        data_reference_keys.sort()
        return data_reference_keys


class SinglePartitionFileDataConnector(SinglePartitionDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        base_directory: str,
        default_regex: dict,
        glob_directive: str = "*",
    ):
        logger.debug(f'Constructing SinglePartitionFileDataConnector "{name}".')

        self.glob_directive = glob_directive

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            base_directory=base_directory,
            glob_directive=glob_directive,
            default_regex=default_regex,
        )

    def _get_data_reference_list(self):
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        globbed_paths = Path(self.base_directory).glob(self.glob_directive)
        path_list: List[str] = [os.path.relpath(str(posix_path), self.base_directory) for posix_path in globbed_paths]

        return path_list

    # TODO: <Alex>Why does this need to override SinglePartitionDataConnector.get_available_data_asset_names()?  The results must be identical.</Alex>
    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        available_data_asset_names: List[str] = []

        for k, v in self._data_references_cache.items():
            if v is not None:
                batch_definition: BatchDefinition = v[0]
                available_data_asset_names.append(batch_definition.data_asset_name)

        return list(set(available_data_asset_names))
