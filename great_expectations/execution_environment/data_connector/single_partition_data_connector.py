import logging
from typing import Union, List, Any, Optional
from pathlib import Path
import copy

from great_expectations.core.id_dict import (
    PartitionRequest,
    PartitionDefinitionSubset,
    PartitionDefinition
)
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.data_connector import DataConnector

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
        # TODO: <Alex>Delete deprecated/unused code.</Alex>
        # assets: dict = None,
        # partitioner: dict = None,
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

        # TODO: <Alex>Delete deprecated/unused code.</Alex>
        # if partitioner is None:
        #     partitioner = {}
        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            # TODO: <Alex>Delete deprecated/unused code.</Alex>
            # assets=assets,
            # partitioners={
            #     "ONE_AND_ONLY_PARTITIONER" : partitioner
            # },
            # default_partitioner_name="ONE_AND_ONLY_PARTITIONER",
            execution_engine=None,
            # data_context_root_directory=None
        )

    def get_available_data_asset_names(self):
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        # This will fetch ALL batch_definitions in the cache
        batch_definition_list = self.get_batch_definition_list_from_batch_request(BatchRequest(
            execution_environment_name=self.execution_environment_name,
            data_connector_name=self.name,
        ))

        data_asset_names = set()
        for batch_definition in batch_definition_list:
            data_asset_names.add(batch_definition.data_asset_name)
        return list(data_asset_names)

    def self_check(
        self,
        pretty_print=True,
        max_examples=3
    ):
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        if pretty_print:
            print("\t"+self.name, ":", self.__class__.__name__)
            print()

        asset_names = self.get_available_data_asset_names()
        asset_names.sort()
        len_asset_names = len(asset_names)

        data_connector_obj = {
            "class_name": self.__class__.__name__,
            "data_asset_count": len_asset_names,
            "example_data_asset_names": asset_names[:max_examples],
            "assets": {}
            # "data_reference_count": self.
        }

        if pretty_print:
            print(f"\tAvailable data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):")
        for asset_name in asset_names[:max_examples]:
            batch_definition_list = self.get_batch_definition_list_from_batch_request(BatchRequest(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=asset_name,
            ))
            len_batch_definition_list = len(batch_definition_list)
            example_data_references = [
                self.convert_batch_request_to_data_reference(
                    batch_request=BatchRequest(
                        execution_environment_name=batch_definition.execution_environment_name,
                        data_connector_name=batch_definition.data_connector_name,
                        data_asset_name=batch_definition.data_asset_name,
                        partition_request=batch_definition.partition_definition,
                    ),
                    pattern=self._default_regex["pattern"],
                    group_names=self._default_regex["group_names"],
                )
                for batch_definition in batch_definition_list
            ][:max_examples]
            example_data_references.sort()

            if pretty_print:
                print(f"\t\t{asset_name} ({min(len_batch_definition_list, max_examples)} of {len_batch_definition_list}):", example_data_references)

            data_connector_obj["assets"][asset_name] = {
                "batch_definition_count": len_batch_definition_list,
                "example_data_references": example_data_references
            }

        unmatched_data_references = self.get_unmatched_data_references()
        len_unmatched_data_references = len(unmatched_data_references)
        if pretty_print:
            print(f"\n\tUnmatched data_references ({min(len_unmatched_data_references, max_examples)} of {len_unmatched_data_references}):", unmatched_data_references[:max_examples])
        data_connector_obj["unmatched_data_reference_count"] = len_unmatched_data_references
        data_connector_obj["example_unmatched_data_references"] = unmatched_data_references[:max_examples]
        return data_connector_obj

    def refresh_data_references_cache(
        self,
    ):
        """
        """
        # Map data_references to batch_definitions
        self._data_references_cache = {}

        for data_reference in self._get_data_reference_list():
            mapped_batch_definition_list = self._map_data_reference_to_batch_definition_list(
                data_reference=data_reference,
                data_asset_name=None
            )
            self._data_references_cache[data_reference] = mapped_batch_definition_list

    def get_data_reference_list_count(self):
        return len(self._data_references_cache)

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: Any,
        data_asset_name: Optional[str]
    ) -> Optional[List[BatchDefinition]]:
        regex_config = copy.deepcopy(self._default_regex)

        batch_request: BatchRequest = self.convert_data_reference_to_batch_request(
            data_reference=data_reference,
            pattern=regex_config["pattern"],
            group_names=regex_config["group_names"],
        )
        if batch_request is None:
            return None
        
        return [
            BatchDefinition(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=batch_request.data_asset_name,
                partition_definition=PartitionDefinition(**batch_request.partition_request),
            )
        ]

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        if batch_request.data_connector_name != self.name:
            raise ValueError(f"data_connector_name {batch_request.data_connector_name} does not match name {self.name}.")

        if self._data_references_cache == None:
            self.refresh_data_references_cache()
        
        batches = []
        for data_reference, batch_definition in self._data_references_cache.items():
            if batch_definition == None:
                # The data_reference is unmatched.
                continue
            if self._batch_definition_matches_batch_request(batch_definition[0], batch_request):
                batches += batch_definition

        return batches

    def get_unmatched_data_references(self):
        if self._data_references_cache is None:
            raise ValueError("_data_references_cache is None. Have you called refresh_data_references_cache yet?")

        return [k for k,v in self._data_references_cache.items() if v == None]


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
        globbed_paths = Path(self.base_directory).glob(self.glob_directive)
        path_list = [
            str(posix_path) for posix_path in globbed_paths
        ]

        # Trim paths to exclude the base_directory
        base_directory_len = len(str(self.base_directory))
        path_list = [path[base_directory_len:] for path in path_list]
        return path_list

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        if self._data_references_cache is None:
            self.refresh_data_references_cache()

        available_data_asset_names = []

        for k, v in self._data_references_cache.items():
            if v is not None:
                batch_definition: BatchDefinition = v[0]
                available_data_asset_names.append(batch_definition.data_asset_name)

        return list(set(available_data_asset_names))