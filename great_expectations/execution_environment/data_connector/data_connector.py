# -*- coding: utf-8 -*-
import copy
import itertools
from typing import List, Dict, Union, Callable, Any, Tuple, Optional
from ruamel.yaml.comments import CommentedMap
import json
import re
from string import Template
import sre_parse
import sre_constants

import logging

from great_expectations.data_context.types.base import (
    PartitionerConfig,
    partitionerConfigSchema
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.partition_request import (
    PartitionRequest,
    build_partition_request
)
from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinitionSubset,
    PartitionDefinition,
    BatchSpec,
)
from great_expectations.core.batch import (
    BatchMarkers,
    BatchDefinition,
)
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


class DataConnector(object):
    """
    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines
    can use to get individual batches of data. They add flexibility in how to obtain data
    such as with time-based partitioning, downsampling, or other techniques appropriate
    for the ExecutionEnvironment.

    For example, a DataConnector could produce a SQL query that logically represents "rows in
    the Events table with a timestamp on February 7, 2012," which a SqlAlchemyExecutionEnvironment
    could use to materialize a SqlAlchemyDataset corresponding to that batch of data and
    ready for validation.

    A batch is a sample from a data asset, sliced according to a particular rule. For
    example, an hourly slide of the Events table or “most recent `users` records.”

    A Batch is the primary unit of validation in the Great Expectations DataContext.
    Batches include metadata that identifies how they were constructed--the same “batch_spec”
    assembled by the data connector, While not every ExecutionEnvironment will enable re-fetching a
    specific batch of data, GE can store snapshots of batches or store metadata from an
    external data version control system.
    """
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine: ExecutionEngine = None,
    ):
        self._name = name
        self._execution_environment_name = execution_environment_name
        self._execution_engine = execution_engine

        # This is a dictionary which maps data_references onto batch_requests.
        self._data_references_cache = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def execution_environment_name(self) -> str:
        return self._execution_environment_name

    def get_batch_data_and_metadata_from_batch_definition(
        self,
        batch_definition: BatchDefinition,
    ) -> Tuple[
        Any, #batch_data
        BatchSpec,
        BatchMarkers,
    ]:
        batch_spec: BatchSpec = self._build_batch_spec_from_batch_definition(
            batch_definition=batch_definition
        )
        batch_data, batch_markers = self._execution_engine.get_batch_data_and_markers(batch_spec=batch_spec)
        return (
            batch_data,
            batch_spec,
            batch_markers,
        )

    def _build_batch_spec_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> BatchSpec:
        batch_spec_params: dict = self._generate_batch_spec_parameters_from_batch_definition(
            batch_definition=batch_definition
        )
        # TODO Abe 20201018: Decide if we want to allow batch_spec_passthrough parameters anywhere.
        batch_spec: BatchSpec = BatchSpec(
            **batch_spec_params
        )

        return batch_spec

    def refresh_data_references_cache(
        self,
    ):
        raise NotImplementedError

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.	
        This method is used to refresh the cache.
        """
        raise NotImplementedError

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name: str) -> List[Any]:
        """
        Fetch data_references corresponding to data_asset_name from the cache.
        """
        raise NotImplementedError

    def get_data_reference_list_count(self) -> int:
        raise NotImplementedError

    def get_unmatched_data_references(self) -> List[Any]:
        raise NotImplementedError

    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this data connector.

        Returns:
            A list of available names
        """
        raise NotImplementedError

    def get_batch_definition_list_from_batch_request(
        self,
        batch_request: BatchRequest,
    ) -> List[BatchDefinition]:
        raise NotImplementedError

    def _map_data_reference_to_batch_definition_list(
        self,
        data_reference: Any,
        data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        raise NotImplementedError

    def _map_batch_definition_to_data_reference(self, batch_definition: BatchDefinition) -> Any:
        raise NotImplementedError

    def _generate_batch_spec_parameters_from_batch_definition(
        self,
        batch_definition: BatchDefinition
    ) -> dict:
        raise NotImplementedError

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
            "data_assets": {}
            # "data_reference_count": self.
        }

        if pretty_print:
            print(f"\tAvailable data_asset_names ({min(len_asset_names, max_examples)} of {len_asset_names}):")

        for asset_name in asset_names[:max_examples]:
            data_reference_list = self._get_data_reference_list_from_cache_by_data_asset_name(asset_name)
            len_batch_definition_list = len(data_reference_list)
            example_data_references = data_reference_list[:max_examples]

            if pretty_print:
                print(f"\t\t{asset_name} ({min(len_batch_definition_list, max_examples)} of {len_batch_definition_list}):", example_data_references)

            data_connector_obj["data_assets"][asset_name] = {
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

    def _validate_batch_request(self, batch_request: BatchRequest):
        if not (
            batch_request.execution_environment_name is None
            or batch_request.execution_environment_name == self.execution_environment_name
        ):
            raise ValueError(
                f'''execution_envrironment_name in BatchRequest: "{batch_request.execution_environment_name}" does not match DataConnector execution_environment_name:
"{self.execution_environment_name}".
                '''
            )
        if not (batch_request.data_connector_name is None or batch_request.data_connector_name == self.name):
            raise ValueError(
                f'data_connector_name in BatchRequest: "{batch_request.data_connector_name}" does not match DataConnector name: "{self.name}".'
            )
