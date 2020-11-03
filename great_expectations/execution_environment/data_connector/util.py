# -*- coding: utf-8 -*-

# Utility methods for dealing with DataConnector objects

from typing import List, Dict, Union, Callable, Any, Tuple, Optional
import copy
import re
import sre_parse
import sre_constants

import logging

from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import (
    PartitionDefinitionSubset,
    PartitionDefinition,
)
from great_expectations.core.batch import BatchDefinition
from great_expectations.execution_environment.data_connector.partition_query import PartitionQuery
import great_expectations.exceptions as ge_exceptions

from great_expectations.execution_environment.data_connector.sorter import Sorter

from great_expectations.data_context.util import (
    instantiate_class_from_config
)

logger = logging.getLogger(__name__)


DEFAULT_DATA_ASSET_NAME: str = "DEFAULT_ASSET_NAME"


def batch_definition_matches_batch_request(
    batch_definition: BatchDefinition,
    batch_request: BatchRequest,
) -> bool:
    assert isinstance(batch_definition, BatchDefinition)
    assert isinstance(batch_request, BatchRequest)

    if batch_request.execution_environment_name:
        if batch_request.execution_environment_name != batch_definition.execution_environment_name:
            return False
    if batch_request.data_connector_name:
        if batch_request.data_connector_name != batch_definition.data_connector_name:
            return False
    if batch_request.data_asset_name:
        if batch_request.data_asset_name != batch_definition.data_asset_name:
            return False

    if batch_request.partition_request:
        assert isinstance(batch_request.partition_request, dict)
        partition_query = batch_request.partition_request.get("partition_query")
        if partition_query:
            assert isinstance(partition_query, dict)
            for key in partition_query.keys():
                if not (
                        key in batch_definition.partition_definition and batch_definition.partition_definition[key] ==
                        partition_query[key]
                ):
                    return False
    return True


def map_data_reference_string_to_batch_definition_list_using_regex(
    execution_environment_name: str,
    data_connector_name: str,
    data_asset_name: str,
    data_reference: str,
    regex_pattern: str,
    group_names: List[str],
) -> Optional[List[BatchDefinition]]:
    # TODO: <Alex>Per Abe: The two-steps process involving a BatchRequest intermediary can be simplified by turning
    # convert_data_reference_string_to_batch_request_using_regex() into convert_data_reference_string_to_batch_definition_using_regex()
    # and passing it all the arguments it needs to create a BatchDefinition.
    # We could not do it before, because the Partitioner difd not have all the required arguments. But the DataConnector does have them.
    #</Alex>

    batch_request: BatchRequest = convert_data_reference_string_to_batch_request_using_regex(
        data_reference=data_reference,
        regex_pattern=regex_pattern,
        group_names=group_names,
    )
    if batch_request is None:
        return None

    if data_asset_name is None:
        data_asset_name = batch_request.data_asset_name

    return [
        BatchDefinition(
            execution_environment_name=execution_environment_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_definition=PartitionDefinition(batch_request.partition_request),
        )
    ]


def convert_data_reference_string_to_batch_request_using_regex(
    data_reference: str,
    regex_pattern: str,
    group_names: List[str],
) -> Optional[BatchRequest]:
    # noinspection PyUnresolvedReferences
    matches: Optional[re.Match] = re.match(regex_pattern, data_reference)
    if matches is None:
        return None

    groups: list = list(matches.groups())
    partition_definition: PartitionDefinitionSubset = PartitionDefinitionSubset(
        dict(zip(group_names, groups))
    )

    # TODO: <Alex>Accommodating "data_asset_name" inside partition_definition (e.g., via "group_names") is problematic; idea: resurrect the Partition class.</Alex>
    data_asset_name: str = DEFAULT_DATA_ASSET_NAME
    if "data_asset_name" in partition_definition:
        data_asset_name = partition_definition.pop("data_asset_name")

    batch_request: BatchRequest = BatchRequest(
        data_asset_name=data_asset_name,
        partition_request=partition_definition,
    )

    return batch_request


def map_batch_definition_to_data_reference_string_using_regex(
    batch_definition: BatchDefinition,
    regex_pattern: str,
    group_names: List[str],
) -> str:
    # TODO: <Alex>Per Abe: The two-steps process involving a BatchRequest intermediary can be simplified by turning
    # convert_data_reference_string_to_batch_request_using_regex() into convert_data_reference_string_to_batch_definition_using_regex()
    # and passing it all the arguments it needs to create a BatchDefinition.
    # We could not do it before, because the Partitioner difd not have all the required arguments. But the DataConnector does have them.
    #</Alex>
    data_asset_name: str = batch_definition.data_asset_name
    partition_definition: PartitionDefinition = batch_definition.partition_definition
    partition_request: dict = partition_definition
    batch_request: BatchRequest = BatchRequest(
        data_asset_name=data_asset_name,
        partition_request=partition_request,
    )
    data_reference: str = convert_batch_request_to_data_reference_string_using_regex(
        batch_request=batch_request,
        regex_pattern=regex_pattern,
        group_names=group_names
    )
    return data_reference


# TODO: <Alex>How are we able to recover the full file path, including the file extension?  Relying on file extension being part of the regex_pattern does not work when multiple file extensions are specified as part of the regex_pattern.</Alex>
def convert_batch_request_to_data_reference_string_using_regex(
    batch_request: BatchRequest,
    regex_pattern: str,
    group_names: List[str],
) -> str:
    if not isinstance(batch_request, BatchRequest):
        raise TypeError("batch_request is not of an instance of type BatchRequest")

    template_arguments: dict = copy.deepcopy(batch_request.partition_request)
    # TODO: <Alex>How does "data_asset_name" factor in the computation of "converted_string"?  Does it have any effect?</Alex>
    if batch_request.data_asset_name is not None:
        template_arguments["data_asset_name"] = batch_request.data_asset_name

    filepath_template: str = _invert_regex_to_data_reference_template(
        regex_pattern=regex_pattern,
        group_names=group_names,
    )
    converted_string = filepath_template.format(
        **template_arguments
    )

    return converted_string


# noinspection PyUnresolvedReferences
def _invert_regex_to_data_reference_template(
    regex_pattern: str,
    group_names: List[str],
) -> str:
    """
    NOTE Abe 20201017: This method is almost certainly still brittle. I haven't exhaustively mapped the OPCODES in sre_constants
    """
    data_reference_template: str = ""
    group_name_index: int = 0

    # print("-"*80)
    parsed_sre = sre_parse.parse(regex_pattern)
    for token, value in parsed_sre:
        # print(type(token), token, value)

        if token == sre_constants.LITERAL:
            # Transcribe the character directly into the template
            data_reference_template += chr(value)

        elif token == sre_constants.SUBPATTERN:
            # Replace the captured group with "{next_group_name}" in the template
            data_reference_template += "{"+group_names[group_name_index]+"}"
            group_name_index += 1

        elif token in [
            sre_constants.MAX_REPEAT,
            sre_constants.IN,
            sre_constants.BRANCH,
            sre_constants.ANY,
        ]:
            # Replace the uncaptured group a wildcard in the template
            data_reference_template += "*"

        elif token in [
            sre_constants.AT,
            sre_constants.ASSERT_NOT,
            sre_constants.ASSERT,
        ]:
            pass
        else:
            raise ValueError(f"Unrecognized regex token {token} in regex pattern {regex_pattern}.")

    # Collapse adjacent wildcards into a single wildcard
    data_reference_template: str = re.sub("\\*+", "*", data_reference_template)

    return data_reference_template


def build_sorters_from_config(config_list: List[Dict[str, Any]]):
    sorter_dict: Dict[str, Sorter] = {}
    if config_list is not None:
        for sorter_config in config_list:
            # if sorters were not configured
            if sorter_config is None:
                return
            if 'name' not in sorter_config:
                raise ValueError("Sorter config should have a name")
            sorter_name = sorter_config['name']
            new_sorter: Sorter = _build_sorter_from_config(sorter_config=sorter_config)
            sorter_dict[sorter_name] = new_sorter
    return sorter_dict


def _build_sorter_from_config(sorter_config) -> Sorter:
    """Build a Sorter using the provided configuration and return the newly-built Sorter."""
    runtime_environment: dict = {
        "name": sorter_config['name']
    }
    sorter: Sorter = instantiate_class_from_config(
        config=sorter_config,
        runtime_environment=runtime_environment,
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector.sorter"
       },
    )
    return sorter
