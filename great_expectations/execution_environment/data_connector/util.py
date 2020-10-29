# -*- coding: utf-8 -*-

# Utility methods for dealing with DataConnector objects

from typing import List, Optional
import copy
import re
import sre_parse
import sre_constants

import logging

from great_expectations.core.batch import BatchRequest
from great_expectations.core.id_dict import PartitionDefinition
from great_expectations.core.batch import BatchDefinition

logger = logging.getLogger(__name__)


DEFAULT_DATA_ASSET_NAME: str = "DEFAULT_ASSET_NAME"


# TODO: <Alex>The semantics of this method are too rigid (see below) and are incomplete.
#  It should replaced with a rich query interface (see great_expectations/execution_environment/data_connector/partition_request.py).
#  </Alex>
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
    # FIXME: This is too rigid. Needs to take into account ranges and stuff.
    if batch_request.partition_request is not None:
        for k, v in batch_request.partition_request.items():
            if (k not in batch_definition.partition_definition) or batch_definition.partition_definition[k] != v:
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
    # noinspection PyUnresolvedReferences
    matches: Optional[re.Match] = re.match(regex_pattern, data_reference)
    if matches is None:
        return None

    groups: list = list(matches.groups())
    partition_definition: PartitionDefinition = PartitionDefinition(
        dict(zip(group_names, groups))
    )

    # TODO: <Alex>Accommodating "data_asset_name" inside partition_definition (e.g., via "group_names") is problematic, because it comingles concepts.</Alex>
    data_asset_name_of_partition: str = DEFAULT_DATA_ASSET_NAME
    if "data_asset_name" in partition_definition:
        data_asset_name_of_partition = partition_definition.pop("data_asset_name")

    if data_asset_name is None:
        data_asset_name = data_asset_name_of_partition

    return [
        BatchDefinition(
            execution_environment_name=execution_environment_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            partition_definition=partition_definition
        )
    ]


def map_batch_definition_to_data_reference_string_using_regex(
    batch_definition: BatchDefinition,
    regex_pattern: str,
    group_names: List[str],
) -> str:
    if not isinstance(batch_definition, BatchDefinition):
        raise TypeError("batch_definition is not of an instance of type BatchDefinition")

    data_asset_name: str = batch_definition.data_asset_name
    partition_definition: PartitionDefinition = batch_definition.partition_definition

    template_arguments: dict = copy.deepcopy(partition_definition)
    # TODO: <Alex>How does "data_asset_name" factor in the computation of "converted_string"?  Does it have any effect?</Alex>
    if data_asset_name is not None:
        template_arguments["data_asset_name"] = data_asset_name

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
