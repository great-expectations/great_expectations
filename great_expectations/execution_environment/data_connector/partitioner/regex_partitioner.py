import regex as re
from typing import List, Union, Any
import copy
from pathlib import Path
from string import Template
import sre_parse
import sre_constants

import logging

from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.id_dict import PartitionDefinition
import great_expectations.exceptions as ge_exceptions

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
)

logger = logging.getLogger(__name__)


class RegexPartitioner(Partitioner):
    DEFAULT_GROUP_NAME_PATTERN: str = "group_"

    def __init__(
        self,
        name: str,
        sorters: list = None,
        allow_multipart_partitions: bool = False,
        runtime_keys: list = None,
        pattern: str = r"(.*)",
        group_names: List[str] = None,
    ):
        logger.debug(f'Constructing RegexPartitioner "{name}".')
        super().__init__(
            name=name,
            sorters=sorters,
            allow_multipart_partitions=allow_multipart_partitions,
            runtime_keys=runtime_keys
        )

        self._pattern = pattern
        self._group_names = group_names

    def convert_data_reference_to_batch_request(
        self,
        data_reference: Any = None
    ) -> Union[BatchRequest, None]:
        matches: Union[re.Match, None] = re.match(self._pattern, data_reference)
        if matches is None:
            # TODO: <Alex>Do we need this commented out line?</Alex>
            #raise ValueError(f'No match found for data_reference: "{data_reference}".')
            return None
        groups: tuple = matches.groups()
        group_names: list = [
            f"{RegexPartitioner.DEFAULT_GROUP_NAME_PATTERN}{idx}" for idx, group_value in enumerate(groups)
        ]
        self._validate_sorters_configuration(
            partition_keys=self._group_names,
            num_actual_partition_keys=len(groups)
        )
        for idx, group_name in enumerate(self._group_names):
            group_names[idx] = group_name
        partition_definition: dict = {}
        for idx, group_value in enumerate(groups):
            group_name: str = group_names[idx]
            partition_definition[group_name] = group_value
        # TODO: <Alex>Abe: Does PartitionDefinition have a role in the new design?  If so, what does it consist of?</Alex>
        partition_definition: PartitionDefinition = PartitionDefinition(partition_definition)
        # TODO: <Alex>Do runtime_parameters have a role in the new design?  Otherwise, remove unused code.</Alex>
        # if runtime_parameters:
        #     partition_definition.update(runtime_parameters)
        # TODO: <Alex>Remove unused code.</Alex>
        # partition_name: str = self.DEFAULT_DELIMITER.join(
        #     [str(value) for value in partition_definition.values()]
        # )

        # TODO: <Alex>Given how partition_definition is created (above), this cannot work as is without additional development.</Alex>
        # TODO: <Alex>The following code assumes that PartitionDefinition may contain "data_asset_name"; however, given how PartitionDefinition is computed (above), this is not the case.</Alex>
        if "data_asset_name" in partition_definition:
            data_asset_name = partition_definition.pop("data_asset_name")
        else:
            # TODO: <Alex>This needs to be implemented more cleanly, such as for example in the DataConnector class hierarchy.</Alex>
            # adding this, so things don't crash
            data_asset_name = "DEFAULT_ASSET_NAME"
            # TODO: <Alex>The code below appears to have been copied accidentally (it is identical to code above) -- commenting out the copy below.</Alex>
            # groups: tuple = matches.groups()
            # group_names: list = [
            #     f"{RegexPartitioner.DEFAULT_GROUP_NAME_PATTERN}{idx}" for idx, group_value in enumerate(groups)
            # ]
            # #
            # self._validate_sorters_configuration(
            #     partition_keys=self._group_names,
            #     num_actual_partition_keys=len(groups)
            # )
            # for idx, group_name in enumerate(self._group_names):
            #     group_names[idx] = group_name
            # partition_definition: dict = {}
            # for idx, group_value in enumerate(groups):
            #     group_name: str = group_names[idx]
            #     partition_definition[group_name] = group_value
            # partition_definition: PartitionDefinition = PartitionDefinition(partition_definition)
            # TODO: <Alex>Remove unused code.</Alex>
            # partition_name: str = self.DEFAULT_DELIMITER.join(
            #     [str(value) for value in partition_definition.values()]
            # )

        return BatchRequest(
            data_asset_name=data_asset_name,
            partition_request=partition_definition,
        )

    def convert_batch_request_to_data_reference(
        self,
        batch_request: BatchRequest = None,
    ) -> str:
        if not isinstance(batch_request, BatchRequest):
            raise TypeError("batch_request is not of an instance of type BatchRequest")

        template_arguments = batch_request.partition_request
        if batch_request.data_asset_name != None:
            template_arguments["data_asset_name"] = batch_request.data_asset_name

        filepath_template = self._invert_regex_to_data_reference_template()
        converted_string = filepath_template.format(
            **template_arguments
        )

        return converted_string

    def _invert_regex_to_data_reference_template(self):
        """
        NOTE Abe 20201017: This method is almost certainly still brittle. I haven't exhaustively mapped the OPCODES in sre_constants
        """
        regex_pattern = self._pattern
        data_reference_template = ""
        group_name_index = 0

        #print("-"*80)
        parsed_sre = sre_parse.parse(regex_pattern)
        for token, value in parsed_sre:
            #print(type(token), token, value)

            if token == sre_constants.LITERAL:
                #Transcribe the character directly into the template
                data_reference_template += chr(value)

            elif token == sre_constants.SUBPATTERN:
                #Replace the captured group with "{next_group_name}" in the template
                data_reference_template += "{"+self._group_names[group_name_index]+"}"
                group_name_index += 1

            elif token in [
                sre_constants.MAX_REPEAT,
                sre_constants.IN,
                sre_constants.BRANCH,
                sre_constants.ANY,
            ]:
                #Replace the uncaptured group a wildcard in the template
                data_reference_template += "*"

            elif token in [
                sre_constants.AT,
                sre_constants.ASSERT_NOT,
                sre_constants.ASSERT,
            ]:
                pass
            else:
                raise ValueError(f"Unrecognized regex token {token} in regex pattern {regex_pattern}.")

        #Collapse adjacent wildcards into a single wildcard
        data_reference_template = re.sub("\*+", "*", data_reference_template)

        return data_reference_template
