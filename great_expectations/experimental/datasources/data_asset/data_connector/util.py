from __future__ import annotations

import copy
import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from great_expectations.core.id_dict import IDDict

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition
    from great_expectations.experimental.datasources.interfaces import BatchRequest

logger = logging.getLogger(__name__)

try:
    from azure.storage.blob import BlobPrefix
except ImportError:
    BlobPrefix = None
    logger.debug(
        "Unable to load azure types; install optional Azure dependency for support."
    )

try:
    from google.cloud import storage
except ImportError:
    storage = None
    logger.debug(
        "Unable to load GCS connection object; install optional Google dependency for support"
    )

try:
    import pyspark
    import pyspark.sql as pyspark_sql
except ImportError:
    pyspark = None
    pyspark_sql = None
    logger.debug(
        "Unable to load pyspark and pyspark.sql; install optional Spark dependency for support."
    )


def batch_definition_matches_batch_request(
    batch_definition: BatchDefinition,
    batch_request: BatchRequest,
) -> bool:
    if not (
        batch_request.datasource_name == batch_definition.datasource_name
        and batch_request.data_asset_name == batch_definition.data_asset_name
    ):
        return False

    if batch_request.options:
        for key in batch_request.options.keys():
            if not (
                key in batch_definition.batch_identifiers
                and batch_definition.batch_identifiers[key]
                == batch_request.options[key]
            ):
                return False

    return True


def map_data_reference_string_to_batch_definition_list_using_regex(
    datasource_name: str,
    data_connector_name: str,
    data_reference: str,
    regex_pattern: re.Pattern,
    data_asset_name: str,
) -> List[BatchDefinition]:
    batch_identifiers: Optional[
        IDDict
    ] = convert_data_reference_string_to_batch_identifiers_using_regex(
        data_reference=data_reference,
        regex_pattern=regex_pattern,
    )
    if batch_identifiers is None:
        return []

    # Importing at module level causes circular dependencies.
    from great_expectations.core.batch import BatchDefinition

    return [
        BatchDefinition(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_identifiers=IDDict(batch_identifiers),
        )
    ]


def convert_data_reference_string_to_batch_identifiers_using_regex(
    data_reference: str,
    regex_pattern: re.Pattern,
    unnamed_regex_group_prefix: str = "unnamed_group_",
) -> Optional[IDDict]:
    # noinspection PyUnresolvedReferences
    matches: Optional[re.Match] = regex_pattern.match(data_reference)
    if matches is None:
        return None

    num_all_matched_group_values: int = regex_pattern.groups

    # Check for `(?P<name>)` named group syntax
    defined_group_name_to_group_index_mapping: Dict[str, int] = dict(
        regex_pattern.groupindex
    )
    defined_group_name_indexes: Set[int] = set(
        defined_group_name_to_group_index_mapping.values()
    )
    defined_group_name_to_group_value_mapping: Dict[str, str] = matches.groupdict()

    all_matched_group_values: List[str] = list(matches.groups())

    assert len(all_matched_group_values) == num_all_matched_group_values

    group_name_to_group_value_mapping: Dict[str, str] = copy.deepcopy(
        defined_group_name_to_group_value_mapping
    )

    idx: int
    group_idx: int
    matched_group_value: str
    for idx, matched_group_value in enumerate(all_matched_group_values):
        group_idx = idx + 1
        if group_idx not in defined_group_name_indexes:
            group_name: str = f"{unnamed_regex_group_prefix}{group_idx}"
            group_name_to_group_value_mapping[group_name] = matched_group_value

    batch_identifiers = IDDict(group_name_to_group_value_mapping)

    return batch_identifiers
