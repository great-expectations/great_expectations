import logging
logger = logging.getLogger(__name__)

from six import string_types

from .base_resource_identifiers import (
    DataContextResourceIdentifier,
)


class DataAssetIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "datasource",
        "generator",
        "generator_asset"
    ]
    _key_types = {
        "datasource" : string_types,
        "generator" : string_types,
        "generator_asset" : string_types,
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)

# FIXME: this should be removed / refactored to the BatchKwargs/BatchId Types
# class BatchIdentifier(DataContextResourceIdentifier):
#     _key_order = [
#         "data_asset_identifier",
#         "batch_runtime_id",
#     ]
#     _key_types = {
#         "data_asset_identifier" : DataAssetIdentifier,
#         "batch_runtime_id" : string_types,
#     }
#     # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
#     _required_keys = set(_key_order)
#     _allowed_keys = set(_key_order)


# class RunIdentifier(DataContextResourceIdentifier):
#     _key_order = [
#         "execution_context",
#         "start_time_utc",
#     ]
#     _key_types = {
#         "execution_context" : string_types,
#         "start_time_utc" : int,
#     }
#     # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
#     _required_keys = set(_key_order)
#     _allowed_keys = set(_key_order)


class ExpectationSuiteIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "data_asset_name",
        "expectation_suite_name",
    ]
    _key_types = {
        "data_asset_name" : DataAssetIdentifier,
        "expectation_suite_name" : string_types,
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)


class ValidationResultIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "expectation_suite_identifier",
        "run_id",
        # "purpose"
    ]
    _key_types = {
        "expectation_suite_identifier": ExpectationSuiteIdentifier,
        "run_id": string_types
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)
