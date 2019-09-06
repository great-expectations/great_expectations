import logging
logger = logging.getLogger(__name__)

from six import string_types

from .base_resource_identifiers import (
    OrderedDataContextKey,
)

from great_expectations.datasource.types.batch_kwargs import BatchFingerprint

# TODO: Rename to DataAssetKey, for consistency
class DataAssetIdentifier(OrderedDataContextKey):
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

# TODO: Rename to ExpectationSuiteKey, for consistency
class ExpectationSuiteIdentifier(OrderedDataContextKey):
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

# TODO: Rename to ValidatioResultKey, for consistency
class ValidationResultIdentifier(OrderedDataContextKey):
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
