import logging
logger = logging.getLogger(__name__)

from six import string_types

from great_expectations.data_context.types.base_resource_identifiers import (
    DataContextKey,
    OrderedDataContextKey,
)



# TODO: Rename to DataAssetKey, for consistency
class DataAssetIdentifier(OrderedDataContextKey):

    def __init__(self, *args, **kwargs):
        delimiter = kwargs.pop('delimiter', '/')
        super(DataAssetIdentifier, self).__init__(*args, **kwargs)
        self.__delimiter = delimiter

    _key_order = [
        "datasource",
        "generator",
        "generator_asset"
    ]
    _key_types = {
        "datasource": string_types,
        "generator": string_types,
        "generator_asset": string_types
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order) | {"_DataAssetIdentifier__delimiter"}

    def __str__(self):
        return self.__delimiter.join(
            (self.datasource,
             self.generator,
             self.generator_asset)
        )

    def __repr__(self):
        return str(self)


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

# TODO: Rename to SiteSectionKey, for consistency
class SiteSectionIdentifier(DataContextKey):
    _required_keys = set([
        "site_section_name",
        "resource_identifier",
    ])
    _allowed_keys = _required_keys
    _key_types = {
        "site_section_name" : string_types,
        "resource_identifier" : DataContextKey,
        # "resource_identifier", ... is NOT strictly typed, since it can contain any type of ResourceIdentifier
    }

    def __hash__(self):
        return hash(self.site_section_name+"::"+self.resource_identifier.to_string())

    def __eq__(self, other):
        print(self)
        print(other)
        return self.__hash__() == other.__hash__()
