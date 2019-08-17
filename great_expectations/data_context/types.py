from collections import namedtuple
from six import string_types

from ..types import LooselyTypedDotDict

NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])

#Note: As of 2018/8/14, this class is VERY MUCH a work in progress
#It should almost certainly be a namedtuple, to keep it immutable.
class NameSpaceDotDict(LooselyTypedDotDict):
    _allowed_keys = set([
        "normalized_data_asset_name",
        "expectation_suite_name",
        "run_id",
    ])

    _required_keys = set([
        "normalized_data_asset_name",
        "expectation_suite_name",
        "run_id",
    ])

    _key_types = {
        "normalized_data_asset_name" : NormalizedDataAssetName,
        "expectation_suite_name" : string_types,
        "run_id" : string_types,
    }

class Config(LooselyTypedDotDict):
    pass

class DataContextConfig(Config):
    _allowed_keys = set([
        "plugins_directory",
        "datasources",
        "stores",
        "data_docs", #TODO: Rename this to sites, to remove a layer of extraneous nesting
    ])

    _required_keys = set([
        "plugins_directory",
        "datasources",
        "stores",
        "data_docs",
    ])

    _key_types = {
        "plugins_directory" : [string_types, None],
        "datasources" : dict,
        "stores" : dict,
        "data_docs" : dict,
    }
