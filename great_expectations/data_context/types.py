from collections import namedtuple

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
        "expectation_suite_name" : str,
        "run_id" : str,
    }
