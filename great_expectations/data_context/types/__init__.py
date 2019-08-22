from ...types import LooselyTypedDotDict
from collections import namedtuple
from six import string_types

from .configurations import (
    DataContextConfig
)

NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])


class NameSpaceDotDict(LooselyTypedDotDict):
    # Note: As of 2018/8/14, this class is VERY MUCH a work in progress
    # It should almost certainly be a namedtuple, to keep it immutable.
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
        "normalized_data_asset_name": NormalizedDataAssetName,
        "expectation_suite_name": string_types,
        "run_id": string_types,
    }
