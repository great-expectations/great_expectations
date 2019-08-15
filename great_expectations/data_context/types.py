from ..types import LooselyTypedDotDict

#Note: As of 2018/8/14, this class is VERY MUCH a work in progress
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
