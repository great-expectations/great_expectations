from ..types import LooselyTypedDotDict

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
