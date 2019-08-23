from six import string_types

from great_expectations.types import (
    RequiredKeysDotDict,
    AllowedKeysDotDict,
)

# NOTE: Abe 2019/08/23 : The basics work reasonably well, but this class probably still needs to be hardened quite a bit
# TODO: Move this to great_expectations.types.base.py
class OrderedKeysDotDict(AllowedKeysDotDict):
    _key_order = []

    def __init__(self, *args, **kwargs):
        assert set(self._key_order) == set(self._allowed_keys)

        if args == ():
            super(OrderedKeysDotDict, self).__init__(*args, **kwargs)

        else:
            new_kwargs = dict(zip(self._key_order, args))
            super(OrderedKeysDotDict, self).__init__(**new_kwargs)

class DataContextResourceIdentifier(OrderedKeysDotDict):
    pass

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


class BatchIdentifier():
    _key_order = [
        "data_asset_identifier",
        "batch_runtime_id",
    ]
    _key_types = {
        "data_asset_identifier" : DataAssetIdentifier,
        "batch_runtime_id" : string_types,
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)


class RunIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "execution_context",
        "start_time_utc",
    ]
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)


class ExpectationSuiteIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "data_asset_identifier",
        "suite_name",
        "purpose"
    ]
    _key_types = {
        "data_asset_identifier" : DataAssetIdentifier,
        "suite_name" : string_types,
        "purpose" : string_types
    }
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)


class ValidationResultIdentifier(DataContextResourceIdentifier):
    _key_order = [
        "expectation_suite_identifier",
        "run_id",
        "purpose"
    ]
    # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
    _required_keys = set(_key_order)
    _allowed_keys = set(_key_order)
