from typing import Mapping, Optional

import pytest

from great_expectations.zep.bi_directional_dict import BiDict

pytestmark = [pytest.mark.unit]
param = pytest.param


@pytest.mark.parametrize(
    ["positional_mapping_arg", "kwargs", "expected_data"],
    [
        param(
            {"foo": "bar"},
            {},
            {"foo": "bar", "bar": "foo"},
            id="mapping as positional arg",
        ),
        param(
            None,
            {"fizz": "buzz", "flt": float},
            {"fizz": "buzz", "buzz": "fizz", "flt": float, float: "flt"},
            id="kwargs only with Types",
        ),
        param(
            {"a": "b", "foo": "bar"},
            {"foo": "BAR"},
            {"a": "b", "b": "a", "foo": "BAR", "BAR": "foo"},
            id="kwargs overwrite positional mapping",
        ),
    ],
)
def test_init(
    positional_mapping_arg: Optional[Mapping], kwargs: dict, expected_data: dict
):
    d: BiDict = BiDict(positional_mapping_arg, **kwargs)
    assert expected_data == d.data


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
