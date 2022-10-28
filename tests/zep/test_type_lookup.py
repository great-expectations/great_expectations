from typing import Hashable, Mapping, Optional, Tuple

import pytest

from great_expectations.zep.type_lookup import TypeLookup, TypeLookupError

pytestmark = [pytest.mark.unit]
param = pytest.param


@pytest.mark.parametrize(
    ["positional_mapping_arg", "kwargs", "expected_data"],
    [
        param(
            {"foo": "bar", str: "string"},
            {},
            {"foo": "bar", "bar": "foo", str: "string", "string": str},
            id="mapping as positional arg",
        ),
        param(
            None,
            {"fizz": "buzz", "flt": float},
            {"fizz": "buzz", "buzz": "fizz", "flt": float, float: "flt"},
            id="kwargs only",
        ),
        param(
            {"a": "b"},
            {"c": "d"},
            {"a": "b", "b": "a", "c": "d", "d": "c"},
            id="positional & kwargs",
        ),
    ],
)
def test_init(
    positional_mapping_arg: Optional[Mapping], kwargs: dict, expected_data: dict
):
    d = TypeLookup(positional_mapping_arg, **kwargs)
    assert expected_data == d


@pytest.mark.parametrize(["key", "value"], [(str, "string"), ("integer", int)])
def test_map_key_to_value(key: Hashable, value: Hashable):
    d = TypeLookup()
    d[key] = value
    assert d[value] == key


@pytest.mark.parametrize(
    ["initial", "kv_pair"],
    [
        (TypeLookup(my_list=list), ("your_list", list)),
        (TypeLookup(my_list=list), ("my_list", dict)),
    ],
)
def test_no_key_or_value_overwrites(
    initial: TypeLookup, kv_pair: Tuple[Hashable, Hashable]
):
    key, value = kv_pair
    with pytest.raises(TypeLookupError, match=r"`.*` already set"):
        initial[key] = value


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
