from pprint import pprint as pp
from typing import Hashable, Iterable, Mapping, Optional, Tuple

import pytest

from great_expectations.zep.type_lookup import TypeLookup, TypeLookupError, ValidTypes

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


@pytest.mark.parametrize(
    "collection_to_check",
    [
        ["a_list"],
        {"not_present", "a_dict", list},
    ],
)
def test_raise_if_contains_raises(collection_to_check: Iterable[ValidTypes]):
    type_lookup = TypeLookup(a_list=list, a_dict=dict)

    with pytest.raises(TypeLookupError, match=r"Items are already present .*"):
        type_lookup.raise_if_contains(collection_to_check)


@pytest.mark.parametrize(
    "collection_to_check",
    [
        ["not_present"],
        {"not_present", "a_tuple", set},
    ],
)
def test_raise_if_contains_does_not_raise(collection_to_check: Iterable[ValidTypes]):
    type_lookup = TypeLookup(a_list=list, a_dict=dict)

    type_lookup.raise_if_contains(collection_to_check)


def test_original_keys():
    t = TypeLookup({"a_list": list, "a_tuple": tuple, "a_set": set})
    keys = set(t.keys())
    original_keys = set(t.original_keys())

    print(keys)
    print(original_keys)

    assert original_keys.issubset(keys)
    assert original_keys != keys


def test_original_items():
    t = TypeLookup({"a_list": list, "a_tuple": tuple, "a_set": set})
    keys = set(t.keys())
    original_keys = set(t.original_keys())

    print("keys\t\t", keys)
    print("original keys\t", original_keys)

    assert original_keys.issubset(keys)
    assert original_keys != keys
    original_values = original_keys.symmetric_difference(keys)
    print("original values\t", original_values)

    for _, o_value in t.original_items():
        assert o_value in original_values


class TestTransactions:
    def test_transaction_happy_path(self):
        t = TypeLookup({"a_list": list, "a_dict": dict})

        with t.transaction() as txn_t:
            print(f"t\t{len(t)}")
            print(f"txn_t\t{len(txn_t)}\n")

            txn_t["a_set"] = set
            print(f"t\t{len(t)}")
            print(f"txn_t\t{len(txn_t)}\n")
            assert set in txn_t
            assert set not in t

            txn_t["a_tuple"] = tuple
            print(f"t\t{len(t)}")
            print(f"txn_t\t{len(txn_t)}\n")
            assert tuple in txn_t
            assert tuple not in t

        pp(t.data)
        assert set in t
        assert tuple in t

    def test_transaction_exit_early(self):
        t = TypeLookup({"a_list": list, "a_dict": dict})

        with pytest.raises(ValueError, match="oh uh"):
            with t.transaction() as txn_t:
                print(f"t\t{len(t)}")
                print(f"txn_t\t{len(txn_t)}\n")

                txn_t["a_set"] = set
                print(f"t\t{len(t)}")
                print(f"txn_t\t{len(txn_t)}\n")
                assert set not in t

                txn_t["a_tuple"] = tuple
                print(f"t\t{len(t)}")
                print(f"txn_t\t{len(txn_t)}\n")

                raise ValueError("oh uh")

        pp(t.data)
        assert set not in t
        assert tuple not in t


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
