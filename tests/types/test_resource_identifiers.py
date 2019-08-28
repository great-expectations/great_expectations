import pytest
import logging
logger = logging.getLogger(__name__)

from six import PY2, string_types
import sys

from great_expectations.data_context.types.resource_identifiers import (
    OrderedKeysDotDict,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import (
    parse_string_to_data_context_resource_identifier
)

def test_OrderedKeysDotDict_subclass():
    # NOTE: Abe 2019/08/23 : The basics work reasonably well, but this class probably still needs to be hardened quite a bit
    # TODO: Move this to types.test_base_types.py

    class MyOKDD(OrderedKeysDotDict):
        _key_order = ["A", "B", "C"]
        _key_types = {
            "A" : string_types,
            "B" : int,
        }

        # NOTE: This pattern is kinda awkward.
        # It would be nice to ONLY specify _key_order
        # Instead, we need to add these two lines at the end of every OrderedKeysDotDict class definition
        # ... There's probably a way to do this with decorators...
        _allowed_keys = set(_key_order)
        _required_keys = set(_key_order)

    MyOKDD(**{
        "A" : "A",
        "B" : 10,
        "C" : "C",
    })

    #OrderedKeysDotDicts can parse from tuples
    MyOKDD("a", 10, "c")

    #OrderedKeysDotDicts coerce to _key_types by default
    assert MyOKDD("10", "10", "20") == {
        "A" : "10",
        "B" : 10, # <- Not a string anymore!
        "C" : "20",
    }

    with pytest.raises(ValueError):
        assert MyOKDD("a", "10.5", 20)

    #OrderedKeysDotDicts raise an IndexError if args don't line up with keys
    with pytest.raises(IndexError):
        MyOKDD("a")

    with pytest.raises(IndexError):
        MyOKDD("a", 10, "c", "d")


def test_OrderedKeysDotDict__recursively_get_key_length():

    class MyOKDD(OrderedKeysDotDict):
        _key_order = ["A", "B", "C"]
        _key_types = {
            "A" : string_types,
            "B" : int,
        }
        _allowed_keys = set(_key_order)
        _required_keys = set(_key_order)

    assert MyOKDD._recursively_get_key_length() == 3
    assert DataAssetIdentifier._recursively_get_key_length() == 3
    assert ValidationResultIdentifier._recursively_get_key_length() == 7


def test_DataAssetIdentifier():

    DataAssetIdentifier(**{
        "datasource" : "A",
        "generator" : "B",
        "generator_asset" : "C",
    })

    my_id = DataAssetIdentifier("A", "B", "C")
    assert my_id.to_string() == "DataAssetIdentifier.A.B.C"

# NOTE: The following tests are good tests of OrderedDotDict's ability to handle abbreviated input to __init__ when nested

def test_ValidationResultIdentifier__init__totally_nested():
    my_id = ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : {
                    "datasource" : "a",
                    "generator" : "b",
                    "generator_asset" : "c",
                },
                "suite_purpose": "hello",
                "level": "testing",
            },
            "run_id" : {
                "execution_context": "testing",
                "start_time_utc": 12345,
            },
        }
    )

    assert my_id.to_string() == "ValidationResultIdentifier.a.b.c.hello.testing.testing.12345"

def test_ValidationResultIdentifier__init__mostly_nested():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"),
                "suite_purpose": "default",
                "level": "failure",
            },
            "run_id" : {
                "execution_context": "testing",
                "start_time_utc": 12345,
            },
        }
    )

def test_ValidationResultIdentifier__init__mostly_nested_with_typed_child():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : DataAssetIdentifier("a", "b", "c"),
                "suite_purpose": "hello",
                "level": "quarantine",
            },
            "run_id" : {
                "execution_context": "testing",
                "start_time_utc": 12345,
            },
        }
    )

def test_ValidationResultIdentifier__init__partially_flat():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"),
                "suite_purpose": "default",
                "level": "warning",
            },
            "run_id" : ("testing", 12345)
        }
    )

# NOTE: Abe 2019/08/24 : This style of instantiation isn't currently supported. Might want to enable it at some point in the future
# # def test_ValidationResultIdentifier__init__mostly_flat():
#     ValidationResultIdentifier(
#         coerce_types=True,
#         **{
#             "expectation_suite_identifier" : (("a", "b", "c"), "hello", "testing"),
#             "run_id" : ("testing", 12345)
#         }
#     )

# NOTE: Abe 2019/08/24 : This style of instantiation isn't currently supported. Might want to enable it at some point in the future 
# def test_ValidationResultIdentifier__init__very_flat():
#     ValidationResultIdentifier(
#         (("a", "b", "c"), "hello", "testing"),("testing", 12345),
#         coerce_types=True,
#     )

def test_ValidationResultIdentifier__init__nested_except_the_top_layer():
    ValidationResultIdentifier(
        {
            "data_asset_identifier" : {
                "datasource" : "a",
                "generator" : "b",
                "generator_asset" : "c",
            },
            "suite_purpose": "hello",
            "level": "warning",
        },{
            "execution_context": "testing",
            "start_time_utc": 12345,
        },
        coerce_types=True,
    )

def test_ValidationResultIdentifier__init__entirely_flat():
    ValidationResultIdentifier(
        "a", "b", "c", "hello", "testing", "testing", 12345,
        coerce_types=True,
    )

def test_OrderedKeysDotDict__zip_keys_and_args_to_dict():
    assert ValidationResultIdentifier._zip_keys_and_args_to_dict(
        ["a", "b", "c", "hello", "testing", "testing", 12345],
    ) == {
        "expectation_suite_identifier" : ["a", "b", "c", "hello", "testing"],
        "run_id" : ["testing", 12345],
    }

    assert ExpectationSuiteIdentifier._zip_keys_and_args_to_dict(
        ["a", "b", "c", "hello", "warning"]
    ) == {
        "data_asset_identifier" : ["a", "b", "c"],
        "suite_purpose" : "hello",
        "level" : "warning",
    }

    assert ValidationResultIdentifier._zip_keys_and_args_to_dict(
        ["a", "b", "c", "hello", "testing", "testing", 12345],
    ) == {
        "expectation_suite_identifier" : ["a", "b", "c", "hello", "testing"],
        "run_id" : ["testing", 12345],
    }

# TODO: Put this with the other tests for utils.
def test_parse_string_to_data_context_resource_identifier():

    assert parse_string_to_data_context_resource_identifier("DataAssetIdentifier.A.B.C") == DataAssetIdentifier("A", "B", "C")

    assert parse_string_to_data_context_resource_identifier(
        "ValidationResultIdentifier.a.b.c.default.failure.testing.12345"
    ) == ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : {
                    "datasource" : "a",
                    "generator" : "b",
                    "generator_asset" : "c",
                },
                "suite_purpose": "default",
                "level": "failure",
            },
            "run_id" : {
                "execution_context": "testing",
                "start_time_utc": 12345,
            },
        }
    )
