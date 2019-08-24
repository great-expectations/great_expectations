import pytest
import logging
logger = logging.getLogger(__name__)

from six import PY2, string_types
import sys

from great_expectations.data_context.types.resource_identifiers import (
    OrderedKeysDotDict,
    DataAssetIdentifier,
    ValidationResultIdentifier,
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

        # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
        _allowed_keys = set(_key_order)
        _required_keys = set(_key_order)


    MyOKDD(**{
        "A" : "A",
        "B" : 10,
        "C" : "C",
    })

    MyOKDD("a", 10, "c")


def test_DataAssetIdentifier():

    DataAssetIdentifier(**{
        "datasource" : "A",
        "generator" : "B",
        "generator_asset" : "C",
    })

    DataAssetIdentifier("A", "B", "C")

# NOTE: The following tests are good tests of OrderedDotDict's ability to handle abbreviated input to __init__ when nested

def test_ValidationResultIdentifier__init__totally_nested():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : {
                    "datasource" : "a",
                    "generator" : "b",
                    "generator_asset" : "c",
                },
                "suite_name": "hello",
                "purpose": "testing",
            },
            "run_id" : {
                "execution_context": "testing",
                "start_time_utc": 12345,
            },
        }
    )

def test_ValidationResultIdentifier__init__mostly_nested():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"),
                "suite_name": "hello",
                "purpose": "testing",
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
                "suite_name": "hello",
                "purpose": "testing",
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
                "suite_name": "hello",
                "purpose": "testing",
            },
            "run_id" : ("testing", 12345)
        }
    )

def test_ValidationResultIdentifier__init__mostly_flat():
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : (("a", "b", "c"), "hello", "testing"),
            "run_id" : ("testing", 12345)
        }
    )

def test_ValidationResultIdentifier__init__very_flat():
    ValidationResultIdentifier(
        (("a", "b", "c"), "hello", "testing"),("testing", 12345),
        coerce_types=True,
    )

# TODO: This style of instantiation isn't currently supported, but could be with some work on OrderedKeyDotDict
# def test_ValidationResultIdentifier__init__nested_except_the_top_layer():
#     ValidationResultIdentifier(
#         ({
#             "data_asset_identifier" : {
#                 "datasource" : "a",
#                 "generator" : "b",
#                 "generator_asset" : "c",
#             },
#             "suite_name": "hello",
#             "purpose": "testing",
#         },{
#             "execution_context": "testing",
#             "start_time_utc": 12345,
#         }),
#         coerce_types=True,
#     )

# TODO: This style of instantiation isn't currently supported, but could be with some work on OrderedKeyDotDict
# def test_ValidationResultIdentifier__init__entirely_flat():
#     ValidationResultIdentifier(
#         ("a", "b", "c", "hello", "testing", "testing", 12345),
#         coerce_types=True,
#     )