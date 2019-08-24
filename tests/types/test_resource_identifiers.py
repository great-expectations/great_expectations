import pytest

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

def test_ValidationResultIdentifier():
    print("A")
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : DataAssetIdentifier("a", "b", "c"),
                "suite_name": "hello",
                "purpose": "testing",
            },
            "purpose" : "default",
            "run_id" : "100",
        }
    )

    print("AA")
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
            "purpose" : "default",
            "run_id" : "100",
        }
    )

    print("B")
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"), #DataAssetIdentifier("a", "b", "c"),
                "suite_name": "hello",
                "purpose": "testing",
            },
            "purpose" : "default",
            "run_id" : "100",
        }
    )

    print("C")
    ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"), #DataAssetIdentifier("a", "b", "c"),
                "suite_name": "hello",
                "purpose": "testing",
            },
            "purpose" : "default",
            "run_id" : "100",
        }
    )