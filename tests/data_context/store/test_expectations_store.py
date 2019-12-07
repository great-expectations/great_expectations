import pytest

from great_expectations.core import ExpectationSuite
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.data_context.types import (
    ExpectationSuiteIdentifier,
    DataAssetIdentifier,
)


def test_expectations_store():
    my_store = ExpectationsStore()

    with pytest.raises(TypeError):
        my_store.set("not_a_ValidationResultIdentifier")

    ns_1 = ExpectationSuiteIdentifier.from_tuple(("a", "b", "c", "warning"))
    my_store.set(ns_1, ExpectationSuite(data_asset_name=DataAssetIdentifier("a", "b", "c"),
                                        expectation_suite_name="warning"))
    assert my_store.get(ns_1) == ExpectationSuite(data_asset_name=DataAssetIdentifier("a", "b", "c"),
                                                  expectation_suite_name="warning")

    ns_2 = ExpectationSuiteIdentifier.from_tuple(("a", "b", "c", "failure"))
    my_store.set(ns_2, ExpectationSuite(data_asset_name=DataAssetIdentifier("a", "b", "c"),
                                        expectation_suite_name="failure"))
    assert my_store.get(ns_2) == ExpectationSuite(data_asset_name=DataAssetIdentifier("a", "b", "c"),
                                                  expectation_suite_name="failure")

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }

