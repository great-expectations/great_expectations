import copy
import json
import os
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytest

from great_expectations.data_asset.util import recursively_convert_to_json_serializable

from ..test_utils import (
    candidate_getter_is_on_temporary_notimplemented_list,
    get_dataset,
)

dir_path = os.path.dirname(os.path.realpath(__file__))
test_config_path = os.path.join(dir_path, "test_dataset_implementations.json")
test_config = json.load(open(test_config_path), object_pairs_hook=OrderedDict)
test_datasets = test_config["test_datasets"]


def generate_ids(test):
    return ":".join([test["dataset"], test["func"]])


@pytest.mark.parametrize(
    "test", test_config["tests"], ids=[generate_ids(t) for t in test_config["tests"]]
)
def test_implementations(test_backend, test):
    should_skip = candidate_getter_is_on_temporary_notimplemented_list(
        test_backend, test["func"]
    ) or test_backend in test.get("suppress_test_for", [])
    if should_skip:
        pytest.skip()

    data = test_datasets[test["dataset"]]["data"]
    schema = test_datasets[test["dataset"]]["schemas"].get(test_backend)
    dataset = get_dataset(test_backend, data, schemas=schema)
    func = getattr(dataset, test["func"])
    run_kwargs = copy.deepcopy(test.get("kwargs", {}))
    for arg in run_kwargs.keys():
        if isinstance(run_kwargs[arg], list):
            run_kwargs[arg] = tuple(run_kwargs[arg])
    result = func(**run_kwargs)

    # NOTE: we cannot serialize pd.Series to json directly,
    # so we're going to test our preferred serialization.
    # THIS TEST DOES NOT REPRESENT THE EXPECTED RETURN VALUE
    # OF THE TESTED FUNCTION; THIS IS A JOINT TEST OF THE
    # JSON SERIALIZATION AND THE TEST.
    # See test_get_column_value_counts for a series-specific test
    if test["func"] == "get_column_value_counts":
        result = recursively_convert_to_json_serializable(result)

    if "tolerance" in test:
        assert np.allclose(test["expected"], result, test["tolerance"])
    elif isinstance(test["expected"], list):
        if len(test["expected"]) > 0 and isinstance(test["expected"][0], dict):
            for item in test["expected"]:
                assert item in result
        else:
            assert test["expected"] == result
    else:
        assert test["expected"] == result


def test_get_column_value_counts(test_backend):
    schemas = {
        "SparkDFDataset": {
            "x": "FloatType",
            "y": "IntegerType",
            "z": "IntegerType",
            "n": "IntegerType",
            "b": "BooleanType",
        },
        "mysql": {
            "x": "DOUBLE",
            "y": "INTEGER",
            "z": "INTEGER",
            "n": "INTEGER",
            "b": "TINYINT",
        },
        "mssql": {
            "x": "FLOAT",
            "y": "INTEGER",
            "z": "INTEGER",
            "n": "INTEGER",
            "b": "BIT",
        },
    }
    data = {
        "x": [2.0, 5.0],
        "y": [5, 5],
        "z": [0, 10],
        "n": [0, None],
        "b": [True, False],
    }
    dataset = get_dataset(test_backend, data, schemas=schemas)

    res = dataset.get_column_value_counts("x")
    expected = pd.Series(data["x"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"

    assert res.equals(expected)

    res = dataset.get_column_value_counts("y")
    expected = pd.Series(data["y"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("z")
    expected = pd.Series(data["z"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("n")
    expected = pd.Series(data["n"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("b")
    expected = pd.Series(data["b"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    data = {
        "a": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "b": ["a", "b", "b", "c", "c", "c", "d", "d", "d", "d"],
        "c": ["a", "b", "b", "c", "c", "c", "d", None, None, None],
        "d": ["a", "b", "c", "d", "e", "f", "g", None, None, None],
    }
    schemas = {
        "SparkDFDataset": {
            "a": "IntegerType",
            "b": "StringType",
            "c": "StringType",
            "d": "StringType",
        }
    }
    dataset = get_dataset(test_backend, data, schemas=schemas)

    res = dataset.get_column_value_counts("a")
    expected = pd.Series(data["a"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("b")
    expected = pd.Series(data["b"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("c")
    expected = pd.Series(data["c"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)

    res = dataset.get_column_value_counts("d")
    expected = pd.Series(data["d"]).value_counts()
    expected.sort_index(inplace=True)
    expected.index.name = "value"
    expected.name = "count"
    assert res.equals(expected)
