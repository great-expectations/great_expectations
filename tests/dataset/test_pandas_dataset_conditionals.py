import datetime
import json

import numpy as np
import pandas as pd
import pytest

import great_expectations as ge
from tests.test_utils import (
    expectationConfigurationSchema,
    expectationValidationResultSchema,
)


def duplicate_and_obfuscuate(df):
    df_a = df.copy()
    df_b = df.copy()

    df_a["group"] = "a"
    df_b["group"] = "b"

    for column in df_b.columns:
        if column == "group":
            continue

        if df_b[column].dtype in ["int", "float"]:
            df_b[column] += np.max(df_b[column])
            continue

        if df_b[column].dtype == "object" and all(
            [(type(elem) == str) or (elem is None) for elem in df_b[column]]
        ):
            df_b[column] = df_b[column].astype(str) + "__obfuscate"
            continue

    return ge.from_pandas(pd.concat([df_a, df_b], ignore_index=True, sort=False))


def test_expectation_decorator_summary_mode():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset({"x": [1, 2, 3, 4, 5, 6, 7, 7, None, None]})
    )

    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    # print('&'*80)
    # print(json.dumps(df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY"),
    #                  indent=2))

    exp_output = expectationValidationResultSchema.load(
        {
            "success": False,
            "result": {
                "element_count": 10,
                "missing_count": 2,
                "missing_percent": 20.0,
                "unexpected_count": 3,
                "partial_unexpected_counts": [
                    {"value": 7.0, "count": 2},
                    {"value": 6.0, "count": 1},
                ],
                "unexpected_percent": 30.0,
                "unexpected_percent_nonmissing": 37.5,
                "partial_unexpected_list": [6.0, 7.0, 7.0],
                "partial_unexpected_index_list": [5, 6, 7],
            },
        }
    )

    assert (
        df.expect_column_values_to_be_between(
            "x",
            min_value=1,
            max_value=5,
            result_format="SUMMARY",
            condition_parser="pandas",
            row_condition="group=='a'",
        )
        == exp_output
    )

    assert (
        df.expect_column_values_to_be_between(
            "x", min_value=1, max_value=5, result_format="SUMMARY"
        )
        != exp_output
    )

    exp_output = expectationValidationResultSchema.load(
        {
            "success": True,
            "result": {
                "observed_value": 4.375,
                "element_count": 10,
                "missing_count": 2,
                "missing_percent": 20.0,
            },
        }
    )

    assert (
        df.expect_column_mean_to_be_between(
            "x",
            3,
            7,
            result_format="SUMMARY",
            row_condition="group=='a'",
            condition_parser="pandas",
        )
        == exp_output
    )

    assert (
        df.expect_column_mean_to_be_between("x", 3, 7, result_format="SUMMARY")
        != exp_output
    )


def test_positional_arguments():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset(
            {
                "x": [1, 3, 5, 7, 9],
                "y": [2, 4, 6, 8, 10],
                "z": [None, "a", "b", "c", "abc"],
            }
        )
    )

    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    exp_output = expectationValidationResultSchema.load(
        {
            "success": True,
            "result": {
                "observed_value": 5,
                "element_count": 5,
                "missing_count": 0,
                "missing_percent": 0.0,
            },
        }
    )

    assert (
        df.expect_column_mean_to_be_between(
            "x", 4, 6, condition_parser="pandas", row_condition='group=="a"'
        )
        == exp_output
    )
    assert df.expect_column_mean_to_be_between("x", 4, 6) != exp_output

    out = df.expect_column_values_to_be_between(
        "y", 1, 6, condition_parser="pandas", row_condition='group=="a"'
    )
    t = {
        "out": {
            "success": False,
            "unexpected_list": [8, 10],
            "unexpected_index_list": [3, 4],
        }
    }
    if "out" in t:
        assert t["out"]["success"] == out.success
        if "unexpected_index_list" in t["out"]:
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
        if "unexpected_list" in t["out"]:
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]

    out = df.expect_column_values_to_be_between(
        "y", 1, 6, mostly=0.5, condition_parser="pandas", row_condition='group=="a"'
    )
    t = {
        "out": {
            "success": True,
            "unexpected_list": [8, 10],
            "unexpected_index_list": [3, 4],
        }
    }
    if "out" in t:
        assert t["out"]["success"] == out.success
        if "unexpected_index_list" in t["out"]:
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
        if "unexpected_list" in t["out"]:
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]

    out = df.expect_column_values_to_be_in_set(
        "z", ["a", "b", "c"], condition_parser="pandas", row_condition='group=="a"'
    )
    t = {
        "out": {
            "success": False,
            "unexpected_list": ["abc"],
            "unexpected_index_list": [4],
        }
    }
    if "out" in t:
        assert t["out"]["success"] == out.success
        if "unexpected_index_list" in t["out"]:
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
        if "unexpected_list" in t["out"]:
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]

    out = df.expect_column_values_to_be_in_set(
        "z",
        ["a", "b", "c"],
        mostly=0.5,
        condition_parser="pandas",
        row_condition='group=="a"',
    )
    t = {
        "out": {
            "success": True,
            "unexpected_list": ["abc"],
            "unexpected_index_list": [4],
        }
    }
    if "out" in t:
        assert t["out"]["success"] == out.success
        if "unexpected_index_list" in t["out"]:
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
        if "unexpected_list" in t["out"]:
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]


def test_result_format_argument_in_decorators():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset(
            {
                "x": [1, 3, 5, 7, 9],
                "y": [2, 4, 6, 8, 10],
                "z": [None, "a", "b", "c", "abc"],
            }
        )
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    # Test explicit Nones in result_format
    exp_output = expectationValidationResultSchema.load(
        {
            "success": True,
            "result": {
                "observed_value": 5,
                "element_count": 5,
                "missing_count": 0,
                "missing_percent": 0.0,
            },
        }
    )

    assert (
        df.expect_column_mean_to_be_between(
            "x",
            4,
            6,
            result_format=None,
            condition_parser="pandas",
            row_condition="group=='a'",
        )
        == exp_output
    )

    assert (
        df.expect_column_mean_to_be_between("x", 4, 6, result_format=None) != exp_output
    )

    exp_output = expectationValidationResultSchema.load(
        {
            "result": {
                "element_count": 5,
                "missing_count": 0,
                "missing_percent": 0.0,
                "partial_unexpected_counts": [
                    {"count": 1, "value": 8},
                    {"count": 1, "value": 10},
                ],
                "partial_unexpected_index_list": [3, 4],
                "partial_unexpected_list": [8, 10],
                "unexpected_count": 2,
                "unexpected_index_list": [3, 4],
                "unexpected_list": [8, 10],
                "unexpected_percent": 40.0,
                "unexpected_percent_nonmissing": 40.0,
            },
            "success": False,
        }
    )

    assert (
        df.expect_column_values_to_be_between(
            "y",
            1,
            6,
            result_format=None,
            condition_parser="pandas",
            row_condition="group=='a'",
        )
        == exp_output
    )

    assert df.expect_column_values_to_be_between(
        "y",
        1,
        6,
        result_format=None,
        condition_parser="pandas",
        row_condition="group=='a'",
    ) != df.expect_column_values_to_be_between("y", 1, 6, result_format=None)
    # Test unknown output format
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(
            "y",
            1,
            6,
            result_format="QUACK",
            condition_parser="pandas",
            row_condition="group=='a'",
        )

    with pytest.raises(ValueError):
        df.expect_column_mean_to_be_between(
            "x",
            4,
            6,
            result_format="QUACK",
            condition_parser="pandas",
            row_condition="group=='a'",
        )


def test_ge_pandas_subsetting_with_conditionals():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset(
            {
                "A": [1, 2, 3, 4],
                "B": [5, 6, 7, 8],
                "C": ["a", "b", "c", "d"],
                "D": ["e", "f", "g", "h"],
            }
        )
    )

    # Put some simple expectations on the data frame
    df.expect_column_values_to_be_in_set(
        "A", [1, 2, 3, 4], condition_parser="pandas", row_condition="group=='a'"
    )
    df.expect_column_values_to_be_in_set(
        "B", [5, 6, 7, 8], condition_parser="pandas", row_condition="group=='a'"
    )
    df.expect_column_values_to_be_in_set(
        "C", ["a", "b", "c", "d"], condition_parser="pandas", row_condition="group=='a'"
    )
    df.expect_column_values_to_be_in_set(
        "D", ["e", "f", "g", "h"], condition_parser="pandas", row_condition="group=='a'"
    )

    # The subsetted data frame should:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Inherit ALL the expectations of the parent data frame

    exp1 = df.get_expectation_suite().expectations

    sub1 = df[["A", "D"]]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df[["A"]]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df[:3]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df[1:2]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df[:-1]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df[-1:]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df.iloc[:3, 1:4]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1

    sub1 = df.loc[0:, "A":"B"]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.get_expectation_suite().expectations == exp1


def test_row_condition_in_expectation_config():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset({"x": [1, 2, 3, 4, 5, 6, 7, 7, None, None]})
    )

    df.set_default_expectation_argument("include_config", True)

    exp_expectation_config = expectationConfigurationSchema.load(
        {
            "meta": {},
            "kwargs": {
                "column": "x",
                "min_value": 1,
                "max_value": 5,
                "result_format": "SUMMARY",
                "row_condition": "group=='a'",
                "condition_parser": "pandas",
            },
            "expectation_type": "expect_column_values_to_be_between",
        }
    )

    assert (
        "row_condition"
        in df.expect_column_values_to_be_between(
            "x",
            min_value=1,
            max_value=5,
            result_format="SUMMARY",
            condition_parser="pandas",
            row_condition="group=='a'",
        ).expectation_config["kwargs"]
    )

    assert (
        "group=='a'"
        == df.expect_column_values_to_be_between(
            "x",
            min_value=1,
            max_value=5,
            result_format="SUMMARY",
            condition_parser="pandas",
            row_condition="group=='a'",
        ).expectation_config["kwargs"]["row_condition"]
    )

    assert df.expect_column_values_to_be_between(
        "x",
        min_value=1,
        max_value=5,
        result_format="SUMMARY",
        condition_parser="pandas",
        row_condition="group=='a'",
    ).expectation_config.isEquivalentTo(exp_expectation_config)


# TODO: this test should be changed when other engines will be implemented
def test_condition_parser_in_expectation_config():
    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset({"x": [1, 2, 3, 4, 5, 6, 7, 7, None, None]})
    )

    df.set_default_expectation_argument("include_config", True)

    observe = df.expect_column_values_to_be_between(
        "x",
        min_value=1,
        max_value=5,
        result_format="SUMMARY",
        condition_parser="pandas",
        row_condition="group=='a'",
    )
    assert "pandas" == observe.expectation_config["kwargs"]["condition_parser"]

    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between(
            "x",
            min_value=1,
            max_value=5,
            result_format="SUMMARY",
            row_condition="group=='a'",
            condition_parser="SQL",
        )
