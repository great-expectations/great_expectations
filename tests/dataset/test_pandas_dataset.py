import datetime
import json

import pandas as pd
import pytest

import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.profile import ColumnsExistProfiler
from great_expectations.self_check.util import (
    expectationSuiteSchema,
    expectationValidationResultSchema,
)


def test_expect_column_values_to_be_dateutil_parseable():
    D = ge.dataset.PandasDataset(
        {
            "c1": ["03/06/09", "23 April 1973", "January 9, 2016"],
            "c2": ["9/8/2012", "covfefe", 25],
            "c3": ["Jared", "June 1, 2013", "July 18, 1976"],
            "c4": ["1", "2", "49000004632"],
            "already_datetime": [
                datetime.datetime(2015, 1, 1),
                datetime.datetime(2016, 1, 1),
                datetime.datetime(2017, 1, 1),
            ],
        }
    )
    D.set_default_expectation_argument("result_format", "COMPLETE")

    T = [
        {
            "in": {"column": "c1"},
            "out": {
                "success": True,
                "unexpected_list": [],
                "unexpected_index_list": [],
            },
        },
        {
            "in": {"column": "c2", "catch_exceptions": True},
            # 'out':{'success':False, 'unexpected_list':['covfefe', 25], 'unexpected_index_list': [1, 2]}},
            "error": {
                "traceback_substring": "TypeError: Values passed to expect_column_values_to_be_dateutil_parseable must be of type string"
            },
        },
        {
            "in": {"column": "c3"},
            "out": {
                "success": False,
                "unexpected_list": ["Jared"],
                "unexpected_index_list": [0],
            },
        },
        {
            "in": {"column": "c3", "mostly": 0.5},
            "out": {
                "success": True,
                "unexpected_list": ["Jared"],
                "unexpected_index_list": [0],
            },
        },
        {
            "in": {"column": "c4"},
            "out": {
                "success": False,
                "unexpected_list": ["49000004632"],
                "unexpected_index_list": [2],
            },
        },
        {
            "in": {"column": "already_datetime", "catch_exceptions": True},
            "error": {
                "traceback_substring": "TypeError: Values passed to expect_column_values_to_be_dateutil_parseable must be of type string"
            },
        },
    ]

    for t in T:
        out = D.expect_column_values_to_be_dateutil_parseable(**t["in"])
        if "out" in t:
            assert t["out"]["success"] == out.success
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]
        elif "error" in t:
            assert out.exception_info["raised_exception"] is True
            assert (
                t["error"]["traceback_substring"]
                in out.exception_info["exception_traceback"]
            )


def test_expect_column_values_to_be_json_parseable():
    d1 = json.dumps({"i": [1, 2, 3], "j": 35, "k": {"x": "five", "y": 5, "z": "101"}})
    d2 = json.dumps({"i": 1, "j": 2, "k": [3, 4, 5]})
    d3 = json.dumps({"i": "a", "j": "b", "k": "c"})
    d4 = json.dumps(
        {"i": [4, 5], "j": [6, 7], "k": [8, 9], "l": {4: "x", 5: "y", 6: "z"}}
    )
    D = ge.dataset.PandasDataset(
        {
            "json_col": [d1, d2, d3, d4],
            "not_json": [4, 5, 6, 7],
            "py_dict": [
                {"a": 1, "out": 1},
                {"b": 2, "out": 4},
                {"c": 3, "out": 9},
                {"d": 4, "out": 16},
            ],
            "most": [d1, d2, d3, "d4"],
        }
    )
    D.set_default_expectation_argument("result_format", "COMPLETE")

    T = [
        {
            "in": {"column": "json_col"},
            "out": {
                "success": True,
                "unexpected_index_list": [],
                "unexpected_list": [],
            },
        },
        {
            "in": {"column": "not_json"},
            "out": {
                "success": False,
                "unexpected_index_list": [0, 1, 2, 3],
                "unexpected_list": [4, 5, 6, 7],
            },
        },
        {
            "in": {"column": "py_dict"},
            "out": {
                "success": False,
                "unexpected_index_list": [0, 1, 2, 3],
                "unexpected_list": [
                    {"a": 1, "out": 1},
                    {"b": 2, "out": 4},
                    {"c": 3, "out": 9},
                    {"d": 4, "out": 16},
                ],
            },
        },
        {
            "in": {"column": "most"},
            "out": {
                "success": False,
                "unexpected_index_list": [3],
                "unexpected_list": ["d4"],
            },
        },
        {
            "in": {"column": "most", "mostly": 0.75},
            "out": {
                "success": True,
                "unexpected_index_list": [3],
                "unexpected_list": ["d4"],
            },
        },
    ]

    for t in T:
        out = D.expect_column_values_to_be_json_parseable(**t["in"])
        assert t["out"]["success"] == out.success
        assert t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
        assert t["out"]["unexpected_list"] == out.result["unexpected_list"]


def test_expectation_decorator_summary_mode():
    df = ge.dataset.PandasDataset(
        {
            "x": [1, 2, 3, 4, 5, 6, 7, 7, None, None],
        }
    )
    df.set_default_expectation_argument("result_format", "COMPLETE")
    df.set_default_expectation_argument("include_config", False)

    # print '&'*80
    # print json.dumps(df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY"), indent=2)

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
            "x", min_value=1, max_value=5, result_format="SUMMARY"
        )
        == exp_output
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
        df.expect_column_mean_to_be_between("x", 3, 7, result_format="SUMMARY")
        == exp_output
    )


def test_positional_arguments():

    df = ge.dataset.PandasDataset(
        {"x": [1, 3, 5, 7, 9], "y": [2, 4, 6, 8, 10], "z": [None, "a", "b", "c", "abc"]}
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

    assert df.expect_column_mean_to_be_between("x", 4, 6) == exp_output

    out = df.expect_column_values_to_be_between("y", 1, 6)
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

    out = df.expect_column_values_to_be_between("y", 1, 8, strict_max=True)
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

    out = df.expect_column_values_to_be_between("y", 2, 100, strict_min=True)
    t = {
        "out": {"success": False, "unexpected_list": [2], "unexpected_index_list": [0]}
    }
    if "out" in t:
        assert t["out"]["success"] == out.success
        if "unexpected_index_list" in t["out"]:
            assert (
                t["out"]["unexpected_index_list"] == out.result["unexpected_index_list"]
            )
        if "unexpected_list" in t["out"]:
            assert t["out"]["unexpected_list"] == out.result["unexpected_list"]

    out = df.expect_column_values_to_be_between("y", 1, 6, mostly=0.5)
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

    out = df.expect_column_values_to_be_in_set("z", ["a", "b", "c"])
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

    out = df.expect_column_values_to_be_in_set("z", ["a", "b", "c"], mostly=0.5)
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
    df = ge.dataset.PandasDataset(
        {"x": [1, 3, 5, 7, 9], "y": [2, 4, 6, 8, 10], "z": [None, "a", "b", "c", "abc"]}
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
        df.expect_column_mean_to_be_between("x", 4, 6, result_format=None) == exp_output
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
        df.expect_column_values_to_be_between("y", 1, 6, result_format=None)
        == exp_output
    )

    # Test unknown output format
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between("y", 1, 6, result_format="QUACK")

    with pytest.raises(ValueError):
        df.expect_column_mean_to_be_between("x", 4, 6, result_format="QUACK")


def test_from_pandas():
    pd_df = pd.DataFrame(
        {"x": [1, 3, 5, 7, 9], "y": [2, 4, 6, 8, 10], "z": [None, "a", "b", "c", "abc"]}
    )

    ge_df = ge.from_pandas(pd_df)
    assert isinstance(ge_df, ge.data_asset.DataAsset)
    assert list(ge_df.columns) == ["x", "y", "z"]
    assert list(ge_df["x"]) == list(pd_df["x"])
    assert list(ge_df["y"]) == list(pd_df["y"])
    assert list(ge_df["z"]) == list(pd_df["z"])

    # make an empty subclass to test dataset_class argument
    class CustomPandasDataset(ge.dataset.PandasDataset):
        pass

    ge_df_custom = ge.from_pandas(pd_df, dataset_class=CustomPandasDataset)

    assert not isinstance(ge_df, CustomPandasDataset)
    assert isinstance(ge_df_custom, CustomPandasDataset)
    assert list(ge_df_custom.columns) == ["x", "y", "z"]
    assert list(ge_df_custom["x"]) == list(pd_df["x"])
    assert list(ge_df_custom["y"]) == list(pd_df["y"])
    assert list(ge_df_custom["z"]) == list(pd_df["z"])


def test_ge_pandas_concatenating_no_autoinspect():
    df1 = ge.dataset.PandasDataset({"A": ["A0", "A1", "A2"], "B": ["B0", "B1", "B2"]})

    df1.expect_column_to_exist("A")
    df1.expect_column_to_exist("B")
    df1.expect_column_values_to_match_regex("A", "^A[0-2]$")
    df1.expect_column_values_to_match_regex("B", "^B[0-2]$")

    df2 = ge.dataset.PandasDataset({"A": ["A3", "A4", "A5"], "B": ["B3", "B4", "B5"]})

    df2.expect_column_to_exist("A")
    df2.expect_column_to_exist("B")
    df2.expect_column_values_to_match_regex("A", "^A[3-5]$")
    df2.expect_column_values_to_match_regex("B", "^B[3-5]$")

    df = pd.concat([df1, df2])

    exp_c = []

    # The concatenated data frame will:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Have no expectations (since no default expectations are created), even expectations that were common
    #      to the concatenated dataframes and still make sense (since no autoinspection happens).

    assert isinstance(df, ge.dataset.PandasDataset)
    assert df.get_expectation_suite().expectations == exp_c


def test_ge_pandas_joining():
    df1 = ge.dataset.PandasDataset(
        {"A": ["A0", "A1", "A2"], "B": ["B0", "B1", "B2"]}, index=["K0", "K1", "K2"]
    )

    df1.expect_column_values_to_match_regex("A", "^A[0-2]$")
    df1.expect_column_values_to_match_regex("B", "^B[0-2]$")

    df2 = ge.dataset.PandasDataset(
        {"C": ["C0", "C2", "C3"], "D": ["C0", "D2", "D3"]}, index=["K0", "K2", "K3"]
    )

    df2.expect_column_values_to_match_regex("C", "^C[0-2]$")
    df2.expect_column_values_to_match_regex("D", "^D[0-2]$")

    df = df1.join(df2)

    exp_j = [
        # No autoinspection is default 20180920
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'A'}},
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'B'}},
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'C'}},
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'D'}}
    ]

    # The joined data frame will:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Have no expectations (no autoinspection)

    assert isinstance(df, ge.dataset.PandasDataset)
    assert df.get_expectation_suite().expectations == exp_j


def test_ge_pandas_merging():
    df1 = ge.dataset.PandasDataset({"id": [1, 2, 3, 4], "name": ["a", "b", "c", "d"]})

    df1.expect_column_values_to_match_regex("name", "^[A-Za-z ]+$")

    df2 = ge.dataset.PandasDataset(
        {"id": [1, 2, 3, 4], "salary": [57000, 52000, 59000, 65000]}
    )

    df2.expect_column_values_to_match_regex("salary", "^[0-9]{4,6}$")

    df = df1.merge(df2, on="id")

    exp_m = [
        # No autoinspection as of 20180920
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'id'}},
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'name'}},
        # {'expectation_type': 'expect_column_to_exist',
        #  'kwargs': {'column': 'salary'}}
    ]

    # The merged data frame will:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Have no expectations (no autoinspection is now default)

    assert isinstance(df, ge.dataset.PandasDataset)
    assert df.get_expectation_suite().expectations == exp_m


def test_ge_pandas_sampling():
    df = ge.dataset.PandasDataset(
        {
            "A": [1, 2, 3, 4],
            "B": [5, 6, 7, 8],
            "C": ["a", "b", "c", "d"],
            "D": ["e", "f", "g", "h"],
        }
    )

    # Put some simple expectations on the data frame
    df.profile(profiler=ColumnsExistProfiler)
    df.expect_column_values_to_be_in_set("A", [1, 2, 3, 4])
    df.expect_column_values_to_be_in_set("B", [5, 6, 7, 8])
    df.expect_column_values_to_be_in_set("C", ["a", "b", "c", "d"])
    df.expect_column_values_to_be_in_set("D", ["e", "f", "g", "h"])

    exp1 = df.get_expectation_suite().expectations

    # The sampled data frame should:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Inherit ALL the expectations of the parent data frame

    samp1 = df.sample(n=2)
    assert isinstance(samp1, ge.dataset.PandasDataset)
    assert samp1.get_expectation_suite().expectations == exp1

    samp1 = df.sample(frac=0.25, replace=True)
    assert isinstance(samp1, ge.dataset.PandasDataset)
    assert samp1.get_expectation_suite().expectations == exp1

    # Change expectation on column "D", sample, and check expectations.
    # The failing expectation on column "D" is NOT automatically dropped
    # in the sample.
    df.expect_column_values_to_be_in_set("D", ["e", "f", "g", "x"])
    samp1 = df.sample(n=2)
    exp1 = expectationSuiteSchema.load(
        {
            "expectation_suite_name": "test",
            "expectations": [
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "A"},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "B"},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "C"},
                },
                {
                    "expectation_type": "expect_column_to_exist",
                    "kwargs": {"column": "D"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "A", "value_set": [1, 2, 3, 4]},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "B", "value_set": [5, 6, 7, 8]},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "C", "value_set": ["a", "b", "c", "d"]},
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": "D", "value_set": ["e", "f", "g", "x"]},
                },
            ],
        }
    )
    assert (
        samp1.get_expectation_suite(discard_failed_expectations=False).expectations
        == exp1.expectations
    )


def test_ge_pandas_subsetting():
    df = ge.dataset.PandasDataset(
        {
            "A": [1, 2, 3, 4],
            "B": [5, 6, 7, 8],
            "C": ["a", "b", "c", "d"],
            "D": ["e", "f", "g", "h"],
        }
    )

    # Put some simple expectations on the data frame
    df.expect_column_values_to_be_in_set("A", [1, 2, 3, 4])
    df.expect_column_values_to_be_in_set("B", [5, 6, 7, 8])
    df.expect_column_values_to_be_in_set("C", ["a", "b", "c", "d"])
    df.expect_column_values_to_be_in_set("D", ["e", "f", "g", "h"])

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


def test_ge_pandas_automatic_failure_removal():
    df = ge.dataset.PandasDataset(
        {
            "A": [1, 2, 3, 4],
            "B": [5, 6, 7, 8],
            "C": ["a", "b", "c", "d"],
            "D": ["e", "f", "g", "h"],
        }
    )

    # Put some simple expectations on the data frame
    df.profile(ge.profile.ColumnsExistProfiler)
    df.expect_column_values_to_be_in_set("A", [1, 2, 3, 4])
    df.expect_column_values_to_be_in_set("B", [5, 6, 7, 8])
    df.expect_column_values_to_be_in_set("C", ["w", "x", "y", "z"])
    df.expect_column_values_to_be_in_set("D", ["e", "f", "g", "h"])

    # First check that failing expectations are NOT automatically
    # dropped when sampling.
    # For this data frame, the expectation on column "C" above fails.
    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "B"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "C"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "A", "value_set": [1, 2, 3, 4]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "B", "value_set": [5, 6, 7, 8]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "C", "value_set": ["w", "x", "y", "z"]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "D", "value_set": ["e", "f", "g", "h"]},
        ),
    ]
    samp1 = df.sample(n=2)
    assert (
        samp1.get_expectation_suite(discard_failed_expectations=False).expectations
        == exp1
    )

    # Now check subsetting to verify that failing expectations are NOT
    # automatically dropped when subsetting.
    sub1 = df[["A", "D"]]
    assert (
        samp1.get_expectation_suite(discard_failed_expectations=False).expectations
        == exp1
    )

    # Set property/attribute so that failing expectations are
    # automatically removed when sampling or subsetting.
    df.discard_subset_failing_expectations = True

    ###
    # Note: Order matters in this test, and a validationoperator may change order
    ###

    exp_samp = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "B"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "C"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "A", "value_set": [1, 2, 3, 4]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "B", "value_set": [5, 6, 7, 8]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "D", "value_set": ["e", "f", "g", "h"]},
        ),
    ]

    samp2 = df.sample(n=2)
    assert (
        samp2.get_expectation_suite(discard_failed_expectations=False).expectations
        == exp_samp
    )

    # Now check subsetting. In additional to the failure on column "C",
    # the expectations on column "B" now fail since column "B" doesn't
    # exist in the subset.
    sub2 = df[["A", "D"]]
    exp_sub = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "A", "value_set": [1, 2, 3, 4]},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist", kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={"column": "D", "value_set": ["e", "f", "g", "h"]},
        ),
    ]
    assert (
        samp2.get_expectation_suite(discard_failed_expectations=False).expectations
        == exp_samp
    )


def test_subclass_pandas_subset_retains_subclass():
    """A subclass of PandasDataset should still be that subclass after a Pandas subsetting operation"""

    class CustomPandasDataset(ge.dataset.PandasDataset):
        @ge.dataset.MetaPandasDataset.column_map_expectation
        def expect_column_values_to_be_odd(self, column):
            return column.map(lambda x: x % 2)

        @ge.dataset.MetaPandasDataset.column_map_expectation
        def expectation_that_crashes_on_sixes(self, column):
            return column.map(lambda x: (x - 6) / 0 != "duck")

    df = CustomPandasDataset(
        {
            "all_odd": [1, 3, 5, 5, 5, 7, 9, 9, 9, 11],
            "mostly_odd": [1, 3, 5, 7, 9, 2, 4, 1, 3, 5],
            "all_even": [2, 4, 4, 6, 6, 6, 8, 8, 8, 8],
            "odd_missing": [1, 3, 5, None, None, None, None, 1, 3, None],
            "mixed_missing": [1, 3, 5, None, None, 2, 4, 1, 3, None],
            "all_missing": [None, None, None, None, None, None, None, None, None, None],
        }
    )

    df2 = df.sample(frac=0.5)
    assert type(df2) == type(df)

    def test_validate_map_expectation_on_categorical_column(self):
        """Map expectations should work on categorical columns"""

        D = ge.dataset.PandasDataset(
            {
                "cat_column_1": [
                    "cat_one",
                    "cat_two",
                    "cat_one",
                    "cat_two",
                    "cat_one",
                    "cat_two",
                    "cat_one",
                    "cat_two",
                ],
            }
        )

        D["cat_column_1"] = D["cat_column_1"].astype("category")

        D.set_default_expectation_argument("result_format", "COMPLETE")

        out = D.expect_column_value_lengths_to_equal("cat_column_1", 7)

        self.assertEqual(out["success"], True)


def test_pandas_deepcopy():
    import copy

    df = ge.dataset.PandasDataset({"a": [1, 2, 3]})
    df2 = copy.deepcopy(df)

    df["a"] = [2, 3, 4]

    # Our copied dataframe should not be affected
    assert df2.expect_column_to_exist("a").success == True
    assert list(df["a"]) == [2, 3, 4]
    assert list(df2["a"]) == [1, 2, 3]


def test_ge_value_count_of_object_dtype_column_with_mixed_types():
    """
    Having mixed type values in a object dtype column (e.g., strings and floats)
    used to raise a TypeError when sorting value_counts. This test verifies
    that the issue is fixed.
    """
    df = ge.dataset.PandasDataset(
        {
            "A": [1.5, 0.009, 0.5, "I am a string in an otherwise float column"],
        }
    )

    value_counts = df.get_column_value_counts("A")
    assert value_counts["I am a string in an otherwise float column"] == 1


def test_expect_values_to_be_of_type_list():
    """
    Having lists in a Pandas column used to raise a ValueError when parsing to
    see if any rows had missing values. This test verifies that the issue is fixed.
    """
    df = ge.dataset.PandasDataset(
        {
            "A": [[1, 2], None, [4, 5], 6],
        }
    )

    validation = df.expect_column_values_to_be_of_type("A", "list")
    assert not validation.success


def test_expect_values_quantiles_to_be_between():
    """
    Test that quantile bounds set to zero actually get interpreted as such. Zero
    used to be interpreted as None (and thus +-inf) and we'd get false negatives.
    """
    T = [
        ([1, 2, 3, 4, 5], [0.5], [[0, 0]], False),
        ([0, 0, 0, 0, 0], [0.5], [[0, 0]], True),
    ]

    for data, quantiles, value_ranges, success in T:
        df = ge.dataset.PandasDataset({"A": data})

        validation = df.expect_column_quantile_values_to_be_between(
            "A",
            {
                "quantiles": quantiles,
                "value_ranges": value_ranges,
            },
        )
        assert validation.success is success
