import pytest

import decimal
import warnings

import pandas as pd
import numpy as np
import great_expectations as ge

import unittest
from six import PY2

from great_expectations.core import ExpectationConfiguration, expectationConfigurationSchema, ExpectationSuite, \
    expectationSuiteSchema, ExpectationValidationResult
from great_expectations.exceptions import InvalidExpectationConfigurationError
from tests.test_utils import convert_test_obj_to_json_dict


def test_get_and_save_expectation_suite(tmp_path_factory):
    directory_name = str(tmp_path_factory.mktemp("test_get_and_save_expectation_config"))
    df = ge.dataset.PandasDataset({
        'x': [1, 2, 4],
        'y': [1, 2, 5],
        'z': ['hello', 'jello', 'mello'],
    })

    df.expect_column_values_to_be_in_set('x', [1, 2, 4])
    df.expect_column_values_to_be_in_set(
        'y', [1, 2, 4], catch_exceptions=True, include_config=True)
    df.expect_column_values_to_match_regex('z', 'ello')

    ### First test set ###

    output_config = ExpectationSuite(
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "x",
                    "value_set": [
                        1,
                        2,
                        4
                    ]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "z",
                    "regex": "ello"
                }
            )
        ],
        data_asset_name="default",
        expectation_suite_name="default",
        data_asset_type="Dataset",
        meta={
            "great_expectations.__version__": ge.__version__
        }
    )

    assert output_config == df.get_expectation_suite()

    df.save_expectation_suite(directory_name + '/temp1.json')
    with open(directory_name + '/temp1.json') as infile:
        loaded_config = expectationSuiteSchema.loads(infile.read()).data
    assert output_config == loaded_config

    ### Second test set ###

    output_config = ExpectationSuite(
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "x",
                    "value_set": [
                        1,
                        2,
                        4
                    ]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "y",
                    "value_set": [
                        1,
                        2,
                        4
                    ]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "z",
                    "regex": "ello"
                }
            )
        ],
        data_asset_name="default",
        expectation_suite_name="default",
        data_asset_type="Dataset",
        meta={
            "great_expectations.__version__": ge.__version__
        }
    )

    assert output_config == df.get_expectation_suite(discard_failed_expectations=False)
    df.save_expectation_suite(
        directory_name + '/temp2.json',
        discard_failed_expectations=False
    )
    with open(directory_name + '/temp2.json') as infile:
        loaded_suite = expectationSuiteSchema.loads(infile.read()).data
    assert output_config == loaded_suite

    ### Third test set ###

    output_config = ExpectationSuite(
        expectations=[
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "x",
                    "value_set": [
                        1,
                        2,
                        4
                    ],
                    "result_format": "BASIC"
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "z",
                    "regex": "ello",
                    "result_format": "BASIC"
                }
            )
        ],
        data_asset_name="default",
        expectation_suite_name="default",
        data_asset_type="Dataset",
        meta={
            "great_expectations.__version__": ge.__version__
        }
    )
    assert output_config == df.get_expectation_suite(
        discard_result_format_kwargs=False,
        discard_include_config_kwargs=False,
        discard_catch_exceptions_kwargs=False)

    df.save_expectation_suite(
        directory_name + '/temp3.json',
        discard_result_format_kwargs=False,
        discard_include_config_kwargs=False,
        discard_catch_exceptions_kwargs=False,
    )
    with open(directory_name + '/temp3.json') as infile:
        loaded_suite = expectationSuiteSchema.loads(infile.read()).data
    assert output_config == loaded_suite


def test_expectation_meta():
    df = ge.dataset.PandasDataset({
        'x': [1, 2, 4],
        'y': [1, 2, 5],
        'z': ['hello', 'jello', 'mello'],
    })
    result = df.expect_column_median_to_be_between(
        'x', 2, 2, meta={"notes": "This expectation is for lolz."})
    k = 0
    assert True is result.success
    suite = df.get_expectation_suite()
    for expectation_config in suite.expectations:
        if expectation_config.expectation_type == 'expect_column_median_to_be_between':
            k += 1
            assert {"notes": "This expectation is for lolz."} == expectation_config.meta
    assert 1 == k

    # This should raise an error because meta isn't serializable.
    with pytest.raises(InvalidExpectationConfigurationError) as exc:
        df.expect_column_values_to_be_increasing("x", meta={"unserializable_content": np.complex(0, 0)})
    assert "which cannot be serialized to json" in exc.value.message


def test_set_default_expectation_argument():
    df = ge.dataset.PandasDataset({
        'x': [1, 2, 4],
        'y': [1, 2, 5],
        'z': ['hello', 'jello', 'mello'],
    })

    assert {"include_config": False,
            "catch_exceptions": False,
            "result_format": 'BASIC'} == df.get_default_expectation_arguments()

    df.set_default_expectation_argument("result_format", "SUMMARY")

    assert {
            "include_config": False,
            "catch_exceptions": False,
            "result_format": 'SUMMARY',
    } == df.get_default_expectation_arguments()


def test_test_column_map_expectation_function():
    asset = ge.dataset.PandasDataset({
        'x': [1, 3, 5, 7, 9],
        'y': [1, 2, None, 7, 9],
    })

    def is_odd(self, column, mostly=None, result_format=None, include_config=False, catch_exceptions=None, meta=None):
        return column % 2 == 1

    assert asset.test_column_map_expectation_function(is_odd, column='x') == ExpectationValidationResult(
        result={'element_count': 5, 'missing_count': 0, 'missing_percent': 0, 'unexpected_percent': 0.0,
                'partial_unexpected_list': [], 'unexpected_percent_nonmissing': 0.0, 'unexpected_count': 0},
        success=True)

    assert asset.test_column_map_expectation_function(is_odd, 'x', result_format="BOOLEAN_ONLY") == \
        ExpectationValidationResult(success=True)

    assert asset.test_column_map_expectation_function(is_odd, column='y', result_format="BOOLEAN_ONLY") == \
        ExpectationValidationResult(success=False)

    assert asset.test_column_map_expectation_function(is_odd, column='y', result_format="BOOLEAN_ONLY", mostly=.7) == \
        ExpectationValidationResult(success=True)


def test_test_column_aggregate_expectation_function():
    asset = ge.dataset.PandasDataset({
        'x': [1, 3, 5, 7, 9],
        'y': [1, 2, None, 7, 9],
    })

    def expect_second_value_to_be(self, column, value,
                                  result_format=None, include_config=False, catch_exceptions=None, meta=None):
        return {
            "success": self[column].iloc[1] == value,
            "result": {
                "observed_value": self[column].iloc[1],
            }
        }

    assert asset.test_column_aggregate_expectation_function(expect_second_value_to_be, 'x', 2) == \
        ExpectationValidationResult(
            result={'observed_value': 3, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0.0},
            success=False
        )

    assert asset.test_column_aggregate_expectation_function(expect_second_value_to_be, column='x', value=3) == \
        ExpectationValidationResult(
            result={'observed_value': 3.0, 'element_count': 5, 'missing_count': 0, 'missing_percent': 0.0},
            success=True
        )

    assert asset.test_column_aggregate_expectation_function(
        expect_second_value_to_be, 'y', value=3, result_format="BOOLEAN_ONLY") == \
        ExpectationValidationResult(success=False)

    assert asset.test_column_aggregate_expectation_function(
        expect_second_value_to_be, 'y', 2, result_format="BOOLEAN_ONLY") == ExpectationValidationResult(success=True)


def test_meta_version_warning():
    asset = ge.data_asset.DataAsset()

    with pytest.warns(UserWarning) as w:
        out = asset.validate(expectation_suite=ExpectationSuite(expectations=[], data_asset_name="default",
                                                                expectation_suite_name="test",
                                                                meta={}))
    assert w[0].message.args[0] == "WARNING: No great_expectations version found in configuration object."

    with pytest.warns(UserWarning) as w:
        out = asset.validate(expectation_suite=ExpectationSuite(expectations=[], data_asset_name="default",
                                                                expectation_suite_name="test",
                                                                meta={"great_expectations.__version__": "0.0.0"}))
    assert w[0].message.args[0] == \
            "WARNING: This configuration object was built using version 0.0.0 of great_expectations, but is currently "\
            "being validated by version %s." % ge.__version__


def test_format_map_output():
    df = ge.dataset.PandasDataset({
        "x": list("abcdefghijklmnopqrstuvwxyz"),
    })

    ### Normal Test ###

    success = True
    element_count = 20
    nonnull_values = pd.Series(range(15))
    nonnull_count = 15
    boolean_mapped_success_values = pd.Series([True for i in range(15)])
    success_count = 15
    unexpected_list = []
    unexpected_index_list = []

    assert df._format_map_output(
            "BOOLEAN_ONLY",
            success,
            element_count, nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == {'success': True}

    assert df._format_map_output(
            "BASIC",
            success,
            element_count, nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == {'success': True,
            'result': {
                'element_count': 20,
                'missing_count': 5,
                'missing_percent': 25.0,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': 0.0,
                'unexpected_percent_nonmissing': 0.0
            }
    }

    assert df._format_map_output(
            "SUMMARY",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == {
            'success': True,
            'result': {
                'element_count': 20,
                'missing_count': 5,
                'missing_percent': 25.0,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': 0.0,
                'unexpected_percent_nonmissing': 0.0,
                'partial_unexpected_index_list': [],
                'partial_unexpected_counts': []
            }
    }

    assert df._format_map_output(
            "COMPLETE",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == {
            'success': True,
            'result':
                {
                    'element_count': 20,
                    'missing_count': 5,
                    'missing_percent': 25.0,
                    'partial_unexpected_list': [],
                    'unexpected_count': 0,
                    'unexpected_percent': 0.0,
                    'unexpected_percent_nonmissing': 0.0,
                    'partial_unexpected_index_list': [],
                    'partial_unexpected_counts': [],
                    'unexpected_list': [],
                    'unexpected_index_list': []
                }
        }

    ### Test the case where all elements are null ###

    success = True
    element_count = 20
    nonnull_values = pd.Series([])
    nonnull_count = 0
    boolean_mapped_success_values = pd.Series([])
    success_count = 0
    unexpected_list = []
    unexpected_index_list = []

    assert df._format_map_output(
            "BOOLEAN_ONLY",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {'success': True}

    assert df._format_map_output(
            "BASIC",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': True,
            'result': {
                'element_count': 20,
                'missing_count': 20,
                'missing_percent': 100,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': 0.0,
                'unexpected_percent_nonmissing': None
            }
        }

    assert df._format_map_output(
            "SUMMARY",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': True,
            'result': {
                'element_count': 20,
                'missing_count': 20,
                'missing_percent': 100,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': 0.0,
                'unexpected_percent_nonmissing': None,
                'partial_unexpected_index_list': [],
                'partial_unexpected_counts': []
            }
        }

    assert df._format_map_output(
            "COMPLETE",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': True,
            'result': {
                'element_count': 20,
                'missing_count': 20,
                'missing_percent': 100,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': 0.0,
                'unexpected_percent_nonmissing': None,
                'partial_unexpected_index_list': [],
                'partial_unexpected_counts': [],
                'unexpected_list': [],
                'unexpected_index_list': []
            }
        }

    ### Test the degenerate case where there are no elements ###

    success = False
    element_count = 0
    nonnull_values = pd.Series([])
    nonnull_count = 0
    boolean_mapped_success_values = pd.Series([])
    success_count = 0
    unexpected_list = []
    unexpected_index_list = []

    assert df._format_map_output(
            "BOOLEAN_ONLY",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {'success': False}

    assert df._format_map_output(
            "BASIC",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': False,
            'result': {
                'element_count': 0,
                'missing_count': 0,
                'missing_percent': None,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': None,
                'unexpected_percent_nonmissing': None
            }
        }

    assert df._format_map_output(
            "SUMMARY",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': False,
            'result': {
                'element_count': 0,
                'missing_count': 0,
                'missing_percent': None,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': None,
                'unexpected_percent_nonmissing': None,
                'partial_unexpected_counts': [],
                'partial_unexpected_index_list': []
            }
        }

    assert df._format_map_output(
            "COMPLETE",
            success,
            element_count,
            nonnull_count,
            len(unexpected_list),
            unexpected_list, unexpected_index_list
        ) == \
        {
            'success': False,
            'result': {
                'element_count': 0,
                'missing_count': 0,
                'missing_percent': None,
                'partial_unexpected_list': [],
                'unexpected_count': 0,
                'unexpected_percent': None,
                'unexpected_percent_nonmissing': None,
                'partial_unexpected_counts': [],
                'partial_unexpected_index_list': [],
                'unexpected_list': [],
                'unexpected_index_list': []
            }
        }


def test_calc_map_expectation_success():
    df = ge.dataset.PandasDataset({
        "x": list("abcdefghijklmnopqrstuvwxyz")
    })
    assert df._calc_map_expectation_success(
            success_count=10,
            nonnull_count=10,
            mostly=None
        ) == \
        (True, 1.0)

    assert df._calc_map_expectation_success(
            success_count=90,
            nonnull_count=100,
            mostly=.9
        ) == \
        (True, .9)

    assert df._calc_map_expectation_success(
            success_count=90,
            nonnull_count=100,
            mostly=.8
        ) == \
        (True, .9)

    assert df._calc_map_expectation_success(
            success_count=80,
            nonnull_count=100,
            mostly=.9
        ) == \
        (False, .8)

    assert df._calc_map_expectation_success(
            success_count=0,
            nonnull_count=0,
            mostly=None
        ) == \
        (True, None)

    assert df._calc_map_expectation_success(
            success_count=0,
            nonnull_count=100,
            mostly=None
        ) == \
        (False, 0.0)

    assert df._calc_map_expectation_success(
            success_count=decimal.Decimal(80), nonnull_count=100, mostly=.8) == \
        (False, decimal.Decimal(80) / decimal.Decimal(100))

    assert df._calc_map_expectation_success(
            success_count=100,
            nonnull_count=100,
            mostly=0
        ) == \
        (True, 1.0)

    assert df._calc_map_expectation_success(
            success_count=50,
            nonnull_count=100,
            mostly=0
        ) == \
        (True, 0.5)

    assert df._calc_map_expectation_success(
            success_count=0,
            nonnull_count=100,
            mostly=0
        ) == \
        (True, 0.0)


def test_find_expectations():
    my_df = ge.dataset.PandasDataset({
        'x': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'y': [1, 2, None, 4, None, 6, 7, 8, 9, None],
        'z': ['cello', 'hello', 'jello', 'bellow', 'fellow', 'mellow', 'wellow', 'xello', 'yellow', 'zello'],
    }, profiler=ge.profile.ColumnsExistProfiler)
    my_df.expect_column_values_to_be_of_type('x', 'int')
    my_df.expect_column_values_to_be_of_type('y', 'int')
    my_df.expect_column_values_to_be_of_type('z', 'int')
    my_df.expect_column_values_to_be_increasing('x')
    my_df.expect_column_values_to_match_regex('z', 'ello')

    assert my_df.find_expectations("expect_column_to_exist", "w") == []

    assert my_df.find_expectations("expect_column_to_exist", "x", expectation_kwargs={}) == \
        [
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "x"}
            )
        ]

    assert my_df.find_expectations("expect_column_to_exist", expectation_kwargs={"column": "y"}) == \
        [
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "y"}
            )
        ]

    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "x"
            }
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "y"
            }
        ),
        ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={
                    "column": "z"
                }
            )
    ]

    if PY2:
        assert sorted(my_df.find_expectations("expect_column_to_exist")) == sorted(exp1)
    else:
        assert my_df.find_expectations("expect_column_to_exist") == exp1

    with pytest.raises(ValueError) as exc:
        my_df.find_expectations("expect_column_to_exist", "x", {"column": "y"})

    assert 'Conflicting column names in remove_expectation:' in str(exc.value)

    exp1 = [
        ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "x"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "x", "type_": "int"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_increasing",
            kwargs={"column": "x"}
        )
    ]

    if PY2:
        assert sorted(my_df.find_expectations(column="x")) == sorted(exp1)
    else:
        assert my_df.find_expectations(column="x") == exp1


def test_remove_expectation():
    my_df = ge.dataset.PandasDataset({
        'x': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'y': [1, 2, None, 4, None, 6, 7, 8, 9, None],
        'z': ['cello', 'hello', 'jello', 'bellow', 'fellow', 'mellow', 'wellow', 'xello', 'yellow', 'zello'],
    }, profiler=ge.profile.ColumnsExistProfiler)
    my_df.expect_column_values_to_be_of_type('x', 'int')
    my_df.expect_column_values_to_be_of_type('y', 'int')
    my_df.expect_column_values_to_be_of_type('z', 'int', include_config=True, catch_exceptions=True)
    my_df.expect_column_values_to_be_increasing('x')
    my_df.expect_column_values_to_match_regex('z', 'ello')

    with pytest.raises(ValueError) as exc:
        my_df.remove_expectation("expect_column_to_exist", "w", dry_run=True),

    assert 'No matching expectation found.' in str(exc.value)

    assert my_df.remove_expectation("expect_column_to_exist", "x", expectation_kwargs={}, dry_run=True) == \
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "x"}
        )

    assert my_df.remove_expectation("expect_column_to_exist", expectation_kwargs={"column": "y"}, dry_run=True) == \
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "y"}
        )

    assert my_df.remove_expectation("expect_column_to_exist", expectation_kwargs={"column": "y"},
                                    remove_multiple_matches=True, dry_run=True) == \
        [
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "y"}
            )
        ]

    with pytest.raises(ValueError) as exc:
        my_df.remove_expectation("expect_column_to_exist", dry_run=True)

    assert 'Multiple expectations matched arguments. No expectations removed.' in str(exc.value)

    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "x"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "y"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "z"}
        )
    ]

    if PY2:
        assert sorted(
            my_df.remove_expectation("expect_column_to_exist", remove_multiple_matches=True, dry_run=True)) == \
        sorted(exp1)
    else:
        assert my_df.remove_expectation("expect_column_to_exist", remove_multiple_matches=True, dry_run=True) == \
            exp1

    with pytest.raises(ValueError) as exc:
        my_df.remove_expectation("expect_column_to_exist", "x", {
                                 "column": "y"}, dry_run=True)

    assert 'Conflicting column names in remove_expectation' in str(exc.value)

    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "x"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "x", "type_": "int"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_increasing",
            kwargs={"column": "x"}
        )
    ]

    if PY2:
        assert sorted(convert_test_obj_to_json_dict(my_df.remove_expectation(column="x", remove_multiple_matches=True,
                                                            dry_run=True))) == sorted(exp1)
    else:
        assert my_df.remove_expectation(column="x", remove_multiple_matches=True, dry_run=True) == exp1

    assert len(my_df._expectation_suite.expectations) == 8

    assert my_df.remove_expectation("expect_column_to_exist", "x") is None
    assert len(my_df._expectation_suite.expectations) == 7
    assert my_df.remove_expectation(column="x", remove_multiple_matches=True) is None
    assert len(my_df._expectation_suite.expectations) == 5

    my_df.remove_expectation(column="z", remove_multiple_matches=True)
    assert len(my_df._expectation_suite.expectations) == 2

    assert my_df.get_expectation_suite(discard_failed_expectations=False) == \
        ExpectationSuite(
            expectations=[
                ExpectationConfiguration(
                    expectation_type="expect_column_to_exist",
                    kwargs={"column": "y"}
                ),
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={"column": "y", "type_": "int"}
                )
            ],
            data_asset_name="default",
            expectation_suite_name="default",
            data_asset_type="Dataset",
            meta={
                "great_expectations.__version__": ge.__version__
            }
    )


def test_discard_failing_expectations():
    df = ge.dataset.PandasDataset({
        'A': [1, 2, 3, 4],
        'B': [5, 6, 7, 8],
        'C': ['a', 'b', 'c', 'd'],
        'D': ['e', 'f', 'g', 'h']
    }, profiler=ge.profile.ColumnsExistProfiler)

    # Put some simple expectations on the data frame
    df.expect_column_values_to_be_in_set("A", [1, 2, 3, 4])
    df.expect_column_values_to_be_in_set("B", [5, 6, 7, 8])
    df.expect_column_values_to_be_in_set("C", ['a', 'b', 'c', 'd'])
    df.expect_column_values_to_be_in_set("D", ['e', 'f', 'g', 'h'])

    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "B"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "C"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'A', 'value_set': [1, 2, 3, 4]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'B', 'value_set': [5, 6, 7, 8]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'C', 'value_set': ['a', 'b', 'c', 'd']}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'D', 'value_set': ['e', 'f', 'g', 'h']}
        )
    ]

    sub1 = df[:3]

    sub1.discard_failing_expectations()
    # PY2 sorting is allowed and order not guaranteed
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df[1:2]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df[:-1]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df[-1:]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df[['A', 'D']]
    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'A', 'value_set': [1, 2, 3, 4]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'D', 'value_set': ['e', 'f', 'g', 'h']}
        )
    ]
    sub1.discard_failing_expectations()
    if PY2:
       assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df[['A']]
    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'A', 'value_set': [1, 2, 3, 4]}
        ),
    ]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df.iloc[:3, 1:4]
    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "B"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "C"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "D"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'B', 'value_set': [5, 6, 7, 8]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'C', 'value_set': ['a', 'b', 'c', 'd']}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'D', 'value_set': ['e', 'f', 'g', 'h']}
        )
    ]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1

    sub1 = df.loc[0:, 'A':'B']
    exp1 = [
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "A"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "B"}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'A', 'value_set': [1, 2, 3, 4]}
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={'column': 'B', 'value_set': [5, 6, 7, 8]}
        ),
    ]
    sub1.discard_failing_expectations()
    if PY2:
        assert sorted(sub1.find_expectations()) == sorted(exp1)
    else:
        assert sub1.find_expectations() == exp1


def test_test_expectation_function():
    asset = ge.dataset.PandasDataset({
        'x': [1, 3, 5, 7, 9],
        'y': [1, 2, None, 7, 9],
    })
    asset_2 = ge.dataset.PandasDataset({
        'x': [1, 3, 5, 6, 9],
        'y': [1, 2, None, 6, 9],
    })

    def expect_dataframe_to_contain_7(self):
        return {
            "success": bool((self == 7).sum().sum() > 0)
        }

    assert asset.test_expectation_function(expect_dataframe_to_contain_7) == ExpectationValidationResult(success=True)
    assert asset_2.test_expectation_function(expect_dataframe_to_contain_7) == ExpectationValidationResult(
        success=False)
