from __future__ import division

import pytest
import json
import datetime
import pandas as pd
import numpy as np
import great_expectations as ge

from .test_utils import assertDeepAlmostEqual

def duplicate_and_obfuscuate(df):

    df_a = df.copy()
    df_b = df.copy()

    df_a['group'] = 'a'
    df_b['group'] = 'b'

    for column in df_b.columns:
        if column == "group":
            continue

        if df_b[column].dtype in ['int', 'float']:
            df_b[column] += np.max(df_b[column])
            continue

        if df_b[column].dtype == 'object' and all([(type(elem) == str) or (elem is None) for elem in df_b[column]]):
            df_b[column] = df_b[column].astype(str) + '__obfuscate'
            continue

    return ge.from_pandas(pd.concat([df_a, df_b], ignore_index=True, sort=False))

def test_expectation_decorator_summary_mode():

    df = duplicate_and_obfuscuate(
        ge.dataset.PandasDataset({
        'x': [1, 2, 3, 4, 5, 6, 7, 7, None, None]
        })
    )

    df.set_default_expectation_argument("result_format", "COMPLETE")

    # print '&'*80
    # print json.dumps(df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY"), indent=2)

    exp_output = {
        "success": False,
        "result": {
            "element_count": 10,
            "missing_count": 2,
            "missing_percent": .2,
            "unexpected_count": 3,
            "partial_unexpected_counts": [
                {"value": 7.0,
                    "count": 2},
                {"value": 6.0,
                    "count": 1}
            ],
            "unexpected_percent": 0.3,
            "unexpected_percent_nonmissing": 0.375,
            "partial_unexpected_list": [6.0, 7.0, 7.0],
            "partial_unexpected_index_list": [5, 6, 7],
        }
    }
    assert df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY", condition="group=='a'")\
        == exp_output

    assert df.expect_column_values_to_be_between('x', min_value=1, max_value=5, result_format="SUMMARY")\
        != exp_output

    exp_output = {
        'success': True,
        'result': {
            'observed_value': 4.375,
            'element_count': 10,
            'missing_count': 2,
            'missing_percent': .2
        },
    }

    assert df.expect_column_mean_to_be_between("x", 3, 7, result_format="SUMMARY", condition="group=='a'")\
        == exp_output

    assert df.expect_column_mean_to_be_between("x", 3, 7, result_format="SUMMARY")\
        != exp_output


def test_positional_arguments():

    df = duplicate_and_obfuscuate(ge.dataset.PandasDataset({
        'x': [1, 3, 5, 7, 9],
        'y': [2, 4, 6, 8, 10],
        'z': [None, 'a', 'b', 'c', 'abc']
    }))

    df.set_default_expectation_argument('result_format', 'COMPLETE')

    exp_output = {'success': True, 'result': {'observed_value': 5, 'element_count': 5,
                                              'missing_count': 0,
                                              'missing_percent': 0.0}}

    assert df.expect_column_mean_to_be_between('x', 4, 6, condition='group=="a"') == exp_output
    assert df.expect_column_mean_to_be_between('x', 4, 6) != exp_output

    out = df.expect_column_values_to_be_between('y', 1, 6, condition='group=="a"')
    t = {'out': {'success': False, 'unexpected_list': [
        8, 10], 'unexpected_index_list': [3, 4]}}
    if 'out' in t:
        assert t['out']['success'] == out['success']
        if 'unexpected_index_list' in t['out']:
            assert t['out']['unexpected_index_list'] == out['result']['unexpected_index_list']
        if 'unexpected_list' in t['out']:
            assert t['out']['unexpected_list'] == out['result']['unexpected_list']

    out = df.expect_column_values_to_be_between('y', 1, 6, mostly=.5, condition='group=="a"')
    t = {'out': {'success': True, 'unexpected_list': [
        8, 10], 'unexpected_index_list': [3, 4]}}
    if 'out' in t:
        assert t['out']['success'] == out['success']
        if 'unexpected_index_list' in t['out']:
            assert t['out']['unexpected_index_list'] == out['result']['unexpected_index_list']
        if 'unexpected_list' in t['out']:
            assert t['out']['unexpected_list'] == out['result']['unexpected_list']

    out = df.expect_column_values_to_be_in_set('z', ['a', 'b', 'c'], condition='group=="a"')
    t = {'out': {'success': False, 'unexpected_list': [
        'abc'], 'unexpected_index_list': [4]}}
    if 'out' in t:
        assert t['out']['success'] == out['success']
        if 'unexpected_index_list' in t['out']:
            assert t['out']['unexpected_index_list'] == out['result']['unexpected_index_list']
        if 'unexpected_list' in t['out']:
            assert t['out']['unexpected_list'] == out['result']['unexpected_list']

    out = df.expect_column_values_to_be_in_set('z', ['a', 'b', 'c'], mostly=.5, condition='group=="a"')
    t = {'out': {'success': True, 'unexpected_list': [
        'abc'], 'unexpected_index_list': [4]}}
    if 'out' in t:
        assert t['out']['success'] == out['success']
        if 'unexpected_index_list' in t['out']:
            assert t['out']['unexpected_index_list'] == out['result']['unexpected_index_list']
        if 'unexpected_list' in t['out']:
            assert t['out']['unexpected_list'] == out['result']['unexpected_list']


def test_result_format_argument_in_decorators():

    df = duplicate_and_obfuscuate(ge.dataset.PandasDataset({
        'x': [1, 3, 5, 7, 9],
        'y': [2, 4, 6, 8, 10],
        'z': [None, 'a', 'b', 'c', 'abc']
    }))
    df.set_default_expectation_argument('result_format', 'COMPLETE')

    # Test explicit Nones in result_format

    exp_output = {'success': True, 'result': {'observed_value': 5, 'element_count': 5,
                                                'missing_count': 0,
                                                'missing_percent': 0.0
                                                }}
    assert df.expect_column_mean_to_be_between('x', 4, 6, result_format=None, condition="group=='a'")\
        == exp_output

    assert df.expect_column_mean_to_be_between('x', 4, 6, result_format=None)\
        != exp_output

    exp_output = {'result': {'element_count': 5,
                                'missing_count': 0,
                                'missing_percent': 0.0,
                                'partial_unexpected_counts': [{'count': 1, 'value': 8},
                                                            {'count': 1, 'value': 10}],
                                'partial_unexpected_index_list': [3, 4],
                                'partial_unexpected_list': [8, 10],
                                'unexpected_count': 2,
                                'unexpected_index_list': [3, 4],
                                'unexpected_list': [8, 10],
                                'unexpected_percent': 0.4,
                                'unexpected_percent_nonmissing': 0.4},
                    'success': False}

    assert df.expect_column_values_to_be_between('y', 1, 6, result_format=None, condition="group=='a'")\
        == exp_output

    assert df.expect_column_values_to_be_between('y', 1, 6, result_format=None, condition="group=='a'")\
        != df.expect_column_values_to_be_between('y', 1, 6, result_format=None)\

    # Test unknown output format
    with pytest.raises(ValueError):
        df.expect_column_values_to_be_between('y', 1, 6, result_format="QUACK", condition="group=='a'")

    with pytest.raises(ValueError):
        df.expect_column_mean_to_be_between('x', 4, 6, result_format="QUACK", condition="group=='a'")

def test_ge_pandas_subsetting():
    df = duplicate_and_obfuscuate(ge.dataset.PandasDataset({
        'A': [1, 2, 3, 4],
        'B': [5, 6, 7, 8],
        'C': ['a', 'b', 'c', 'd'],
        'D': ['e', 'f', 'g', 'h']
    }))

    # Put some simple expectations on the data frame
    df.expect_column_values_to_be_in_set("A", [1, 2, 3, 4], condition="group=='a'")
    df.expect_column_values_to_be_in_set("B", [5, 6, 7, 8], condition="group=='a'")
    df.expect_column_values_to_be_in_set("C", ['a', 'b', 'c', 'd'], condition="group=='a'")
    df.expect_column_values_to_be_in_set("D", ['e', 'f', 'g', 'h'], condition="group=='a'")

    # The subsetted data frame should:
    #
    #   1. Be a ge.dataset.PandaDataSet
    #   2. Inherit ALL the expectations of the parent data frame

    exp1 = df.find_expectations()

    sub1 = df[['A', 'D']]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df[['A']]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df[:3]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df[1:2]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df[:-1]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df[-1:]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df.iloc[:3, 1:4]
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1

    sub1 = df.loc[0:, 'A':'B']
    assert isinstance(sub1, ge.dataset.PandasDataset)
    assert sub1.find_expectations() == exp1
