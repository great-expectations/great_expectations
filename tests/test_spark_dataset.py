import great_expectations as ge
import pytest

context = ge.get_data_context('SparkCSV', './tests/test_sets')
titanic_dataset = context.get_dataset('Titanic.csv', header=True)
strf_dataset = context.get_dataset('strf_test.csv', header=True)


def test_expect_column_values_to_be_unique():
    result = titanic_dataset.expect_column_values_to_be_unique('_c0')
    assert result['success']

    result = titanic_dataset.expect_column_values_to_be_unique('Age')
    assert not result['success']

    result = titanic_dataset.expect_column_values_to_be_unique('Name')
    assert not result['success']
    assert 'Kelly, Mr James' in result['result']['partial_unexpected_list']

    result = titanic_dataset.expect_column_values_to_be_unique('Name', mostly=0.95)
    assert result['success']


def test_expect_column_values_to_match_strftime_format():
    result = strf_dataset.expect_column_values_to_match_strftime_format('date', '%Y-%m-%d')
    assert result['success']

    result = strf_dataset.expect_column_values_to_match_strftime_format('date', '%Y%m%d')
    assert not result['success']

    result = titanic_dataset.expect_column_values_to_match_strftime_format('Age', '%Y-%m-%d')
    assert not result['success']
