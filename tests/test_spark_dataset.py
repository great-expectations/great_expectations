import great_expectations as ge
import pytest

context = ge.get_data_context('SparkCSV', './tests/test_sets')
titanic_dataset = context.get_dataset('Titanic.csv')


def test_expect_column_to_exist():
    # simple positive
    result = titanic_dataset.expect_column_to_exist('Name')
    assert result['success']

    # simple negative
    result = titanic_dataset.expect_column_to_exist('fake_column')
    assert not result['success']


def test_expect_column_values_to_be_in_set():
    # simple positive
    result = titanic_dataset.expect_column_values_to_be_in_set('Sex', ['male', 'female'])
    assert result['success']

    # simple negative
    result = titanic_dataset.expect_column_values_to_be_in_set('Sex', ['Male', 'Female'])
    assert not result['success']

    # check partial_unexpected_list
    result = titanic_dataset.expect_column_values_to_be_in_set('PClass', ['1st', '2nd', '3rd'])
    assert not result['success']
    assert result['result']['partial_unexpected_list'] == ['*']

    # positive using mostly param
    result = titanic_dataset.expect_column_values_to_be_in_set('PClass', ['1st', '2nd', '3rd'], mostly=0.99)
    assert result['success']


def test_expect_table_row_count_to_be_between():
    # simple posivive
    result = titanic_dataset.expect_table_row_count_to_be_between(1300, 1400)
    assert result['success']

    # simple negative
    result = titanic_dataset.expect_table_row_count_to_be_between(1400, 1500)
    assert not result['success']

    # positive using only max_value
    result = titanic_dataset.expect_table_row_count_to_be_between(max_value=1400)
    assert result['success']

    # positive using only min_value
    result = titanic_dataset.expect_table_row_count_to_be_between(min_value=1300)
    assert result['success']

    # negative using only max_value
    result = titanic_dataset.expect_table_row_count_to_be_between(max_value=20)
    assert not result['success']

    # negative using only min_value
    result = titanic_dataset.expect_table_row_count_to_be_between(min_value=2000)
    assert not result['success']

    # max_value should be int
    with pytest.raises(ValueError):
        titanic_dataset.expect_table_row_count_to_be_between(1300, 1400.5)

    # min_value should be int
    with pytest.raises(ValueError):
        titanic_dataset.expect_table_row_count_to_be_between(1300.5, 1400)

    # have to specify either min_value or max_value
    with pytest.raises(Exception):
        titanic_dataset.expect_table_row_count_to_be_between()


def test_expect_table_row_count_to_equal():
    # simple positive
    result = titanic_dataset.expect_table_row_count_to_equal(1313)
    assert result['success']

    # simple negative
    result = titanic_dataset.expect_table_row_count_to_equal(1000)
    assert not result['success']

    # value should be integer
    with pytest.raises(ValueError):
        titanic_dataset.expect_table_row_count_to_equal(1313.1)

    # value can be float that represents an integer
    result = titanic_dataset.expect_table_row_count_to_equal(1313.0)
    assert result['success']


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
