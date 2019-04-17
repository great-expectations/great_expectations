import great_expectations as ge
import pytest

context = ge.get_data_context('SparkCSV', './tests/test_sets')
titanic_dataset = context.get_dataset('Titanic.csv')
strf_dataset = context.get_dataset('strf_test.csv')


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


# def test_expect_table_row_count_to_be_between():
#     # simple posivive
#     result = titanic_dataset.expect_table_row_count_to_be_between(1300, 1400)
#     assert result['success']

#     # simple negative
#     result = titanic_dataset.expect_table_row_count_to_be_between(1400, 1500)
#     assert not result['success']

#     # positive using only max_value
#     result = titanic_dataset.expect_table_row_count_to_be_between(max_value=1400)
#     assert result['success']

#     # positive using only min_value
#     result = titanic_dataset.expect_table_row_count_to_be_between(min_value=1300)
#     assert result['success']

#     # negative using only max_value
#     result = titanic_dataset.expect_table_row_count_to_be_between(max_value=20)
#     assert not result['success']

#     # negative using only min_value
#     result = titanic_dataset.expect_table_row_count_to_be_between(min_value=2000)
#     assert not result['success']

#     # max_value should be int
#     with pytest.raises(ValueError):
#         titanic_dataset.expect_table_row_count_to_be_between(1300, 1400.5)

#     # min_value should be int
#     with pytest.raises(ValueError):
#         titanic_dataset.expect_table_row_count_to_be_between(1300.5, 1400)

#     # have to specify either min_value or max_value
#     with pytest.raises(Exception):
#         titanic_dataset.expect_table_row_count_to_be_between()


# def test_expect_table_row_count_to_equal():
#     # simple positive
#     result = titanic_dataset.expect_table_row_count_to_equal(1313)
#     assert result['success']

#     # simple negative
#     result = titanic_dataset.expect_table_row_count_to_equal(1000)
#     assert not result['success']

#     # value should be integer
#     with pytest.raises(ValueError):
#         titanic_dataset.expect_table_row_count_to_equal(1313.1)

#     # value can be float that represents an integer
#     result = titanic_dataset.expect_table_row_count_to_equal(1313.0)
#     assert result['success']


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


def test_expect_table_columns_to_match_ordered_list():
    result = titanic_dataset.expect_table_columns_to_match_ordered_list(
        ["_c0", "Name", "PClass", "Age", "Sex", "Survived", "SexCode"]
    )
    assert result['success']

    result = titanic_dataset.expect_table_columns_to_match_ordered_list(
        ["_c0", "Name", "PClass", "Age", "Sex"]
    )
    assert not result['success']
    assert result['details']['mismatched'] == [
        {'Expected': None, 'Expected Column Position': 5, 'Found': 'Survived'},
        {'Expected': None, 'Expected Column Position': 6, 'Found': 'SexCode'}
    ]

    result = titanic_dataset.expect_table_columns_to_match_ordered_list(
        ["_c0", "Name", "PClass", "Age", "Sex", "Survived", "SexCode", "fake_column"]
    )
    assert not result['success']
    assert result['details']['mismatched'] == [
        {'Expected': "fake_column", 'Expected Column Position': 7, 'Found': None},
    ]


def test_expect_column_values_to_not_be_in_set():
    result = titanic_dataset.expect_column_values_to_not_be_in_set('Age', [-1, 0])
    assert result['success']

    result = titanic_dataset.expect_column_values_to_not_be_in_set('Survived', ['1', '0'])
    assert not result['success']

    result = titanic_dataset.expect_column_values_to_not_be_in_set(
        'Name', ['Crosby, Captain Edward Gifford'], mostly=0.99
    )
    assert result['success']
    assert result['result']['partial_unexpected_list'] == ['Crosby, Captain Edward Gifford']


def test_expect_column_value_lengths_to_equal():
    # TODO check that TypeError is raised when trying to run this expectation on an int or float type column

    result = titanic_dataset.expect_column_value_lengths_to_equal('Survived', 1)
    assert result['success']

    result = titanic_dataset.expect_column_value_lengths_to_equal('Name', 10)
    assert not result['success']

    result = titanic_dataset.expect_column_value_lengths_to_equal('PClass', 3)
    assert not result['success']

    result = titanic_dataset.expect_column_value_lengths_to_equal('PClass', 3, mostly=0.99)
    assert result['success']


def test_expect_column_values_to_match_strftime_format():
    result = strf_dataset.expect_column_values_to_match_strftime_format('date', '%Y-%m-%d')
    assert result['success']

    result = strf_dataset.expect_column_values_to_match_strftime_format('date', '%Y%m%d')
    assert not result['success']

    result = titanic_dataset.expect_column_values_to_match_strftime_format('Age', '%Y-%m-%d')
    assert not result['success']


def test_expect_column_values_to_be_null():
    result = titanic_dataset.expect_column_values_to_be_null('Name')
    assert not result['success']


def test_expect_column_values_to_not_be_null():
    result = titanic_dataset.expect_column_values_to_not_be_null('Name')
    assert result['success']
