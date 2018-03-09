import pytest

from tests.util import get_dataset, evaluate_json_test_alternate, evaluate_test, make_test_assertions

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']

@pytest.fixture(scope="module",
                params=dataset_implementations)
def dataset_type(request):
    return request.param


@pytest.fixture(scope="class")
def test_data(dataset_type):
    data = {
        "x": [1, 2, 4],
        "y": [1, 2, 5],
        "z": ["hello", "jello", "mello"]
    }
    return get_dataset(dataset_type, data)

def test_expect_column_values_to_be_in_set_positive_case(test_data):

    test = {
        "title": "Basic positive test case",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {
            "column": "x",
            "values_set": [1, 2, 4]
        },
        "out": {
            "success": True
        }
    }

    evaluate_json_test_alternate(test_data, test)


def test_expect_column_values_to_be_in_set_negative_case(test_data):

    test = {
        "title": "Negative test case",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {
            "column": "x",
            "values_set": [2, 4]
        },
        "out": {
            "success": False,
            "unexpected_index_list": [0],
            "unexpected_list": [1]
        }
    }

    evaluate_json_test_alternate(test_data, test)


def test_expect_column_values_to_be_in_set_empty_values_set(test_data):

    test = {
        "title": "Empty values_set",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {
            "column": "x",
            "values_set": []
        },
        "out": {
            "success": False,
            "unexpected_index_list": [0, 1, 2],
            "unexpected_list": [1, 2, 4]
        }
    }

    evaluate_json_test_alternate(test_data, test)


def test_expect_column_values_to_be_in_set_basic_strings(test_data):
    test = {
        "title": "Basic strings set",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {
            "column": "z",
            "values_set": ["hello", "jello", "mello"]
        },
        "out": {
            "success": True
        }
    }

    evaluate_json_test_alternate(test_data, test)


def test_expect_column_values_to_be_in_set_missing_parameters(test_data):
    test = {
        "title": "Missing required parameters",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {},
        "exception": "TypeError"
    }

    evaluate_json_test_alternate(test_data, test)