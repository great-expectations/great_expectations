import pytest

from tests.util import get_dataset, evaluate_json_test_alternate

dataset_implementations = ['PandasDataSet', 'SqlAlchemyDataSet']
test_cases = [
    {
        "title": "Basic positive test case",
        "expectation_type": "expect_column_values_to_be_in_set",
        "in": {
            "column": "x",
            "values_set": [1, 2, 4]
        },
        "out": {
            "success": True
        }
    },
    {
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
    },
    {
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
    },
    {
        "title": "Basic strings set",
        "expectation_type": "expect_column_values_to_be_in_set",

        "in": {
            "column": "z",
            "values_set": ["hello", "jello", "mello"]
        },
        "out": {
            "success": True
        }
    },
    {
        "title": "Missing required parameters",
        "expectation_type": "expect_column_values_to_be_in_set",

        "in": {},
        "exception": "TypeError"
    }
]

test_case_ids = [test['title'] for test in test_cases]


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


@pytest.fixture(scope="class",
                params=test_cases,
                ids=test_case_ids)
def test_case(request):
    return request.param


def test_expect_column_values_to_be_in_set(test_data, test_case):
    evaluate_json_test_alternate(test_data, test_case)
