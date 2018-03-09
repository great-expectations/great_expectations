import pytest
import json

from tests.util import get_dataset, evaluate_json_test_alternate

file = open('./expect_column_values_to_be_in_set.json')
test_configurations = json.load(file)

dataset = test_configurations['dataset']
test_cases = test_configurations['tests']
test_case_ids = [test['title'] for test in test_cases]


@pytest.fixture(scope="module")
def test_data(dataset_type):
    return get_dataset(dataset_type, dataset)


@pytest.fixture(scope="module",
                params=test_cases,
                ids=test_case_ids)
def test_case(request):
    return request.param


def test_expect_column_values_to_be_in_set(test_data, test_case):
    evaluate_json_test_alternate(test_data, "expect_column_values_to_be_in_set", test_case)
