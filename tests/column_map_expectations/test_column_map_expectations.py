###
###
#
# This file should not be modified. To adjust test cases, edit the related json file.
#
###
###


import pytest

import os
import json
import glob

from tests.util import get_dataset, evaluate_json_test

dir_path = os.path.dirname(os.path.realpath(__file__))
test_configuration_files = glob.glob(dir_path+'/*.json')
# print test_configuration_files

# test_configurations_dict = {}
# for filename in test_configuration_files:
#   file = open(filename)
#   test_configurations_dict['filename'] = json.load(file)

# dataset = test_configurations['dataset']
# expectation_name = test_configurations['expectation_type']
# test_cases = test_configurations['tests']
# test_case_ids = [test['title'] for test in test_cases]

# @pytest.fixture(scope="module")
contexts = ['PandasDataSet', 'SqlAlchemyDataSet']

def pytest_generate_tests(metafunc):
    parametrized_tests = []
    ids = []
    for c in contexts:
        for filename in test_configuration_files:
            file = open(filename)
            test_configuration = json.load(file)
            # print test_configuration["expectation_type"]
            for d in test_configuration['datasets']:            
                my_dataset = get_dataset(c, d["data"])

                for test in d["tests"]:
                    #FIXME:
                    # my_data.remove_all_expectations()
                    parametrized_tests.append({
                        "expectation_type": test_configuration["expectation_type"],
                        "dataset": my_dataset,
                        "test": test,
                    })
                    if not "title" in test:
                        test["title"] = "BLANK"
                    ids.append(c+":"+test_configuration["expectation_type"]+":"+test["title"])

    print len(parametrized_tests)

    metafunc.parametrize(
        "test_case",
        parametrized_tests,
        ids=ids
    )

def test_case_runner(test_case):
    # print test_case["test"], test_case["expectation_type"]
    # print test_case["dataset"].get_expectations_config()
    evaluate_json_test(
        test_case["dataset"],
        test_case["expectation_type"],
        test_case["test"]
    )


