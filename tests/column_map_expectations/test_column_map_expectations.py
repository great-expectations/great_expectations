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

contexts = ['PandasDataSet', 'SqlAlchemyDataSet']

def pytest_generate_tests(metafunc):

    dir_path = os.path.dirname(os.path.realpath(__file__))
    test_configuration_files = glob.glob(dir_path+'/*.json')

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
                    parametrized_tests.append({
                        "expectation_type": test_configuration["expectation_type"],
                        "dataset": my_dataset,
                        "test": test,
                    })
                    #FIXME: title should be required.
                    #If it's not present, then what?
                    if not "title" in test:
                        test["title"] = "BLANK"
                    ids.append(c+":"+test_configuration["expectation_type"]+":"+test["title"])

    metafunc.parametrize(
        "test_case",
        parametrized_tests,
        ids=ids
    )

def test_case_runner(test_case):
    #FIXME:
    # test_case["dataset"].remove_all_expectations()
    evaluate_json_test(
        test_case["dataset"],
        test_case["expectation_type"],
        test_case["test"]
    )


