import pytest

import os
import json
import glob
import warnings

from ..test_utils import get_dataset, candidate_test_is_on_temporary_notimplemented_list, evaluate_json_test

contexts = ['PandasDataset', 'SqlAlchemyDataset']


def pytest_generate_tests(metafunc):

    # Load all the JSON files in the directory
    dir_path = os.path.dirname(os.path.realpath(__file__))
    expectation_dirs = [dir_ for dir_ in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, dir_))]
    
    parametrized_tests = []
    ids = []
    
    for expectation_category in expectation_dirs:
    
        test_configuration_files = glob.glob(dir_path+'/' + expectation_category + '/*.json')
        for c in contexts:
            for filename in test_configuration_files:
                file = open(filename)
                test_configuration = json.load(file)

                if candidate_test_is_on_temporary_notimplemented_list(c, test_configuration["expectation_type"]):
                    warnings.warn("Skipping generation of tests for expectation " + test_configuration["expectation_type"] +
                                " and context " + c)
                else:
                    for d in test_configuration['datasets']:
                        schemas = d["schemas"] if "schemas" in d else None
                        my_dataset = get_dataset(c, d["data"], schemas=schemas)

                        for test in d["tests"]:
                            parametrized_tests.append({
                                "expectation_type": test_configuration["expectation_type"],
                                "dataset": my_dataset,
                                "test": test,
                            })

                            ids.append(expectation_category + "/" +
                                c+":"+test_configuration["expectation_type"]+":"+test["title"])

    metafunc.parametrize(
        "test_case",
        parametrized_tests,
        ids=ids
    )


def test_case_runner(test_case):
    # Note: this should never be done in practice, but we are wiping expectations to reuse datasets during testing.
    test_case["dataset"]._initialize_expectations()

    evaluate_json_test(
        test_case["dataset"],
        test_case["expectation_type"],
        test_case["test"]
    )
