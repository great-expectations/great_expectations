import pytest

import os
import json
import glob
import logging

from sqlalchemy.dialects.sqlite import dialect as sqliteDialect
from sqlalchemy.dialects.postgresql import dialect as postgresqlDialect


from great_expectations.dataset import SqlAlchemyDataset, PandasDataset
from ..test_utils import get_dataset, candidate_test_is_on_temporary_notimplemented_list, evaluate_json_test

contexts = ['PandasDataset', 'SqlAlchemyDataset']
logger = logging.getLogger(__name__)

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
                    logger.debug("Skipping generation of tests for expectation " + test_configuration["expectation_type"] +
                                " and context " + c)
                else:
                    for d in test_configuration['datasets']:
                        schemas = d["schemas"] if "schemas" in d else None
                        data_asset = get_dataset(c, d["data"], schemas=schemas)

                        for test in d["tests"]:
                               # Pass the test if we are in a test condition that is a known exception

                            # Don't generate tests based on certain configurations

                            # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
                            if 'allow_cross_type_comparisons' in test['in'] and isinstance(data_asset, SqlAlchemyDataset):
                                continue

                            if 'suppress_test_for' in test:
                                # Optionally suppress the test for specified DataAsset types
                                if 'SQLAlchemy' in test['suppress_test_for'] and isinstance(data_asset, SqlAlchemyDataset):
                                    continue
                                if 'sqlite' in test['suppress_test_for'] and isinstance(data_asset, SqlAlchemyDataset) and isinstance(data_asset.engine.dialect, sqliteDialect):
                                    continue
                                if 'postgresql' in test['suppress_test_for'] and isinstance(data_asset, SqlAlchemyDataset) and isinstance(data_asset.engine.dialect, postgresqlDialect):
                                    continue
                                if 'Pandas' in test['suppress_test_for'] and isinstance(data_asset, PandasDataset):
                                    continue

                            parametrized_tests.append({
                                "expectation_type": test_configuration["expectation_type"],
                                "dataset": data_asset,
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
