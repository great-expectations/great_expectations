import glob
import json
import logging
import os
import random
import string
import tempfile
from collections import OrderedDict

import pandas as pd
import pytest

from great_expectations.dataset import PandasDataset, SparkDFDataset, SqlAlchemyDataset
from great_expectations.self_check.util import (
    BigQueryDialect,
    candidate_test_is_on_temporary_notimplemented_list,
    evaluate_json_test,
    get_dataset,
    mssqlDialect,
    mysqlDialect,
    postgresqlDialect,
    sqliteDialect,
)
from tests.conftest import build_test_backends_list

logger = logging.getLogger(__name__)
tmp_dir = str(tempfile.mkdtemp())


def pytest_generate_tests(metafunc):
    # Load all the JSON files in the directory
    dir_path = os.path.dirname(os.path.realpath(__file__))
    expectation_dirs = [
        dir_
        for dir_ in os.listdir(dir_path)
        if os.path.isdir(os.path.join(dir_path, dir_))
    ]

    parametrized_tests = []
    ids = []
    for expectation_category in expectation_dirs:

        test_configuration_files = glob.glob(
            dir_path + "/" + expectation_category + "/*.json"
        )
        backends = build_test_backends_list(metafunc)
        for c in backends:
            for filename in test_configuration_files:
                file = open(filename)
                # Use OrderedDict so that python2 will use the correct order of columns in all cases
                test_configuration = json.load(file, object_pairs_hook=OrderedDict)

                for d in test_configuration["datasets"]:
                    datasets = []
                    # optional only_for and suppress_test flag at the datasets-level that can prevent data being
                    # added to incompatible backends. Currently only used by expect_column_values_to_be_unique
                    only_for = d.get("only_for")
                    if only_for and not isinstance(only_for, list):
                        # coerce into list if passed in as string
                        only_for = [only_for]
                    suppress_test_for = d.get("suppress_test_for")
                    if suppress_test_for and not isinstance(suppress_test_for, list):
                        # coerce into list if passed in as string
                        suppress_test_for = [suppress_test_for]
                    if candidate_test_is_on_temporary_notimplemented_list(
                        c, test_configuration["expectation_type"]
                    ):
                        skip_expectation = True
                        schemas = data_asset = None
                    elif suppress_test_for and c in suppress_test_for:
                        continue
                    elif only_for and c not in only_for:
                        continue
                    else:
                        skip_expectation = False
                        if isinstance(d["data"], list):
                            sqlite_db_path = os.path.abspath(
                                os.path.join(
                                    tmp_dir,
                                    "sqlite_db"
                                    + "".join(
                                        [
                                            random.choice(
                                                string.ascii_letters + string.digits
                                            )
                                            for _ in range(8)
                                        ]
                                    )
                                    + ".db",
                                )
                            )
                            for dataset in d["data"]:
                                datasets.append(
                                    get_dataset(
                                        c,
                                        dataset["data"],
                                        dataset.get("schemas"),
                                        table_name=dataset.get("dataset_name"),
                                        sqlite_db_path=sqlite_db_path,
                                    )
                                )
                            data_asset = datasets[0]
                        else:
                            schemas = d["schemas"] if "schemas" in d else None
                            data_asset = get_dataset(c, d["data"], schemas=schemas)

                    for test in d["tests"]:
                        generate_test = True
                        skip_test = False
                        only_for = test.get("only_for")
                        if only_for:
                            # if we're not on the "only_for" list, then never even generate the test
                            generate_test = False
                            if not isinstance(only_for, list):
                                # coerce into list if passed in as string
                                only_for = [only_for]

                            if isinstance(data_asset, SqlAlchemyDataset):
                                # Call out supported dialects
                                if "sqlalchemy" in only_for:
                                    generate_test = True
                                elif (
                                    "sqlite" in only_for
                                    and sqliteDialect is not None
                                    and isinstance(
                                        data_asset.engine.dialect, sqliteDialect
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "postgresql" in only_for
                                    and postgresqlDialect is not None
                                    and isinstance(
                                        data_asset.engine.dialect, postgresqlDialect
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mysql" in only_for
                                    and mysqlDialect is not None
                                    and isinstance(
                                        data_asset.engine.dialect, mysqlDialect
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mssql" in only_for
                                    and mssqlDialect is not None
                                    and isinstance(
                                        data_asset.engine.dialect, mssqlDialect
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "bigquery" in only_for
                                    and BigQueryDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and hasattr(data_asset.engine.dialect, "name")
                                    and data_asset.engine.dialect.name.lower()
                                    == "bigquery"
                                ):
                                    generate_test = True
                            elif isinstance(data_asset, PandasDataset):
                                if "pandas" in only_for:
                                    generate_test = True
                                if (
                                    "pandas_022" in only_for or "pandas_023" in only_for
                                ) and int(pd.__version__.split(".")[1]) in [22, 23]:
                                    generate_test = True
                                if ("pandas>=24" in only_for) and int(
                                    pd.__version__.split(".")[1]
                                ) > 24:
                                    generate_test = True
                            elif isinstance(data_asset, SparkDFDataset):
                                if "spark" in only_for:
                                    generate_test = True

                        if not generate_test:
                            continue

                        suppress_test_for = test.get("suppress_test_for")
                        if suppress_test_for:
                            if not isinstance(suppress_test_for, list):
                                # coerce into list if passed in as string
                                suppress_test_for = [suppress_test_for]

                            if (
                                "sqlalchemy" in suppress_test_for
                                and isinstance(data_asset, SqlAlchemyDataset)
                                or (
                                    "sqlite" in suppress_test_for
                                    and sqliteDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and isinstance(
                                        data_asset.engine.dialect, sqliteDialect
                                    )
                                )
                                or (
                                    "postgresql" in suppress_test_for
                                    and postgresqlDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and isinstance(
                                        data_asset.engine.dialect, postgresqlDialect
                                    )
                                )
                                or (
                                    "mysql" in suppress_test_for
                                    and mysqlDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and isinstance(
                                        data_asset.engine.dialect, mysqlDialect
                                    )
                                )
                                or (
                                    "mssql" in suppress_test_for
                                    and mssqlDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and isinstance(
                                        data_asset.engine.dialect, mssqlDialect
                                    )
                                )
                                or (
                                    "bigquery" in suppress_test_for
                                    and BigQueryDialect is not None
                                    and isinstance(data_asset, SqlAlchemyDataset)
                                    and hasattr(data_asset.engine.dialect, "name")
                                    and data_asset.engine.dialect.name.lower()
                                    == "bigquery"
                                )
                                or (
                                    "pandas" in suppress_test_for
                                    and isinstance(data_asset, PandasDataset)
                                )
                                or (
                                    "spark" in suppress_test_for
                                    and isinstance(data_asset, SparkDFDataset)
                                )
                            ):
                                skip_test = True
                        # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
                        if "allow_cross_type_comparisons" in test["in"] and isinstance(
                            data_asset, SqlAlchemyDataset
                        ):
                            skip_test = True

                        parametrized_tests.append(
                            {
                                "expectation_type": test_configuration[
                                    "expectation_type"
                                ],
                                "dataset": data_asset,
                                "test": test,
                                "skip": skip_expectation or skip_test,
                            }
                        )

                        ids.append(
                            c
                            + "/"
                            + expectation_category
                            + "/"
                            + test_configuration["expectation_type"]
                            + ":"
                            + test["title"]
                        )

    metafunc.parametrize("test_case", parametrized_tests, ids=ids)


@pytest.mark.order(index=1)
def test_case_runner(test_case):
    if test_case["skip"]:
        pytest.skip()

    # Note: this should never be done in practice, but we are wiping expectations to reuse datasets during testing.
    test_case["dataset"]._initialize_expectations()

    evaluate_json_test(
        test_case["dataset"], test_case["expectation_type"], test_case["test"]
    )
