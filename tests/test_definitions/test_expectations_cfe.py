import glob
import json
import os
import random
import string
from collections import OrderedDict

import pandas as pd
import pytest

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from tests.conftest import build_test_backends_list, build_test_backends_list_cfe
from tests.test_definitions.test_expectations import mssqlDialect as mssqlDialect
from tests.test_definitions.test_expectations import mysqlDialect as mysqlDialect
from tests.test_definitions.test_expectations import (
    postgresqlDialect as postgresqlDialect,
)
from tests.test_definitions.test_expectations import sqliteDialect as sqliteDialect
from tests.test_definitions.test_expectations import tmp_dir
from tests.test_utils import (
    candidate_test_is_on_temporary_notimplemented_list_cfe,
    evaluate_json_test,
    evaluate_json_test_cfe,
    get_test_batch,
)


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
        for c in build_test_backends_list_cfe(metafunc):
            for filename in test_configuration_files:
                file = open(filename)
                # Use OrderedDict so that python2 will use the correct order of columns in all cases
                test_configuration = json.load(file, object_pairs_hook=OrderedDict)

                for d in test_configuration["datasets"]:
                    datasets = []
                    if candidate_test_is_on_temporary_notimplemented_list_cfe(
                        c, test_configuration["expectation_type"]
                    ):
                        skip_expectation = True
                        schemas = batch = None
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
                                    get_test_batch(
                                        c,
                                        dataset["data"],
                                        dataset.get("schemas"),
                                        table_name=dataset.get("dataset_name"),
                                        sqlite_db_path=sqlite_db_path,
                                    )
                                )
                            batch = datasets[0]
                        else:
                            schemas = d["schemas"] if "schemas" in d else None
                            batch = get_test_batch(c, d["data"], schemas=schemas)

                    for test in d["tests"]:
                        generate_test = True
                        skip_test = False
                        if "only_for" in test:
                            # if we're not on the "only_for" list, then never even generate the test
                            generate_test = False
                            if not isinstance(test["only_for"], list):
                                raise ValueError("Invalid test specification.")

                            if batch and isinstance(
                                batch.execution_engine, SqlAlchemyExecutionEngine
                            ):
                                # Call out supported dialects
                                if "sqlalchemy" in test["only_for"]:
                                    generate_test = True
                                elif (
                                    "sqlite" in test["only_for"]
                                    and sqliteDialect is not None
                                    and isinstance(
                                        batch.execution_engine.engine.dialect,
                                        sqliteDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "postgresql" in test["only_for"]
                                    and postgresqlDialect is not None
                                    and isinstance(
                                        batch.execution_engine.engine.dialect,
                                        postgresqlDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mysql" in test["only_for"]
                                    and mysqlDialect is not None
                                    and isinstance(
                                        batch.execution_engine.engine.dialect,
                                        mysqlDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mssql" in test["only_for"]
                                    and mssqlDialect is not None
                                    and isinstance(
                                        batch.execution_engine.engine.dialect,
                                        mssqlDialect,
                                    )
                                ):
                                    generate_test = True
                            elif batch and isinstance(
                                batch.execution_engine, PandasExecutionEngine
                            ):
                                if "pandas" in test["only_for"]:
                                    generate_test = True
                                if (
                                    "pandas_022" in test["only_for"]
                                    or "pandas_023" in test["only_for"]
                                ) and int(pd.__version__.split(".")[1]) in [22, 23]:
                                    generate_test = True
                                if ("pandas>=24" in test["only_for"]) and int(
                                    pd.__version__.split(".")[1]
                                ) > 24:
                                    generate_test = True
                            elif batch and isinstance(
                                batch.execution_engine, SparkDFExecutionEngine
                            ):
                                if "spark" in test["only_for"]:
                                    generate_test = True

                        if not generate_test:
                            continue

                        if "suppress_test_for" in test and (
                            (
                                "sqlalchemy" in test["suppress_test_for"]
                                and batch
                                and isinstance(
                                    batch.execution_engine, SqlAlchemyExecutionEngine
                                )
                            )
                            or (
                                "sqlite" in test["suppress_test_for"]
                                and sqliteDialect is not None
                                and batch
                                and isinstance(
                                    batch.execution_engine, SqlAlchemyExecutionEngine
                                )
                                and isinstance(
                                    batch.execution_engine.engine.dialect, sqliteDialect
                                )
                            )
                            or (
                                "postgresql" in test["suppress_test_for"]
                                and postgresqlDialect is not None
                                and batch
                                and isinstance(
                                    batch.execution_engine, SqlAlchemyExecutionEngine
                                )
                                and isinstance(
                                    batch.execution_engine.engine.dialect,
                                    postgresqlDialect,
                                )
                            )
                            or (
                                "mysql" in test["suppress_test_for"]
                                and mysqlDialect is not None
                                and batch
                                and isinstance(
                                    batch.execution_engine, SqlAlchemyExecutionEngine
                                )
                                and isinstance(
                                    batch.execution_engine.engine.dialect, mssqlDialect
                                )
                            )
                            or (
                                "mssql" in test["suppress_test_for"]
                                and mssqlDialect is not None
                                and batch
                                and isinstance(
                                    batch.execution_engine, SqlAlchemyExecutionEngine
                                )
                                and isinstance(
                                    batch.execution_engine.engine.dialect, mssqlDialect
                                )
                            )
                            or (
                                "pandas" in test["suppress_test_for"]
                                and batch
                                and isinstance(
                                    batch.execution_engine, PandasExecutionEngine
                                )
                            )
                            or (
                                "spark" in test["suppress_test_for"]
                                and batch
                                and isinstance(
                                    batch.execution_engine, SparkDFExecutionEngine
                                )
                            )
                        ):
                            skip_test = True
                        # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
                        if (
                            "allow_cross_type_comparisons" in test["in"]
                            and batch
                            and isinstance(
                                batch.execution_engine, SqlAlchemyExecutionEngine
                            )
                        ):
                            skip_test = True

                        parametrized_tests.append(
                            {
                                "expectation_type": test_configuration[
                                    "expectation_type"
                                ],
                                "batch": batch,
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


def test_case_runner_cfe(test_case):
    if test_case["skip"]:
        pytest.skip()

    # Note: this should never be done in practice, but we are wiping expectations to reuse batches during testing.
    # test_case["batch"]._initialize_expectations()

    evaluate_json_test_cfe(
        test_case["batch"], test_case["expectation_type"], test_case["test"]
    )
