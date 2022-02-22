import glob
import json
import os
import random
import string

import pandas as pd
import pytest

from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.self_check.util import (
    BigQueryDialect,
    candidate_test_is_on_temporary_notimplemented_list_cfe,
    evaluate_json_test_cfe,
    get_test_validator_with_data,
    mssqlDialect,
    mysqlDialect,
    postgresqlDialect,
    sqliteDialect,
)
from tests.conftest import build_test_backends_list_cfe
from tests.test_definitions.test_expectations import tmp_dir


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
    backends = build_test_backends_list_cfe(metafunc)
    validator_with_data = None
    for expectation_category in expectation_dirs:

        test_configuration_files = glob.glob(
            dir_path + "/" + expectation_category + "/*.json"
        )
        for c in backends:
            for filename in test_configuration_files:
                file = open(filename)
                test_configuration = json.load(file)

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
                    if candidate_test_is_on_temporary_notimplemented_list_cfe(
                        c, test_configuration["expectation_type"]
                    ):
                        skip_expectation = True
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
                                    get_test_validator_with_data(
                                        c,
                                        dataset["data"],
                                        dataset.get("schemas"),
                                        table_name=dataset.get("dataset_name"),
                                        sqlite_db_path=sqlite_db_path,
                                    )
                                )
                            validator_with_data = datasets[0]
                        else:
                            schemas = d["schemas"] if "schemas" in d else None
                            validator_with_data = get_test_validator_with_data(
                                c, d["data"], schemas=schemas
                            )

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

                            if validator_with_data and isinstance(
                                validator_with_data.execution_engine.active_batch_data,
                                SqlAlchemyBatchData,
                            ):
                                # Call out supported dialects
                                if "sqlalchemy" in only_for:
                                    generate_test = True
                                elif (
                                    "sqlite" in only_for
                                    and sqliteDialect is not None
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        sqliteDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "postgresql" in only_for
                                    and postgresqlDialect is not None
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        postgresqlDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mysql" in only_for
                                    and mysqlDialect is not None
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        mysqlDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "mssql" in only_for
                                    and mssqlDialect is not None
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        mssqlDialect,
                                    )
                                ):
                                    generate_test = True
                                elif (
                                    "bigquery" in only_for
                                    and BigQueryDialect is not None
                                    and hasattr(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        "name",
                                    )
                                    and validator_with_data.execution_engine.active_batch_data.sql_engine_dialect.name
                                    == "bigquery"
                                ):
                                    generate_test = True

                            elif validator_with_data and isinstance(
                                validator_with_data.execution_engine.active_batch_data,
                                PandasBatchData,
                            ):
                                major, minor, *_ = pd.__version__.split(".")
                                if "pandas" in only_for:
                                    generate_test = True
                                if (
                                    (
                                        "pandas_022" in only_for
                                        or "pandas_023" in only_for
                                    )
                                    and major == "0"
                                    and minor in ["22", "23"]
                                ):
                                    generate_test = True
                                if ("pandas>=24" in only_for) and (
                                    (major == "0" and int(minor) >= 24)
                                    or int(major) >= 1
                                ):
                                    generate_test = True
                            elif validator_with_data and isinstance(
                                validator_with_data.execution_engine.active_batch_data,
                                SparkDFBatchData,
                            ):
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
                                (
                                    "sqlalchemy" in suppress_test_for
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                )
                                or (
                                    "sqlite" in suppress_test_for
                                    and sqliteDialect is not None
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        sqliteDialect,
                                    )
                                )
                                or (
                                    "postgresql" in suppress_test_for
                                    and postgresqlDialect is not None
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        postgresqlDialect,
                                    )
                                )
                                or (
                                    "mysql" in suppress_test_for
                                    and mysqlDialect is not None
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        mysqlDialect,
                                    )
                                )
                                or (
                                    "mssql" in suppress_test_for
                                    and mssqlDialect is not None
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        mssqlDialect,
                                    )
                                )
                                or (
                                    "bigquery" in suppress_test_for
                                    and BigQueryDialect is not None
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SqlAlchemyBatchData,
                                    )
                                    and hasattr(
                                        validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                        "name",
                                    )
                                    and validator_with_data.execution_engine.active_batch_data.sql_engine_dialect.name
                                    == "bigquery"
                                )
                                or (
                                    "pandas" in suppress_test_for
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        PandasBatchData,
                                    )
                                )
                                or (
                                    "spark" in suppress_test_for
                                    and validator_with_data
                                    and isinstance(
                                        validator_with_data.execution_engine.active_batch_data,
                                        SparkDFBatchData,
                                    )
                                )
                            ):
                                skip_test = True
                        # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
                        if (
                            "allow_cross_type_comparisons" in test["in"]
                            and validator_with_data
                            and isinstance(
                                validator_with_data.execution_engine.active_batch_data,
                                SqlAlchemyBatchData,
                            )
                        ):
                            skip_test = True

                        parametrized_tests.append(
                            {
                                "expectation_type": test_configuration[
                                    "expectation_type"
                                ],
                                "validator_with_data": validator_with_data,
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


@pytest.mark.order(index=0)
def test_case_runner_cfe(test_case):
    if test_case["skip"]:
        pytest.skip()

    # Note: this should never be done in practice, but we are wiping expectations to reuse batches during testing.
    # test_case["batch"]._initialize_expectations()
    if "parse_strings_as_datetimes" in test_case["test"]["in"]:
        with pytest.deprecated_call():
            evaluate_json_test_cfe(
                validator=test_case["validator_with_data"],
                expectation_type=test_case["expectation_type"],
                test=test_case["test"],
            )
    else:
        evaluate_json_test_cfe(
            validator=test_case["validator_with_data"],
            expectation_type=test_case["expectation_type"],
            test=test_case["test"],
        )
