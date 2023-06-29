import glob
import json
import os
from typing import cast

import pandas as pd
import pytest

import great_expectations.compatibility.bigquery as BigQueryDialect
from great_expectations import DataContext
from great_expectations.compatibility import snowflake, sqlalchemy, trino
from great_expectations.compatibility.sqlalchemy import (
    SQLALCHEMY_NOT_IMPORTED,
)
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.self_check.util import (
    candidate_test_is_on_temporary_notimplemented_list_v3_api,
    evaluate_json_test_v3_api,
    generate_dataset_name_from_expectation_name,
    generate_sqlite_db_path,
    get_test_validator_with_data,
    mssqlDialect,
    mysqlDialect,
    pgDialect,
)
from great_expectations.util import build_in_memory_runtime_context
from tests.conftest import build_test_backends_list_v3_api

pytestmark = pytest.mark.sqlalchemy_version_compatibility

try:
    sqliteDialect = sqlalchemy.sqlite.dialect
except (ImportError, AttributeError):
    sqliteDialect = SQLALCHEMY_NOT_IMPORTED


def pytest_generate_tests(metafunc):  # noqa C901 - 35
    # Load all the JSON files in the directory
    dir_path = os.path.dirname(os.path.realpath(__file__))  # noqa: PTH120
    expectation_dirs = [
        dir_
        for dir_ in os.listdir(dir_path)
        if os.path.isdir(os.path.join(dir_path, dir_))  # noqa: PTH118, PTH112
    ]
    parametrized_tests = []
    ids = []
    backends = build_test_backends_list_v3_api(metafunc)
    validator_with_data = None

    for expectation_category in expectation_dirs:
        test_configuration_files = glob.glob(
            dir_path + "/" + expectation_category + "/*.json"
        )
        for backend in backends:
            for filename in test_configuration_files:
                with open(filename) as file:
                    pk_column: bool = False
                    test_configuration = json.load(file)
                    expectation_type = filename.split(".json")[0].split("/")[-1]
                    for index, test_config in enumerate(
                        test_configuration["datasets"], 1
                    ):
                        datasets = []
                        # optional only_for and suppress_test flag at the datasets-level that can prevent data being
                        # added to incompatible backends. Currently only used by expect_column_values_to_be_unique
                        only_for = test_config.get("only_for")
                        if only_for and not isinstance(only_for, list):
                            # coerce into list if passed in as string
                            only_for = [only_for]
                        suppress_test_for = test_config.get("suppress_test_for")
                        if suppress_test_for and not isinstance(
                            suppress_test_for, list
                        ):
                            # coerce into list if passed in as string
                            suppress_test_for = [suppress_test_for]
                        if candidate_test_is_on_temporary_notimplemented_list_v3_api(
                            backend, test_configuration["expectation_type"]
                        ):
                            skip_expectation = True
                        elif suppress_test_for and backend in suppress_test_for:
                            continue
                        elif only_for and backend not in only_for:
                            continue
                        else:
                            skip_expectation = False
                            if isinstance(test_config["data"], list):
                                sqlite_db_path = generate_sqlite_db_path()
                                sub_index: int = (
                                    1  # additional index needed when dataset is a list
                                )
                                for dataset in test_config["data"]:
                                    dataset_name = (
                                        generate_dataset_name_from_expectation_name(
                                            dataset=dataset,
                                            expectation_type=expectation_type,
                                            index=index,
                                            sub_index=sub_index,
                                        )
                                    )

                                    datasets.append(
                                        get_test_validator_with_data(
                                            execution_engine=backend,
                                            data=dataset["data"],
                                            table_name=dataset_name,
                                            schemas=dataset.get("schemas"),
                                            sqlite_db_path=sqlite_db_path,
                                            context=cast(
                                                DataContext,
                                                build_in_memory_runtime_context(),
                                            ),
                                        )
                                    )
                                validator_with_data = datasets[0]
                            else:
                                if expectation_category in [
                                    "column_map_expectations",
                                    "column_pair_map_expectations",
                                    "multicolumn_map_expectations",
                                ]:
                                    pk_column: bool = True

                                schemas = (
                                    test_config["schemas"]
                                    if "schemas" in test_config
                                    else None
                                )
                                dataset = test_config["data"]
                                dataset_name = (
                                    generate_dataset_name_from_expectation_name(
                                        dataset=dataset,
                                        expectation_type=expectation_type,
                                        index=index,
                                    )
                                )
                                validator_with_data = get_test_validator_with_data(
                                    execution_engine=backend,
                                    data=dataset,
                                    table_name=dataset_name,
                                    schemas=schemas,
                                    context=cast(
                                        DataContext, build_in_memory_runtime_context()
                                    ),
                                    pk_column=pk_column,
                                )

                        for test in test_config["tests"]:
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
                                    validator_with_data.active_batch_data,
                                    SqlAlchemyBatchData,
                                ):
                                    if "sqlalchemy" in only_for:
                                        generate_test = True
                                    elif (
                                        "sqlite" in only_for
                                        and sqliteDialect is not None
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            sqliteDialect,
                                        )
                                    ):
                                        generate_test = True
                                    elif (
                                        "postgresql" in only_for
                                        and pgDialect is not None
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            pgDialect,
                                        )
                                    ):
                                        generate_test = True
                                    elif (
                                        "mysql" in only_for
                                        and mysqlDialect is not None
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            mysqlDialect,
                                        )
                                    ):
                                        generate_test = True
                                    elif (
                                        "snowflake" in only_for
                                        and snowflake.snowflakedialect
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            snowflake.snowflakedialect.SnowflakeDialect,
                                        )
                                    ):
                                        generate_test = True
                                    elif (
                                        "mssql" in only_for
                                        and mssqlDialect is not None
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            mssqlDialect,
                                        )
                                    ):
                                        generate_test = True
                                    elif (
                                        "bigquery" in only_for
                                        and BigQueryDialect is not None
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "bigquery"
                                    ):
                                        generate_test = True
                                    elif (
                                        "bigquery_v3_api" in only_for
                                        and BigQueryDialect is not None
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "bigquery"
                                    ):
                                        # <WILL> : Marker to get the test to only run for CFE
                                        # expect_column_values_to_be_unique:negative_case_all_null_values_bigquery_nones
                                        # works in different ways between CFE (V3) and V2 Expectations. This flag allows for
                                        # the test to only be run in the CFE case
                                        generate_test = True
                                    elif (
                                        "trino" in test["only_for"]
                                        and trino.trinodialect
                                        and trino.trinodialect.TrinoDialect
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "trino"
                                    ):
                                        generate_test = True

                                elif validator_with_data and isinstance(
                                    validator_with_data.active_batch_data,
                                    PandasBatchData,
                                ):
                                    # Call out supported dialects
                                    if "pandas_v3_api" in only_for:
                                        generate_test = True
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
                                    if ("pandas>=024" in only_for) and (
                                        (major == "0" and int(minor) >= 24)
                                        or int(major) >= 1
                                    ):
                                        generate_test = True
                                elif validator_with_data and isinstance(
                                    validator_with_data.active_batch_data,
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
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                    )
                                    or (
                                        "sqlite" in suppress_test_for
                                        and sqliteDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            sqliteDialect,
                                        )
                                    )
                                    or (
                                        "postgresql" in suppress_test_for
                                        and pgDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            pgDialect,
                                        )
                                    )
                                    or (
                                        "mysql" in suppress_test_for
                                        and mysqlDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            mysqlDialect,
                                        )
                                    )
                                    or (
                                        "mssql" in suppress_test_for
                                        and mssqlDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            mssqlDialect,
                                        )
                                    )
                                    or (
                                        "snowflake" in suppress_test_for
                                        and snowflake.snowflakedialect
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and isinstance(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            snowflake.snowflakedialect.SnowflakeDialect,
                                        )
                                    )
                                    or (
                                        "bigquery" in suppress_test_for
                                        and BigQueryDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "bigquery"
                                    )
                                    or (
                                        "bigquery_v3_api" in suppress_test_for
                                        and BigQueryDialect is not None
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "bigquery"
                                    )
                                    or (
                                        "trino" in suppress_test_for
                                        and trino.trinodialect
                                        and trino.trinodialect.TrinoDialect
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            SqlAlchemyBatchData,
                                        )
                                        and hasattr(
                                            validator_with_data.active_batch_data.sql_engine_dialect,
                                            "name",
                                        )
                                        and validator_with_data.active_batch_data.sql_engine_dialect.name
                                        == "trino"
                                    )
                                    or (
                                        "pandas" in suppress_test_for
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            PandasBatchData,
                                        )
                                    )
                                    or (
                                        "pandas_v3_api" in suppress_test_for
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
                                            PandasBatchData,
                                        )
                                    )
                                    or (
                                        "spark" in suppress_test_for
                                        and validator_with_data
                                        and isinstance(
                                            validator_with_data.active_batch_data,
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
                                    validator_with_data.active_batch_data,
                                    SqlAlchemyBatchData,
                                )
                            ):
                                skip_test = True

                            parametrized_tests.append(
                                {
                                    "expectation_type": test_configuration[
                                        "expectation_type"
                                    ],
                                    "pk_column": pk_column,
                                    "validator_with_data": validator_with_data,
                                    "test": test,
                                    "skip": skip_expectation or skip_test,
                                }
                            )

                            ids.append(
                                backend
                                + "/"
                                + expectation_category
                                + "/"
                                + test_configuration["expectation_type"]
                                + ":"
                                + test["title"]
                            )
    metafunc.parametrize("test_case", parametrized_tests, ids=ids)


@pytest.mark.order(index=0)
@pytest.mark.integration
@pytest.mark.slow  # 12.68s
def test_case_runner_v3_api(test_case):
    if test_case["skip"]:
        pytest.skip()

    # Note: this should never be done in practice, but we are wiping expectations to reuse batches during testing.
    # test_case["batch"]._initialize_expectations()
    if "parse_strings_as_datetimes" in test_case["test"]["in"]:
        with pytest.deprecated_call():
            evaluate_json_test_v3_api(
                validator=test_case["validator_with_data"],
                expectation_type=test_case["expectation_type"],
                test=test_case["test"],
                pk_column=test_case["pk_column"],
            )
    else:
        evaluate_json_test_v3_api(
            validator=test_case["validator_with_data"],
            expectation_type=test_case["expectation_type"],
            test=test_case["test"],
            pk_column=test_case["pk_column"],
        )
