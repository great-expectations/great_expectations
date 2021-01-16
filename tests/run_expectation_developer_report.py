import string
import importlib.util
import sys
import traceback
from pandas import DataFrame as pandas_DataFrame

try:
    from pyspark.sql import DataFrame as spark_DataFrame
except ImportError:
    spark_DataFrame = type(None)

from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
)
from tests.test_definitions.test_expectations import mssqlDialect as mssqlDialect
from tests.test_definitions.test_expectations import mysqlDialect as mysqlDialect
from tests.test_definitions.test_expectations import (
    postgresqlDialect as postgresqlDialect,
)
from tests.test_definitions.test_expectations import sqliteDialect as sqliteDialect
from tests.test_definitions.test_expectations import tmp_dir
from tests.test_utils import evaluate_json_test_cfe, get_test_validator_with_data
from tests.test_utils_modular import (
    candidate_test_is_on_temporary_notimplemented_list_cfe,
)


import datetime
import json
import locale
import os
import random
import shutil
import threading
from types import ModuleType
from typing import Union

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from ruamel.yaml import YAML

import great_expectations as ge
from great_expectations.core import ExpectationConfiguration, expectationSuiteSchema
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.datasource import SqlAlchemyDatasource
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.util import import_library_module


# TODO: this is a dup of the class defined in conftest.py
class LockingConnectionCheck:
    def __init__(self, sa, connection_string):
        self.lock = threading.Lock()
        self.sa = sa
        self.connection_string = connection_string
        self._is_valid = None

    def is_valid(self):
        with self.lock:
            if self._is_valid is None:
                try:
                    engine = self.sa.create_engine(self.connection_string)
                    conn = engine.connect()
                    conn.close()
                    self._is_valid = True
                except (ImportError, self.sa.exc.SQLAlchemyError) as e:
                    print(f"{str(e)}")
                    self._is_valid = False
            return self._is_valid


def build_test_backends_list():
    test_backends = ["pandas"]
    no_spark = True
    if not no_spark:
        try:
            import pyspark
            from pyspark.sql import SparkSession
        except ImportError:
            raise ValueError("spark tests are requested, but pyspark is not installed")
        test_backends += ["spark"]
    no_sqlalchemy = False
    if not no_sqlalchemy:
        test_backends += ["sqlite"]

        sa: Union[ModuleType, None] = import_library_module(module_name="sqlalchemy")

        no_postgresql = True
        if not (sa is None or no_postgresql):
            ###
            # NOTE: 20190918 - JPC: Since I've had to relearn this a few times, a note here.
            # SQLALCHEMY coerces postgres DOUBLE_PRECISION to float, which loses precision
            # round trip compared to NUMERIC, which stays as a python DECIMAL

            # Be sure to ensure that tests (and users!) understand that subtlety,
            # which can be important for distributional expectations, for example.
            ###
            connection_string = "postgresql://postgres@localhost/test_ci"
            checker = LockingConnectionCheck(sa, connection_string)
            if checker.is_valid() is True:
                test_backends += ["postgresql"]
            else:
                raise ValueError(
                    f"backend-specific tests are requested, but unable to connect to the database at "
                    f"{connection_string}"
                )
        mysql = False
        if sa and mysql:
            try:
                engine = sa.create_engine("mysql+pymysql://root@localhost/test_ci")
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mysql tests are requested, but unable to connect to the mysql database at "
                    "'mysql+pymysql://root@localhost/test_ci'"
                )
            test_backends += ["mysql"]
        mssql = False
        if sa and mssql:
            try:
                engine = sa.create_engine(
                    "mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@localhost:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true",
                    # echo=True,
                )
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                raise ImportError(
                    "mssql tests are requested, but unable to connect to the mssql database at "
                    "'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@localhost:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'",
                )
            test_backends += ["mssql"]
    return test_backends

def generate_expectation_tests(expectation_type, examples_config, expectation_execution_engines_dict=None):
    """

    :param expectation_type: snake_case name of the expectation type
    :param examples_config: a dictionary that defines the data and test cases for the expectation
    :param expectation_execution_engines_dict: (optional) a dictionary that shows which backends/execution engines the
            expectation is implemented for. It can be obtained from the output of the expectation's self_check method
            Example:
            {
             "PandasExecutionEngine": True,
             "SqlAlchemyExecutionEngine": False,
             "SparkDFExecutionEngine": False
            }
    :return:
    """
    parametrized_tests = []
    backends = build_test_backends_list()

    for c in backends:
        for d in examples_config:
            datasets = []
            if candidate_test_is_on_temporary_notimplemented_list_cfe(
                c, expectation_type
            ):
                skip_expectation = True
                schemas = validator_with_data = None
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

                # use the expectation_execution_engines_dict of the expectation
                # to exclude unimplemented backends from the testing
                if expectation_execution_engines_dict is not None:
                    supress_test_for = test.get("suppress_test_for")
                    if supress_test_for is None:
                        supress_test_for = []
                    if not expectation_execution_engines_dict.get("PandasExecutionEngine"):
                        supress_test_for.append("pandas")
                    if not expectation_execution_engines_dict.get("SqlAlchemyExecutionEngine"):
                        supress_test_for.append("sqlalchemy")
                    if not expectation_execution_engines_dict.get("SparkDFExecutionEngine"):
                        supress_test_for.append("spark")

                    if len(supress_test_for) > 0:
                        test["suppress_test_for"] = supress_test_for

                generate_test = True
                skip_test = False
                if "only_for" in test:
                    # if we're not on the "only_for" list, then never even generate the test
                    generate_test = False
                    if not isinstance(test["only_for"], list):
                        raise ValueError("Invalid test specification.")

                    if validator_with_data and isinstance(
                        validator_with_data.execution_engine.active_batch_data,
                        SqlAlchemyBatchData,
                    ):
                        # Call out supported dialects
                        if "sqlalchemy" in test["only_for"]:
                            generate_test = True
                        elif (
                            "sqlite" in test["only_for"]
                            and sqliteDialect is not None
                            and isinstance(
                                validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                sqliteDialect,
                            )
                        ):
                            generate_test = True
                        elif (
                            "postgresql" in test["only_for"]
                            and postgresqlDialect is not None
                            and isinstance(
                                validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                postgresqlDialect,
                            )
                        ):
                            generate_test = True
                        elif (
                            "mysql" in test["only_for"]
                            and mysqlDialect is not None
                            and isinstance(
                                validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                mysqlDialect,
                            )
                        ):
                            generate_test = True
                        elif (
                            "mssql" in test["only_for"]
                            and mssqlDialect is not None
                            and isinstance(
                                validator_with_data.execution_engine.active_batch_data.sql_engine_dialect,
                                mssqlDialect,
                            )
                        ):
                            generate_test = True
                    elif validator_with_data and isinstance(
                        validator_with_data.execution_engine.active_batch_data,
                        pandas_DataFrame,
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
                    elif validator_with_data and isinstance(
                        validator_with_data.execution_engine.active_batch_data,
                        spark_DataFrame,
                    ):
                        if "spark" in test["only_for"]:
                            generate_test = True

                if not generate_test:
                    continue

                if "suppress_test_for" in test and (
                    (
                        "sqlalchemy" in test["suppress_test_for"]
                        and validator_with_data
                        and isinstance(
                            validator_with_data.execution_engine.active_batch_data,
                            SqlAlchemyBatchData,
                        )
                    )
                    or (
                        "sqlite" in test["suppress_test_for"]
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
                        "postgresql" in test["suppress_test_for"]
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
                        "mysql" in test["suppress_test_for"]
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
                        "mssql" in test["suppress_test_for"]
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
                        "pandas" in test["suppress_test_for"]
                        and validator_with_data
                        and isinstance(
                            validator_with_data.execution_engine.active_batch_data,
                            pandas_DataFrame,
                        )
                    )
                    or (
                        "spark" in test["suppress_test_for"]
                        and validator_with_data
                        and isinstance(
                            validator_with_data.execution_engine.active_batch_data,
                            spark_DataFrame,
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

                if not skip_test:
                    parametrized_tests.append(
                        {
                            "expectation_type": expectation_type,
                            "validator_with_data": validator_with_data,
                            "test": test,
                            "skip": skip_expectation or skip_test,
                            "backend": c
                        }
                    )

    return parametrized_tests




def run(expectation_module_file_path):
    """
    This method accepts the path of a file it assumes to be a Python file that contains an Expectation.
    After loading the module and the class, it runs self_check on the Expectation class.
    It also generates and runs tests that are defined in the "examples" variable in the module (if exist).
    Only tests for the backends for which the Expectation is implemented (as reported bu self_check) are
    executed.

    The method returns a dictionary that contains the self check report and the test report:
    {
      "self_check_report": {
        ...
      },
      "test_report": [
        {
          "test title": "basic_positive_test",
          "backend": "pandas",
          "success": "false",
          "error_message": "",
          "stack_trace": "Traceback (most recent call last):\n  File \"run_expectation_developer_report.py\", line 441, in run\n    test=exp_test[\"test\"],\n  File \"/Users/eugenemandel/projects/great_expectations/tests/test_utils.py\", line 1185, in evaluate_json_test_cfe\n    data_asset=validator.execution_engine.active_batch_data,\n  File \"/Users/eugenemandel/projects/great_expectations/tests/test_utils.py\", line 1202, in check_json_test_result\n    assert result[\"success\"] == value\nAssertionError\n"
        }
      ]
    }

    TODO: lotsa error checking and reporting
    1. is the module name a valid snake_case?
    2. is the Expectation class name a valid camel case?
    3. are the class name and the module name in agreement?

    TODO: need to add an option for pretty-printing of the result


    :param expectation_module_file_path:
    :return:
    """
    res = {}

    module_name = os.path.splitext(os.path.basename(expectation_module_file_path))[0]

    module_spec = importlib.util.spec_from_file_location(module_name, expectation_module_file_path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    expectation_class_name = ''.join(x.title() for x in module_name.split('_'))

    expectation_class = getattr(module, expectation_class_name)


    self_check_report = expectation_class().self_check()
    res["self_check_report"] = self_check_report

    expectation_type = module_name

    test_results = []

    if hasattr(module, "examples"):

        exp_tests = generate_expectation_tests(expectation_type, module.examples, expectation_execution_engines_dict=self_check_report["execution_engines"])

        for exp_test in exp_tests:
            try:
                evaluate_json_test_cfe(
                    validator=exp_test["validator_with_data"],
                    expectation_type=exp_test["expectation_type"],
                    test=exp_test["test"],
                )
                test_results.append(
                    {
                        "test title": exp_test["test"]["title"],
                        "backend": exp_test["backend"],
                        "success": "true"
                    }
                )
            except Exception as e:
                test_results.append(
                    {
                        "test title": exp_test["test"]["title"],
                        "backend": exp_test["backend"],
                        "success": "false",
                        "error_message": str(e),
                        "stack_trace": traceback.format_exc()
                    }
                )


    res["test_report"] = test_results

    return res

if __name__ == '__main__':
    if len(sys.argv) > 0:
        res = run(sys.argv[1])

        print(json.dumps(res, indent=2))

