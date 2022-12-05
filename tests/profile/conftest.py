import os
import shutil
from typing import Set, Tuple

import pytest

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.util import file_relative_path
from great_expectations.self_check.util import get_dataset


@pytest.fixture
def titanic_data_context_modular_api(tmp_path_factory, monkeypatch):
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")
    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "../data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "./fixtures/great_expectations_titanic_0.13.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "../data/Titanic.csv"))
    )
    return gx.data_context.DataContext(context_path)


@pytest.fixture()
def possible_expectations_set():
    return {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
        "expect_column_values_to_be_in_type_list",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_null",
        "expect_column_proportion_of_unique_values_to_be_between",
        "expect_column_min_to_be_between",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_quantile_values_to_be_between",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_unique",
        "expect_compound_columns_to_be_unique",
    }


def get_set_of_columns_and_expectations_from_suite(
    suite: ExpectationSuite,
) -> Tuple[Set[str], Set[str]]:
    """
    Args:
        suite: An expectation suite

    Returns:
        A tuple containing a set of columns and a set of expectations found in a suite
    """
    columns: Set[str] = {
        i.kwargs.get("column") for i in suite.expectations if i.kwargs.get("column")
    }
    expectations: Set[str] = {i.expectation_type for i in suite.expectations}

    return columns, expectations


@pytest.fixture
def non_numeric_low_card_dataset(test_backend):
    """Provide dataset fixtures that have special values and/or are otherwise useful outside
    the standard json testing framework"""

    # fmt: off
    data = {
        "lowcardnonnum": [
            "a", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b", "b", "b", "b", "b", "b",
            "b", "b", "b", "b", "b",
        ]
    }
    # fmt: on
    schemas = {
        "pandas": {
            "lowcardnonnum": "str",
        },
        "postgresql": {
            "lowcardnonnum": "TEXT",
        },
        "sqlite": {
            "lowcardnonnum": "VARCHAR",
        },
        "mysql": {
            "lowcardnonnum": "TEXT",
        },
        "mssql": {
            "lowcardnonnum": "VARCHAR",
        },
        "spark": {
            "lowcardnonnum": "StringType",
        },
    }
    return get_dataset(test_backend, data, schemas=schemas)
