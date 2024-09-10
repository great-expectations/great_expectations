from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.data_context.util import file_relative_path

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource
    from great_expectations.validator.validator import Validator


@pytest.fixture
def taxi_db_path() -> str:
    return file_relative_path(
        __file__,
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db",
    )


@pytest.fixture
def sqlite_datasource(
    in_memory_runtime_context: AbstractDataContext, taxi_db_path: str
) -> SqliteDatasource:
    context = in_memory_runtime_context
    datasource_name = "my_sqlite_datasource"
    return context.sources.add_sqlite(
        datasource_name, connection_string=f"sqlite:///{taxi_db_path}"
    )


@pytest.fixture
def validator(sqlite_datasource: SqliteDatasource) -> Validator:
    datasource = sqlite_datasource
    context = datasource._data_context
    asset = datasource.add_table_asset("yellow_tripdata_sample_2019_01")

    batch_request = asset.build_batch_request()
    expectation_suite_name = "test_suite"
    context.add_expectation_suite(expectation_suite_name=expectation_suite_name)
    return context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )


@pytest.mark.unit
def test_unexpected_rows_expectation_invalid_query_info_message(
    validator: Validator, caplog, capfd
):
    # info log is emitted
    with caplog.at_level(logging.INFO):
        validator.unexpected_rows_expectation(
            unexpected_rows_query="SELECT * FROM yellow_tripdata_sample_2019_02"
        )

    # stdout is printed to console
    out, _ = capfd.readouterr()
    assert "{batch}" in out


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "query, expected_success, expected_observed_value, expected_unexpected_row_count",
    [
        pytest.param(
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            True,
            "0 unexpected rows",
            0,
            id="success",
        ),
        pytest.param(
            "SELECT * FROM {batch} WHERE passenger_count > 5",
            False,
            "252 unexpected rows",
            252,
            id="failure",
        ),
    ],
)
def test_unexpected_rows_expectation_validate(
    validator: Validator,
    query: str,
    expected_success: bool,
    expected_observed_value: int,
    expected_unexpected_row_count: int,
):
    result = validator.unexpected_rows_expectation(unexpected_rows_query=query)

    assert result.success is expected_success

    res = result.result
    assert res["observed_value"] == expected_observed_value

    unexpected_rows = res["details"]["unexpected_rows"]
    assert len(unexpected_rows) == expected_unexpected_row_count


@pytest.mark.unit
@pytest.mark.parametrize(
    "description, unexpected_rows_query",
    [
        pytest.param(
            "passenger_count should be less than or equal to 7",
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            id="with description",
        ),
        pytest.param(
            None,
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            id="no description",
        ),
    ],
)
def test_unexpected_rows_expectation_render(
    description: str | None,
    unexpected_rows_query: str,
):
    expectation = ExpectationConfiguration(
        expectation_type="unexpected_rows_expectation",
        description=description,
        kwargs={"unexpected_rows_query": unexpected_rows_query},
    )
    expectation.render()
    assert (
        expectation.rendered_content[0]
        .value.params.get("unexpected_rows_query")
        .get("value")
        == unexpected_rows_query
    )

    assert expectation.rendered_content[0].value.template == description
    assert (
        expectation.rendered_content[0].value.code_block.get("code_template_str")
        == "$unexpected_rows_query"
    )
    assert expectation.rendered_content[0].value.code_block.get("language") == "sql"
