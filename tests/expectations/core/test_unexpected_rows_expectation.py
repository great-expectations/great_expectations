from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.expectations import UnexpectedRowsExpectation

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import Batch
    from great_expectations.datasource.fluent.sqlite_datasource import SqliteDatasource


@pytest.fixture
def taxi_db_path() -> str:
    return file_relative_path(__file__, "../../test_sets/quickstart/yellow_tripdata.db")


@pytest.fixture
def sqlite_datasource(
    in_memory_runtime_context: AbstractDataContext, taxi_db_path: str
) -> SqliteDatasource:
    context = in_memory_runtime_context
    datasource_name = "my_sqlite_datasource"
    return context.data_sources.add_sqlite(
        datasource_name, connection_string=f"sqlite:///{taxi_db_path}"
    )


@pytest.fixture
def sqlite_batch(sqlite_datasource: SqliteDatasource) -> Batch:
    datasource = sqlite_datasource
    asset = datasource.add_table_asset("yellow_tripdata_sample_2022_01")

    batch_request = asset.build_batch_request()
    return asset.get_batch(batch_request)


@pytest.mark.unit
@pytest.mark.parametrize(
    "query",
    [
        pytest.param("SELECT * FROM table", id="no batch"),
        pytest.param("SELECT * FROM {{ batch }}", id="invalid format"),
        pytest.param("SELECT * FROM {active_batch}", id="legacy syntax"),
    ],
)
def test_unexpected_rows_expectation_invalid_query_info_message(query: str, caplog, capfd):
    # info log is emitted
    with caplog.at_level(logging.INFO):
        UnexpectedRowsExpectation(unexpected_rows_query=query)

    # stdout is printed to console
    out, _ = capfd.readouterr()
    assert "{batch}" in out


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "query, expected_success, expected_observed_value, expected_unexpected_rows",
    [
        pytest.param(
            "SELECT * FROM {batch} WHERE passenger_count > 7",
            True,
            "0 unexpected rows",
            [],
            id="success",
        ),
        pytest.param(
            # There is a single instance where passenger_count == 7
            "SELECT * FROM {batch} WHERE passenger_count > 6",
            False,
            "1 unexpected row",
            [
                {
                    "?": 48422,
                    "DOLocationID": 132,
                    "PULocationID": 132,
                    "RatecodeID": 5.0,
                    "VendorID": 2,
                    "airport_fee": 0.0,
                    "congestion_surcharge": 0.0,
                    "extra": 0.0,
                    "fare_amount": 70.0,
                    "improvement_surcharge": 0.3,
                    "mta_tax": 0.0,
                    "passenger_count": 7.0,
                    "payment_type": 1,
                    "store_and_fwd_flag": "N",
                    "tip_amount": 21.09,
                    "tolls_amount": 0.0,
                    "total_amount": 91.39,
                    "tpep_dropoff_datetime": "2022-01-01 19:20:46",
                    "tpep_pickup_datetime": "2022-01-01 19:20:43",
                    "trip_distance": 0.0,
                }
            ],
            id="failure",
        ),
    ],
)
def test_unexpected_rows_expectation_validate(
    sqlite_batch: Batch,
    query: str,
    expected_success: bool,
    expected_observed_value: int,
    expected_unexpected_rows: list[dict],
):
    batch = sqlite_batch

    expectation = UnexpectedRowsExpectation(unexpected_rows_query=query)
    result = batch.validate(expectation)

    assert result.success is expected_success

    res = result.result
    assert res["observed_value"] == expected_observed_value

    unexpected_rows = res["details"]["unexpected_rows"]
    assert unexpected_rows == expected_unexpected_rows


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
    expectation = UnexpectedRowsExpectation(
        description=description,
        unexpected_rows_query=unexpected_rows_query,
    )
    expectation.render()
    assert (
        expectation.rendered_content[0].value.params.get("unexpected_rows_query").get("value")
        == unexpected_rows_query
    )

    assert expectation.rendered_content[0].value.template == description
    assert (
        expectation.rendered_content[0].value.code_block.get("code_template_str")
        == "$unexpected_rows_query"
    )
    assert expectation.rendered_content[0].value.code_block.get("language") == "sql"
