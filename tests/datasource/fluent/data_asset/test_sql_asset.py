import pytest

from great_expectations.core.partitioners import PartitionerYear
from great_expectations.datasource.fluent.sql_datasource import TableAsset
from tests.datasource.fluent.conftest import CreateSourceFixture


@pytest.fixture
def postgres_asset(empty_data_context, create_source: CreateSourceFixture, monkeypatch):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    monkeypatch.setattr(TableAsset, "test_connection", lambda _: None)
    years = [2021, 2022]
    asset_specified_metadata = {
        "pipeline_name": "my_pipeline",
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
    }

    with create_source(
        validate_batch_spec=lambda _: None,
        dialect="postgresql",
        data_context=empty_data_context,
        partitioner_query_response=[{"year": year} for year in years],
    ) as source:
        asset = source.add_table_asset(
            name="query_asset",
            table_name="my_table",
            batch_metadata=asset_specified_metadata,
            order_by=["year"],
        )
        assert asset.batch_metadata == asset_specified_metadata

        yield asset


@pytest.mark.postgresql
def test_get_batch_list_from_batch_request__sort_ascending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_list_from_batch_request(
        postgres_asset.build_batch_request(
            partitioner=PartitionerYear(column_name="year", sort_ascending=True)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2021, 2022]):
        batches[i].metadata["year"] = year


@pytest.mark.postgresql
def test_get_batch_list_from_batch_request__sort_descending(postgres_asset):
    years = [2021, 2022]
    batches = postgres_asset.get_batch_list_from_batch_request(
        postgres_asset.build_batch_request(
            partitioner=PartitionerYear(column_name="year", sort_ascending=False)
        )
    )

    assert len(batches) == len(years)
    for i, year in enumerate([2022, 2021]):
        batches[i].metadata["year"] = year
