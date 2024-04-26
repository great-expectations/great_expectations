import pytest

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.pandas_datasource import (
    ClipboardAsset,
    CSVAsset,
    ExcelAsset,
    FeatherAsset,
    FWFAsset,
    GBQAsset,
    HDFAsset,
    HTMLAsset,
    JSONAsset,
    ORCAsset,
    PandasDatasource,
    ParquetAsset,
    PickleAsset,
    SASAsset,
    SPSSAsset,
    SQLAsset,
    SQLQueryAsset,
    SQLTableAsset,
    StataAsset,
    TableAsset,
    XMLAsset,
    _PandasDataAsset,
)


@pytest.fixture
def datasource(mocker):
    return mocker.Mock(spec=PandasDatasource)


@pytest.fixture
def asset(request, datasource) -> _PandasDataAsset:
    asset = request.param
    asset._datasource = datasource  # same pattern Datasource uses to init Asset
    return asset


def _asset_parameters():
    return [
        pytest.param(
            ClipboardAsset(name="test_asset", type="clipboard"),
            id="ClipboardAsset",
        ),
        pytest.param(
            CSVAsset(name="test_asset", filepath_or_buffer="./test/file", type="csv"),  # type: ignore[call-arg]
            id="CSVAsset",
        ),
        pytest.param(
            ExcelAsset(name="test_asset", io="./test/file", type="excel"),  # type: ignore[call-arg]
            id="ExcelAsset",
        ),
        pytest.param(
            FeatherAsset(name="test_asset", path="./test/file", type="feather"),  # type: ignore[call-arg]
            id="FeatherAsset",
        ),
        pytest.param(
            FWFAsset(name="test_asset", filepath_or_buffer="./test/file", type="fwf"),  # type: ignore[call-arg]
            id="FWFAsset",
        ),
        pytest.param(
            GBQAsset(name="test_asset", query="select * from my_table;", type="gbq"),  # type: ignore[call-arg]
            id="GBQAsset",
        ),
        pytest.param(
            HDFAsset(name="test_asset", path_or_buf="./test/file", type="hdf"),  # type: ignore[call-arg]
            id="HDFAsset",
        ),
        pytest.param(
            HTMLAsset(name="test_asset", io="./test/file", type="html"),  # type: ignore[call-arg]
            id="HTMLAsset",
        ),
        pytest.param(
            JSONAsset(name="test_asset", path_or_buf="./test/file", type="json"),  # type: ignore[call-arg]
            id="JSONAsset",
        ),
        pytest.param(
            ORCAsset(name="test_asset", path="./test/file", type="orc"),  # type: ignore[call-arg]
            id="ORCAsset",
        ),
        pytest.param(
            ParquetAsset(name="test_asset", path="./test/file", type="parquet"),  # type: ignore[call-arg]
            id="ParquetAsset",
        ),
        pytest.param(
            PickleAsset(name="test_asset", filepath_or_buffer="./test/file", type="pickle"),  # type: ignore[call-arg]
            id="PickleAsset",
        ),
        pytest.param(
            SQLAsset(
                name="test_asset", sql="select * from my_table;", con="connection:url", type="sql"
            ),  # type: ignore[call-arg]
            id="SQLAsset",
        ),
        pytest.param(
            SQLQueryAsset(
                name="test_asset",
                sql="select * from my_table;",
                con="connection:url",
                type="sql_query",
            ),  # type: ignore[call-arg]
            id="SQLQueryAsset",
        ),
        pytest.param(
            SQLTableAsset(
                name="test_asset",
                table_name="my_table",
                con="connection:url",
                type="sql_table",
            ),  # type: ignore[call-arg]
            id="SQLTableAsset",
        ),
        pytest.param(
            SASAsset(name="test_asset", filepath_or_buffer="./test/file", type="sas"),  # type: ignore[call-arg]
            id="SASAsset",
        ),
        pytest.param(
            SPSSAsset(name="test_asset", path="./test/file", type="spss"),  # type: ignore[call-arg]
            id="SPSSAsset",
        ),
        pytest.param(
            StataAsset(name="test_asset", filepath_or_buffer="./test/file", type="stata"),  # type: ignore[call-arg]
            id="StataAsset",
        ),
        pytest.param(
            TableAsset(name="test_asset", filepath_or_buffer="./test/file", type="table"),  # type: ignore[call-arg]
            id="TableAsset",
        ),
        pytest.param(
            XMLAsset(name="test_asset", path_or_buffer="./test/file", type="xml"),  # type: ignore[call-arg]
            id="XMLAsset",
        ),
    ]


@pytest.mark.parametrize("asset", _asset_parameters(), indirect=["asset"])
def test_add_batch_definition_whole_dataframe(asset, datasource):
    # arrange
    name = "batch_def_name"
    expected_batch_definition = BatchDefinition(name=name, partitioner=None)
    datasource.add_batch_definition.return_value = expected_batch_definition

    # act
    batch_definition = asset.add_batch_definition_whole_dataframe(name=name)

    # assert
    assert batch_definition == expected_batch_definition
    datasource.add_batch_definition.assert_called_once_with(expected_batch_definition)
