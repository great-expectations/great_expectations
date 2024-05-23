from typing import Final

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


PATH_LIKE_STR: Final[str] = "./test/file"
QUERY: Final[str] = "select * from my_table;"
CONNECTION_URL: Final[str] = "connection:url"


def _asset_parameters():
    return [
        pytest.param(
            ClipboardAsset(name="test_asset", type="clipboard"),
            id="ClipboardAsset",
        ),
        pytest.param(
            CSVAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="csv"),
            id="CSVAsset",
        ),
        pytest.param(
            ExcelAsset(name="test_asset", io=PATH_LIKE_STR, type="excel"),
            id="ExcelAsset",
        ),
        pytest.param(
            FeatherAsset(name="test_asset", path=PATH_LIKE_STR, type="feather"),
            id="FeatherAsset",
        ),
        pytest.param(
            FWFAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="fwf"),
            id="FWFAsset",
        ),
        pytest.param(
            GBQAsset(name="test_asset", query=QUERY, type="gbq"),
            id="GBQAsset",
        ),
        pytest.param(
            HDFAsset(name="test_asset", path_or_buf=PATH_LIKE_STR, type="hdf"),
            id="HDFAsset",
        ),
        pytest.param(
            HTMLAsset(name="test_asset", io=PATH_LIKE_STR, type="html"),
            id="HTMLAsset",
        ),
        pytest.param(
            JSONAsset(name="test_asset", path_or_buf=PATH_LIKE_STR, type="json"),
            id="JSONAsset",
        ),
        pytest.param(
            ORCAsset(name="test_asset", path=PATH_LIKE_STR, type="orc"),
            id="ORCAsset",
        ),
        pytest.param(
            ParquetAsset(name="test_asset", path=PATH_LIKE_STR, type="parquet"),
            id="ParquetAsset",
        ),
        pytest.param(
            PickleAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="pickle"),
            id="PickleAsset",
        ),
        pytest.param(
            SQLAsset(name="test_asset", sql=QUERY, con=CONNECTION_URL, type="sql"),
            id="SQLAsset",
        ),
        pytest.param(
            SQLQueryAsset(
                name="test_asset",
                sql=QUERY,
                con=CONNECTION_URL,
                type="sql_query",
            ),
            id="SQLQueryAsset",
        ),
        pytest.param(
            SQLTableAsset(
                name="test_asset",
                table_name="my_table",
                con=CONNECTION_URL,
                type="sql_table",
            ),
            id="SQLTableAsset",
        ),
        pytest.param(
            SASAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="sas"),
            id="SASAsset",
        ),
        pytest.param(
            SPSSAsset(name="test_asset", path=PATH_LIKE_STR, type="spss"),
            id="SPSSAsset",
        ),
        pytest.param(
            StataAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="stata"),
            id="StataAsset",
        ),
        pytest.param(
            TableAsset(name="test_asset", filepath_or_buffer=PATH_LIKE_STR, type="table"),
            id="TableAsset",
        ),
        pytest.param(
            XMLAsset(name="test_asset", path_or_buffer=PATH_LIKE_STR, type="xml"),
            id="XMLAsset",
        ),
    ]


@pytest.mark.unit
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
