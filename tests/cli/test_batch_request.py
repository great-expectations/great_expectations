from typing import cast
from unittest import mock

import pytest

from great_expectations.cli.batch_request import (
    _get_data_asset_name_from_data_connector,
    _is_data_connector_of_type,
    get_batch_request,
    _print_configured_asset_sql_data_connector_message,
)
from great_expectations.datasource import (
    Datasource,
    SimpleSqlalchemyDatasource,
    BaseDatasource,
)
from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,
)


@mock.patch("great_expectations.cli.batch_request.BaseDatasource")
@mock.patch("great_expectations.cli.batch_request._get_user_response")
def test_get_data_asset_name_from_data_connector_default_path(
    mock_user_input, mock_datasource
):
    mock_datasource.get_available_data_asset_names.return_value = {
        "my_data_connector": ["a", "b", "c", "d", "e"]
    }
    mock_user_input.side_effect = ["4"]  # Immediately select my asset
    data_asset_name = _get_data_asset_name_from_data_connector(
        mock_datasource, "my_data_connector", "my message prompt"
    )
    assert data_asset_name == "d"


@mock.patch("great_expectations.cli.batch_request.BaseDatasource")
@mock.patch("great_expectations.cli.batch_request._get_user_response")
def test_get_data_asset_name_from_data_connector_pagination(
    mock_user_input, mock_datasource
):
    mock_datasource.get_available_data_asset_names.return_value = {
        "my_data_connector": [f"my_file{n}" for n in range(200)]
    }
    mock_user_input.side_effect = [
        "l",  # Select listing/pagination option
        "n",  # Go to page 2 of my data asset listing
        "n",  # Go to page 3 of my data asset listing
        "34",  # Select the 34th option in page 3
    ]
    data_asset_name = _get_data_asset_name_from_data_connector(
        mock_datasource, "my_data_connector", "my message prompt"
    )
    assert data_asset_name == "my_file128"


@mock.patch("great_expectations.cli.batch_request.BaseDatasource")
@mock.patch("great_expectations.cli.batch_request._get_user_response")
def test_get_data_asset_name_from_data_connector_with_search(
    mock_user_input, mock_datasource
):
    files = [f"my_file{n}" for n in range(200)]
    target_file = "my_file2021-12-30"
    files.append(target_file)
    mock_datasource.get_available_data_asset_names.return_value = {
        "my_data_connector": files
    }
    mock_user_input.side_effect = [
        "s",  # Select search option
        "my_file20",  # Filter listing
        r"my_file\d{4}-\d{2}-\d{2}",  # Use regex to isolate one file with date format
        "1",  # Select the 1st and only option
    ]
    data_asset_name = _get_data_asset_name_from_data_connector(
        mock_datasource, "my_data_connector", "my message prompt"
    )
    assert data_asset_name == target_file


def test__print_configured_asset_sql_data_connector_message_prints_message(
    capsys,
):
    class DummyDataConnector:
        pass

    data_connector = DummyDataConnector()
    data_connector_name: str = "data_connector_name"

    class MockDatasource:
        @property
        def data_connectors(self) -> dict:
            return {data_connector_name: data_connector}

    datasource = MockDatasource()

    assert _is_data_connector_of_type(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=DummyDataConnector,
    )

    _print_configured_asset_sql_data_connector_message(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=DummyDataConnector,
    )

    output = capsys.readouterr().out

    assert "Need to configure a new Data Asset?" in output


def test__print_configured_asset_sql_data_connector_message_doesnt_print_message(
    capsys,
):
    class DummyDataConnector:
        pass

    data_connector = DummyDataConnector()
    data_connector_name: str = "data_connector_name"

    class MockDatasource:
        @property
        def data_connectors(self) -> dict:
            return {data_connector_name: data_connector}

    datasource = MockDatasource()

    class Dummy:
        pass

    assert not _is_data_connector_of_type(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=Dummy,
    )

    _print_configured_asset_sql_data_connector_message(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=Dummy,
    )

    output = capsys.readouterr().out

    assert output == ""
