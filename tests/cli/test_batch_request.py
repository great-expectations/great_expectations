from __future__ import annotations

from unittest import mock

import pytest

from great_expectations.cli.batch_request import (
    _get_data_asset_name_from_data_connector,
    _is_data_connector_of_type,
    _print_configured_asset_sql_data_connector_message,
)


pytestmark = [pytest.mark.cli]


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


class DummyDataConnector:
    pass


class NotDummyDataConnector:
    pass


@pytest.mark.unit
@pytest.mark.parametrize(
    ["data_connector_type", "expected_message"],
    [
        pytest.param(
            DummyDataConnector,
            (
                "Need to configure a new Data Asset? See how to add a new DataAsset to your "
                "DummyDataConnector here: "
                "https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource/\n"
            ),
            id="expected data connector type",
        ),
        pytest.param(NotDummyDataConnector, "", id="not expected data connector type"),
    ],
)
def test__print_configured_asset_sql_data_connector_message_prints_message(
    data_connector_type: DummyDataConnector | NotDummyDataConnector,
    expected_message: str,
    capsys,
):
    data_connector = DummyDataConnector()
    data_connector_name: str = "data_connector_name"

    class MockDatasource:
        @property
        def data_connectors(self) -> dict:
            return {data_connector_name: data_connector}

    datasource = MockDatasource()

    _print_configured_asset_sql_data_connector_message(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=data_connector_type,  # type: ignore[arg-type]
    )

    output = capsys.readouterr().out

    assert output == expected_message


@pytest.mark.unit
@pytest.mark.parametrize(
    ["data_connector_type", "_is_data_connector_of_type_expected"],
    [
        pytest.param(
            DummyDataConnector,
            True,
            id="expected data connector type",
        ),
        pytest.param(
            NotDummyDataConnector, False, id="not expected data connector type"
        ),
    ],
)
def test__is_data_connector_of_type(
    data_connector_type: DummyDataConnector | NotDummyDataConnector,
    _is_data_connector_of_type_expected: bool,
):
    data_connector = DummyDataConnector()
    data_connector_name: str = "data_connector_name"

    class MockDatasource:
        @property
        def data_connectors(self) -> dict:
            return {data_connector_name: data_connector}

    datasource = MockDatasource()

    _is_data_connector_of_type_observed = _is_data_connector_of_type(
        datasource=datasource,  # type: ignore[arg-type]
        data_connector_name=data_connector_name,
        data_connector_type=data_connector_type,  # type: ignore[arg-type]
    )

    assert _is_data_connector_of_type_observed == _is_data_connector_of_type_expected
