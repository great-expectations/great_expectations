from typing import List

from great_expectations import DataContext
from great_expectations.data_context.types.base import DatasourceConfig


def test_list_datasources_base_data_context_no_datasources(
    empty_data_context: DataContext, datasource_config_with_names: DatasourceConfig
) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context: DataContext = empty_data_context

    # no datasources

    observed: List[dict] = context.list_datasources()

    expected: List[dict] = []

    assert observed == expected


def test_list_datasources_base_data_context_one_datasource(
    empty_data_context: DataContext, datasource_config_with_names: DatasourceConfig
) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context: DataContext = empty_data_context

    # one datasource

    context.add_datasource(**datasource_config_with_names.to_dict())

    observed = context.list_datasources()

    expected = [
        {
            "class_name": "Datasource",
            "data_connectors": {
                "tripdata_monthly_configured": {
                    "assets": {
                        "yellow": {
                            "class_name": "Asset",
                            "group_names": ["year", "month"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "yellow_tripdata_(\\d{4})-(\\d{2})\\.csv$",
                        }
                    },
                    "base_directory": "/path/to/trip_data",
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    # NOTE: no "name" field included for data connectors
                }
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        }
    ]

    assert observed == expected


def test_list_datasources_base_data_context_two_datasources(
    empty_data_context: DataContext, datasource_config_with_names: DatasourceConfig
) -> None:
    """What does this test and why?

    When listing datasources, we want to omit the name and id fields. This test uses DataContext.
    """

    context: DataContext = empty_data_context

    # two datasources

    context.add_datasource(**datasource_config_with_names.to_dict())

    datasource_config_two_with_names = datasource_config_with_names
    datasource_config_two_with_names.name = "datasource_two_name"

    context.add_datasource(**datasource_config_two_with_names.to_dict())

    observed = context.list_datasources()

    expected = [
        {
            "class_name": "Datasource",
            "data_connectors": {
                "tripdata_monthly_configured": {
                    "assets": {
                        "yellow": {
                            "class_name": "Asset",
                            "group_names": ["year", "month"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "yellow_tripdata_(\\d{4})-(\\d{2})\\.csv$",
                        }
                    },
                    "base_directory": "/path/to/trip_data",
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    # NOTE: no "name" field included for data connectors
                }
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        },
        {
            "class_name": "Datasource",
            "data_connectors": {
                "tripdata_monthly_configured": {
                    "assets": {
                        "yellow": {
                            "class_name": "Asset",
                            "group_names": ["year", "month"],
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "pattern": "yellow_tripdata_(\\d{4})-(\\d{2})\\.csv$",
                        }
                    },
                    "base_directory": "/path/to/trip_data",
                    "class_name": "ConfiguredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    # NOTE: no "name" field included for data connectors
                }
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "datasource_two_name",
        },
    ]

    assert observed == expected
