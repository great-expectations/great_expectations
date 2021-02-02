import os
import shutil
from typing import List, Union

import pandas as pd
from ruamel.yaml import YAML

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.data_context.util import file_relative_path

from ..test_utils import create_files_in_directory

yaml = YAML()


def test_get_batch_list_from_new_style_datasource_with_file_system_datasource_inferred_assets(
    empty_data_context, tmp_path_factory
):
    context = empty_data_context

    base_directory = str(
        tmp_path_factory.mktemp(
            "test_get_batch_list_from_new_style_datasource_with_file_system_datasource_inferred_assets"
        )
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "path/A-100.csv",
            "path/A-101.csv",
            "directory/B-1.csv",
            "directory/B-2.csv",
        ],
        file_content_fn=lambda: "x,y,z\n1,2,3\n2,3,5",
    )

    config = yaml.load(
        f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_data_connector:
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}
        glob_directive: "*/*.csv"

        default_regex:
            pattern: (.+)/(.+)-(\\d+)\\.csv
            group_names:
                - data_asset_name
                - letter
                - number
    """,
    )

    context.add_datasource(
        "my_datasource",
        **config,
    )

    batch_request: Union[dict, BatchRequest] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_data_connector",
        "data_asset_name": "path",
        "partition_request": {
            "partition_identifiers": {
                # "data_asset_name": "path",
                "letter": "A",
                "number": "101",
            }
        },
    }
    batch_list: List[Batch] = context.get_batch_list(**batch_request)

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]
    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "path"
    assert batch.batch_definition["partition_definition"] == {
        "letter": "A",
        "number": "101",
    }
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (2, 3)


def test_get_batch_list_from_new_style_datasource_with_file_system_datasource_configured_assets(
    empty_data_context, tmp_path_factory
):
    context = empty_data_context

    base_directory = str(
        tmp_path_factory.mktemp(
            "test_get_batch_list_from_new_style_datasource_with_file_system_datasource_configured_assets"
        )
    )

    titanic_asset_base_directory_path: str = os.path.join(base_directory, "data")
    os.makedirs(titanic_asset_base_directory_path)

    titanic_csv_source_file_path: str = file_relative_path(
        __file__, "../test_sets/Titanic.csv"
    )
    titanic_csv_destination_file_path: str = str(
        os.path.join(base_directory, "data/Titanic_19120414_1313.csv")
    )
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    config = yaml.load(
        f"""
class_name: Datasource

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_data_connector:
        class_name: ConfiguredAssetFilesystemDataConnector
        base_directory: {base_directory}
        glob_directive: "*.csv"

        default_regex:
            pattern: (.+)\\.csv
            group_names:
                - name
        assets:
            Titanic:
                base_directory: {titanic_asset_base_directory_path}
                pattern: (.+)_(\\d+)_(\\d+)\\.csv
                group_names:
                    - name
                    - timestamp
                    - size
    """,
    )

    context.add_datasource(
        "my_datasource",
        **config,
    )

    batch_request: Union[dict, BatchRequest] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_data_connector",
        "data_asset_name": "Titanic",
        "partition_request": {
            "partition_identifiers": {
                "name": "Titanic",
                "timestamp": "19120414",
                "size": "1313",
            }
        },
    }
    batch_list: List[Batch] = context.get_batch_list(**batch_request)

    assert len(batch_list) == 1

    batch: Batch = batch_list[0]
    assert batch.batch_spec is not None
    assert batch.batch_definition["data_asset_name"] == "Titanic"
    assert batch.batch_definition["partition_definition"] == {
        "name": "Titanic",
        "timestamp": "19120414",
        "size": "1313",
    }
    assert isinstance(batch.data, pd.DataFrame)
    assert batch.data.shape == (1313, 7)
