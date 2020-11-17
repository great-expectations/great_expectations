import yaml
import pandas as pd

from great_expectations.core.batch import Batch
from ..test_utils import create_files_in_directory


def test_get_batch_list_from_new_style_datasource_with_file_system_execution_environment(
    empty_data_context,
    tmp_path_factory
):
    context = empty_data_context

    base_directory = str(tmp_path_factory.mktemp("test_get_batch_list_from_new_style_datasource_with_file_system_execution_environment"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "path/A-100.csv",
            "path/A-101.csv",
            "directory/B-1.csv",
            "directory/B-2.csv",
        ],
        file_content_fn=lambda: "x,y,z\n1,2,3\n2,3,5"
    )

    config = yaml.load(f"""
class_name: ExecutionEnvironment

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
    """, yaml.FullLoader)
    
    context.add_execution_environment(
        "my_execution_environment",
        config,
    )

    batch_list = context.get_batch_list_from_new_style_datasource({
        "execution_environment_name": "my_execution_environment",
        "data_connector_name": "my_data_connector",
        "data_asset_name": "path",
        "partition_request": {
            "partition_identifiers": {
                # "data_asset_name": "path",
                "letter": "A",
                "number": "101",
            }
        }
    })

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
