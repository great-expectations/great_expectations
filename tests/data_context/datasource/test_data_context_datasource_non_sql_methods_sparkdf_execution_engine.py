import os
import shutil
from typing import List, Union

import pandas as pd
import pytest

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidBatchRequestError
from great_expectations.validator.validator import Validator
from tests.test_utils import create_files_in_directory

yaml = YAMLHandler()


@pytest.fixture
def context_with_single_titanic_csv_spark(
    empty_data_context, tmp_path_factory, spark_session
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
        __file__, "../../test_sets/Titanic.csv"
    )
    titanic_csv_destination_file_path: str = str(
        os.path.join(base_directory, "data/Titanic_19120414_1313.csv")
    )
    shutil.copy(titanic_csv_source_file_path, titanic_csv_destination_file_path)

    config = yaml.load(
        f"""
        class_name: Datasource

        execution_engine:
            class_name: SparkDFExecutionEngine

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
    return context


def test_get_validator(context_with_single_titanic_csv_spark):
    context: "DataContext" = context_with_single_titanic_csv_spark
    batch_request_dict: Union[dict, BatchRequest] = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_data_connector",
        "data_asset_name": "Titanic",
    }
    batch_request: BatchRequest = BatchRequest(**batch_request_dict)
    context.create_expectation_suite(expectation_suite_name="temp_suite")
    my_validator: Validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="temp_suite"
    )
    assert isinstance(my_validator, Validator)
    assert len(my_validator.batches) == 1
    assert my_validator.active_batch.data.dataframe.shape == (1313, 7)
