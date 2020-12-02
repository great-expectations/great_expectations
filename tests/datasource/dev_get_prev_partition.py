import os
import shutil
from typing import List, Union

import pandas as pd
import pytest
from ruamel.yaml import YAML

from great_expectations.core.batch import (
    Batch,
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from great_expectations.datasource.data_connector import (
    DataConnector,
    InferredAssetFilesystemDataConnector,
)
from great_expectations.datasource.data_connector.partitioner.partition import Partition
from great_expectations.datasource.new_datasource import Datasource
from tests.test_utils import (
    create_files_for_regex_partitioner,
    create_files_in_directory,
)

yaml = YAML()


@pytest.fixture
def basic_files_dataconnector_yaml(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("get_previous_partition"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=["my_asset/AAA.csv", "my_asset/BBB.csv", "my_asset/CCC.csv",],
    )

    # These are all part of `my_asset`
    # it has 3 partitions.... AAA, BBB, CCC
    #

    # <WILL> this is going to be configured in a weird way
    # we will ignore data_assets??

    return (
        base_directory,
        f"""
        class_name: InferredAssetFilesystemDataConnector
        base_directory: {base_directory}
        glob_directive: "*/*.csv"
        datasource_name: general_data_source
        default_regex:
          pattern: .*\\/(my_asset)\\/(.*).csv
          group_names:
            - data_asset_name
            - name
        sorters:
          - name: name
            class_name: LexicographicSorter
            orderby: desc
       """,
    )


@pytest.fixture
def basic_datasource(basic_files_dataconnector_yaml):
    my_datasource_yaml = f"""
module_name: great_expectations.datasource.datasource
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine
data_connectors:
    my_connector: {basic_files_dataconnector_yaml[1]}
           """

    my_datasource_loaded_yaml = yaml.load(my_datasource_yaml, Loader=yaml.FullLoader)

    my_datasource: Datasource = instantiate_class_from_config(
        config=my_datasource_loaded_yaml,
        runtime_environment={
            "name": "general_data_source",
            "data_context_root_directory": basic_files_dataconnector_yaml[0],
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={"module_name": "great_expectations.exec",},
    )

    return my_datasource


def test_stub(basic_datasource):
    assert isinstance(basic_datasource, Datasource)

    # TODO : see if empty BatchRequest can be used to return full batch_list
    # batch_list = basic_datasource.get_batch_list_from_batch_request(BatchRequest(data_connector_name="my_connector"))
    # print(batch_list)

    batch_definitions = basic_datasource.get_available_batch_definitions(
        BatchRequest(
            data_connector_name="my_connector", datasource_name="general_data_source",
        )
    )
    for defin in batch_definitions:
        print(defin)
        batch = basic_datasource.get_batch_from_batch_definition(defin)
        print(batch)
