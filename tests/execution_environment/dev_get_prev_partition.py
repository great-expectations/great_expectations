import pytest
import os
import shutil
import pandas as pd
import yaml

from typing import Union, List

from great_expectations.execution_environment.data_connector import (
    DataConnector
)

from great_expectations.data_context import DataContext
from great_expectations.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.core.batch import (
    Batch,
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.data_context.util import (
    file_relative_path,
    instantiate_class_from_config,
)
from tests.test_utils import (
    execution_environment_files_data_connector_regex_partitioner_config,
    create_files_for_regex_partitioner,
    create_files_in_directory,
)



def test_name_date_price_list(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "AAA.csv",
            "BBB.csv",
            "CCC.csv,"
        ]
    )
    my_data_connector_yaml = yaml.load(f"""
           module_name: great_expectations.execution_environment.data_connector
           class_name: FilesDataConnector
           base_directory: {base_directory}
           glob_directive: '*'
           default_partitioner: my_standard_partitioner
           assets:
             DEFAULT_ASSET_NAME:
               config_params:
                 glob_directive: '*'
               partitioner: my_standard_partitioner
           partitioners:
             my_standard_partitioner:
               class_name: RegexPartitioner
               config_params:
                 regex:
                   pattern: .+\/(.+)\.csv
                   group_names:
                   - name
               allow_multipart_partitions: false
               sorters:
               - orderby: asc
                 class_name: LexicographicSorter
                 name: name
              
       """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
    config=my_data_connector_yaml,
    runtime_environment={
            "name": "general_filesystem_data_connector",
            "execution_environment_name": "BASE",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    with pytest.raises(NotImplementedError):
        partitions = my_data_connector.get_previous_batch_definition()
        print(partitions)