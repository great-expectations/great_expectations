
import logging
import pytest
from mock import patch
import pandas as pd

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.files_data_connector import FilesDataConnector
from great_expectations.execution_environment.data_connector.pipeline_data_connector import PipelineDataConnector

from great_expectations.execution_environment.data_connector.partitioner.pipeline_partitioner import PipelinePartitioner

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

data_asset_config = {}


assets_config = {
            'assets': {
                    'test_asset_1': {
                        'config_params': {
                            'partition_name': 'local_df',
                        },
                    }
                }
            }

pipeline_partitioner_config = {
                            'my_part': {
                                'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                                'class_name': 'PipelinePartitioner',
                                'config_params': {},
                                'allow_multipart_partitions': False,
                                'sorters': [],
                                }
                            }

def test_data_connector_instantiation(data_context_with_data_connector_and_partitioner):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    my_data_connector = DataConnector(name="test",
                                      execution_environment=my_execution_environment,
                                      default_partitioner="my_part",
                                      assets=assets_config,
                                      )

    # properties
    assert my_data_connector.name == "test"
    assert my_data_connector.default_partitioner == "my_part"
    assert my_data_connector.assets == assets_config
    assert my_data_connector.config_params == None
    assert my_data_connector.batch_definition_defaults == {}
    assert my_data_connector.partitions_cache == {}


def test_pipeline_data_connector(data_context_with_data_connector_and_partitioner):
    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)
    # test dataframe to attach to pipeline
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    partitioner_config = {}

    # params
    my_pipeline_connector = PipelineDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  #default_partitioner="my_part",
                                                  partitioners=partitioner_config,
                                                  assets=None,
                                                  config_params={},
                                                  batch_definition_defaults=None,
                                                  in_memory_dataset=df)


    please_work = my_pipeline_connector.get_available_partitions(partition_name='my_part')
    print(please_work)

    """
    "[{'name': 'my_part', 'definition': {'my_part': col1  col2
        0     1     3
                                        1     2     4}, 'source': col1  col2
          0     1     3
      1     2     4, 'data_asset_name': 'IN_MEMORY_DATA_ASSET'}]
    """

    # partitioner too
    #my_partitioner = PipelinePartitioner(data_connector=my_data_connector,
    #                             name="mine_all_mine",
    #                             allow_multipart_partitions=False,
    #                             )






batch_paths: list = [
    "my_dir/alex_20200809_1000.csv",
    "my_dir/eugene_20200809_1500.csv",
    "my_dir/james_20200811_1009.csv",
    "my_dir/abe_20200809_1040.csv",
    "my_dir/will_20200809_1002.csv",
    "my_dir/james_20200713_1567.csv",
    "my_dir/eugene_20201129_1900.csv",
    "my_dir/will_20200810_1001.csv",
    "my_dir/james_20200810_1003.csv",
    "my_dir/alex_20200819_1300.csv",
]

@pytest.fixture()
def mock_files_list(monkeypatch):
    # TODO: this needs to be turned into a Class method
    def mock_files(temp_self, data_asset_name):
        return batch_paths
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)


def test_files_data_connector(data_context_with_data_connector_and_partitioner, mock_files_list):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    # <TODO> this should actually be instantiated using the constructor.

    my_files_data_connector = my_execution_environment.get_data_connector("general_filesystem_data_connector")

    # files data connector specific params
    assert my_files_data_connector.known_extensions == ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json', '.csv.gz', '.tsv.gz', '.feather']
    assert my_files_data_connector.reader_options == {}
    assert my_files_data_connector.reader_method == None
    assert my_files_data_connector.base_directory == '/my_dir'


    #print(my_data_connector.get_available_data_asset_names())
    # get_available_data_asset_names()
    """
        This will load the data_assets that are set for this data connector
        - there are two modes off
            1. 
            2. 
    """


    # building the partitions

    # testing the cache

