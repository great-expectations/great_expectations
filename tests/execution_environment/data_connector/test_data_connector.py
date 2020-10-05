import logging
import pytest
from mock import patch
import pandas as pd

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.files_data_connector import FilesDataConnector
from great_expectations.execution_environment.data_connector.pipeline_data_connector import PipelineDataConnector

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.pipeline_partitioner import PipelinePartitioner

from great_expectations.exceptions import DataConnectorError
from .reference_list import ReferenceListForTests
# TODO: <Alex>We might wish to invent more cool paths to test and different column types and sort orders...</Alex>
# TODO: <WILL> Is the order always going to be from "left-to-right"? in the example below, what happens if you want the date-time to take priority?
# TODO: more tests :  more complex custom lists, and playing with asc desc combinations

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)


def test_data_connector_instantiation(data_context_with_data_connector_and_partitioner):
    ge_context = data_context_with_data_connector_and_partitioner

    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")
    assets_config = {'assets': {
        'test_asset_1': {
            'config_params': {
                'partition_name': 'check_dataframe',
                },
            }
        }
    }
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
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    partitioner_config = {
                'my_standard_pipeline_partitioner':
                        {
                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                            'class_name': 'PipelinePartitioner',
                            'allow_multipart_partitions': False,
                            'sorters': [],
                            'config_params': {}
                        }
                  }

    assets_config = {
            'test_asset': {
                    'config_params': {
                            'partition_name': 'check_dataframe',
                                     }
                            }
                    }
    # params
    my_pipeline_connector = PipelineDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  default_partitioner="my_standard_pipeline_partitioner",
                                                  partitioners=partitioner_config,
                                                  assets=assets_config,
                                                  batch_definition_defaults=None,
                                                  in_memory_dataset=df)

    # get data_asset
    assert my_pipeline_connector.get_available_data_asset_names() == ['test_asset']

    # get_available_partitions
    available_partitions = my_pipeline_connector.get_available_partitions(data_asset_name="test_asset", partition_name='check_dataframe')

    assert available_partitions[0].name == 'check_dataframe'
    assert available_partitions[0].definition == {'check_dataframe': df}
    assert available_partitions[0].source is df
    assert available_partitions[0].data_asset_name == 'test_asset'

    # get_available_partitions with no data_asset_name
    available_partitions = my_pipeline_connector.get_available_partitions(partition_name='check_dataframe', repartition=True)
    assert available_partitions[0].data_asset_name == 'DEFAULT_DATA_ASSET'

    # build_batch_spec_from_partitions()
    batch_spec = my_pipeline_connector.build_batch_spec_from_partitions(partitions=available_partitions, batch_definition={}, batch_spec={})
    assert batch_spec == {'dataset': df }

    my_empty_pipeline_connector = PipelineDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  default_partitioner="my_standard_pipeline_partitioner",
                                                  partitioners=partitioner_config,
                                                  assets={},
                                                  batch_definition_defaults=None,
                                                  in_memory_dataset=df)

    assert my_empty_pipeline_connector.get_available_data_asset_names() == []




batch_paths_simple: list = [
    "my_dir/alex_20200809_1000.csv",
    "my_dir/eugene_20200809_1500.csv",
    "my_dir/abe_20200809_1040.csv",
]

@pytest.fixture()
def mock_files_list(monkeypatch):
    # TODO: this needs to be turned into a Class method
    def mock_files(temp_self, data_asset_name=None):
        return batch_paths_simple
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)

def test_files_data_connector_simplest(data_context_with_data_connector_and_partitioner, mock_files_list):
    """
     TEST CASE 1:
         Configuration:
             - only name and execution_environment and base_directory
         Results :
             - data_assets = the root of each file in base_directory
             - partitions = 1 partition for each file in base_directory
                          = data_asset_name = DEFAULT_DATA_ASSET

             * since no sorters are configured, it will be the order in which the files exist in base_directory
     """

    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    my_files_data_connector = FilesDataConnector(name="my_files_data_connnector",
                                                 execution_environment=my_execution_environment,
                                                 config_params={"base_directory": "/my_dir"},
                                                 )

    # files data connector specific params
    assert my_files_data_connector.known_extensions == ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json', '.csv.gz',
                                                        '.tsv.gz', '.feather']
    assert my_files_data_connector.reader_options == {}
    assert my_files_data_connector.reader_method == None
    assert my_files_data_connector.base_directory == '/my_dir'





    assert my_files_data_connector.get_available_data_asset_names() == ['alex_20200809_1000', 'eugene_20200809_1500', 'abe_20200809_1040']

    assert my_files_data_connector.get_available_partitions() == [
                                                                    Partition(name='alex_20200809_1000',
                                                                               definition={
                                                                                   'alex_20200809_1000': 'my_dir/alex_20200809_1000.csv'},
                                                                               source="my_dir/alex_20200809_1000.csv",
                                                                               data_asset_name="DEFAULT_DATA_ASSET"),
                                                                    Partition(name='eugene_20200809_1500',
                                                                               definition={
                                                                                   'eugene_20200809_1500': 'my_dir/eugene_20200809_1500.csv'},
                                                                               source="my_dir/eugene_20200809_1500.csv",
                                                                               data_asset_name="DEFAULT_DATA_ASSET"),
                                                                    Partition(name='abe_20200809_1040',
                                                                              definition={
                                                                                  'abe_20200809_1040': 'my_dir/abe_20200809_1040.csv'},
                                                                              source="my_dir/abe_20200809_1040.csv",
                                                                              data_asset_name="DEFAULT_DATA_ASSET"),
                                                                ]



def test_files_data_connector_partitioners_configured(data_context_with_data_connector_and_partitioner, mock_files_list):

    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    """
     TEST SCENARIO 2:
         Configuration:
             - name and execution_environment and base_directory
             - partitioner, but no sorter

         * since no sorters are configured, it will be the order in which the files exist in base_directory

         Results :
             - data_assets = the root of each file in base_directory
             - partitions = 1 partition for each file in base_directory
                          = data_asset_name = root of file (if data_asset_name not specified, or specified data_asset_name (test_asset_0)

     """

    partitioner_config = {'my_standard_regex_partitioner': {
        'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
        'class_name': 'RegexPartitioner',
        'config_params': {
            'regex': {'pattern': '.+\\/(.+)_(.+)_(.+)\\.csv',
                      'group_names': ['name', 'timestamp', 'price']}
        },
        'allow_multipart_partitions': False},
    }

    assets_config = {
                    'test_asset_0': {
                                    'config_params': {
                                                    'glob_directive': '*',
                                                    },
                                    }
                    }

    my_files_data_connector = FilesDataConnector(name="my_files_data_connnector",
                                             execution_environment=my_execution_environment,
                                             config_params={"base_directory": "/my_dir"},
                                             partitioners=partitioner_config,
                                             assets=assets_config,
                                             batch_definition_defaults=None,
                                             default_partitioner="my_standard_regex_partitioner"
                                             )

    returned_results_named = my_files_data_connector.get_available_partitions()
    assert returned_results_named == [
                                       Partition(name='alex-20200809-1000',
                                                 definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                                                 source="my_dir/alex_20200809_1000.csv", data_asset_name="alex_20200809_1000"),
                                       Partition(name='eugene-20200809-1500',
                                                 definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                                                 source="my_dir/eugene_20200809_1500.csv", data_asset_name="eugene_20200809_1500"),

                                       Partition(name='abe-20200809-1040',
                                                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                                                  source="my_dir/abe_20200809_1040.csv", data_asset_name="abe_20200809_1040"),
                                   ]

    returned_results_named = my_files_data_connector.get_available_partitions(data_asset_name="test_asset_0")
    assert returned_results_named == [
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  source="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  source="my_dir/eugene_20200809_1500.csv", data_asset_name="test_asset_0"),

        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  source="my_dir/abe_20200809_1040.csv", data_asset_name="test_asset_0"),
    ]


    # TODO: This test used to exist. See if it is still necessary
    #with pytest.raises(DataConnectorError):
    #    my_files_data_connector.get_available_partitions(data_asset_name="fake")


def test_files_data_connector_partitioners_configured_sorters(data_context_with_data_connector_and_partitioner, mock_files_list):
    """
    TEST SCENARIO 3:
     Configuration:
         - name and execution_environment and base_directory
         - partitioner configured
         - sorters configured ( name, timestamp, price )

     Results :
         - data_assets = the root of each file in base_directory
         - partitions = 1 partition for each file in base_directory

    """

    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")


    partitioner_config = {'my_standard_regex_partitioner': {
        'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
        'class_name': 'RegexPartitioner',
        'config_params': {
            'regex': {'pattern': '.+\\/(.+)_(.+)_(.+)\\.csv',
                      'group_names': ['name', 'timestamp', 'price']}
        },
        'allow_multipart_partitions': False,
        'sorters': [
                        {
                            'name': 'name',
                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                            'class_name': 'LexicographicSorter',
                            'orderby': 'asc',
                        },
                        {
                            'name': 'timestamp',
                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                            'class_name': 'DateTimeSorter',
                            'orderby': 'desc',
                            'config_params': {
                                'datetime_format': '%Y%m%d',
                            }
                        },
                        {
                            'name': 'price',
                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                            'class_name': 'NumericSorter',
                            'orderby': 'desc',
                        },
                    ]
        },
    }

    assets_config = {
                    'test_asset_0': {
                                    'config_params': {
                                                    'glob_directive': '*',
                                                    },
                                    }
                    }

    my_files_data_connector = FilesDataConnector(name="my_files_data_connnector",
                                             execution_environment=my_execution_environment,
                                             config_params={"base_directory": "/my_dir"},
                                             partitioners=partitioner_config,
                                             assets=assets_config,
                                             batch_definition_defaults=None,
                                             default_partitioner="my_standard_regex_partitioner"
                                             )

    returned_results_named = my_files_data_connector.get_available_partitions(data_asset_name="test_asset_0")
    assert returned_results_named == [
        Partition(name='abe-20200809-1040',
                  definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                  source="my_dir/abe_20200809_1040.csv", data_asset_name="test_asset_0"),
        Partition(name='alex-20200809-1000',
                  definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                  source="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
        Partition(name='eugene-20200809-1500',
                  definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                  source="my_dir/eugene_20200809_1500.csv", data_asset_name="test_asset_0"),
    ]
