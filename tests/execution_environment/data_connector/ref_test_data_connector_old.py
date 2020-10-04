

# < REFERENCE FOR WORK IN PROGRESS TESTS >

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
    def mock_files(temp_self, data_asset_name=None):
        return batch_paths
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)



pipeline_partitioner_config = {
                            'my_part': {
                                'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                                'class_name': 'RegexPartitioner',
                                'allow_multipart_partitions': False,
                                'sorters': [
                                    {
                                        'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                        'class_name': 'CustomListSorter',
                                        "name": 'element',
                                        'orderby': 'asc',
                                        'reference_list': ref_periodic_table
                                    },
                                    {
                                        'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                        'class_name': 'CustomListSorter',
                                        "name": 'element',
                                        'orderby': 'asc',
                                    },
                                    {
                                        'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                        'class_name': 'CustomListSorter',
                                        "name": 'element',
                                        'orderby': 'asc',
                                    }

                                ],
                                }
                            }

def test_files_data_connector(data_context_with_data_connector_and_partitioner, mock_files_list):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    # <TODO> this should actually be instantiated using the constructor.
    #my_files_data_connector = my_execution_environment.get_data_connector("general_filesystem_data_connector")

    my_files_data_connector = FilesDataConnector(name="my_files_data_connnector",
                                                  execution_environment=my_execution_environment,
                                                  config_params={"base_directory": "/my_dir"},
                                                  partitioners=pipeline_partitioner_config,
                                                  assets=None,
                                                  batch_definition_defaults=None,
                                                  )


    # files data connector specific params
    assert my_files_data_connector.known_extensions == ['.csv', '.tsv', '.parquet', '.xls', '.xlsx', '.json', '.csv.gz', '.tsv.gz', '.feather']
    assert my_files_data_connector.reader_options == {}
    assert my_files_data_connector.reader_method == None
    assert my_files_data_connector.base_directory == '/my_dir'
    """
    returned_results_named = my_files_data_connector.get_available_partitions(data_asset_name="test_asset_0")
    assert returned_results_named == [Partition(name='abe-20200809-1040',
                                           definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                                           source="my_dir/abe_20200809_1040.csv", data_asset_name="test_asset_0"),
                                 Partition(name='alex-20200819-1300',
                                           definition={'name': 'alex', 'timestamp': '20200819', 'price': '1300'},
                                           source="my_dir/alex_20200819_1300.csv", data_asset_name="test_asset_0"),
                                 Partition(name='alex-20200809-1000',
                                           definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                                           source="my_dir/alex_20200809_1000.csv", data_asset_name="test_asset_0"),
                                 Partition(name='eugene-20201129-1900',
                                           definition={'name': 'eugene', 'timestamp': '20201129', 'price': '1900'},
                                           source="my_dir/eugene_20201129_1900.csv", data_asset_name="test_asset_0"),
                                 Partition(name='eugene-20200809-1500',
                                           definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                                           source="my_dir/eugene_20200809_1500.csv", data_asset_name="test_asset_0"),
                                 Partition(name='james-20200811-1009',
                                           definition={'name': 'james', 'timestamp': '20200811', 'price': '1009'},
                                           source="my_dir/james_20200811_1009.csv", data_asset_name="test_asset_0"),
                                 Partition(name='james-20200810-1003',
                                           definition={'name': 'james', 'timestamp': '20200810', 'price': '1003'},
                                           source="my_dir/james_20200810_1003.csv", data_asset_name="test_asset_0"),
                                 Partition(name='james-20200713-1567',
                                           definition={'name': 'james', 'timestamp': '20200713', 'price': '1567'},
                                           source="my_dir/james_20200713_1567.csv", data_asset_name="test_asset_0"),
                                 Partition(name='will-20200810-1001',
                                           definition={'name': 'will', 'timestamp': '20200810', 'price': '1001'},
                                           source="my_dir/will_20200810_1001.csv", data_asset_name="test_asset_0"),
                                 Partition(name='will-20200809-1002',
                                           definition={'name': 'will', 'timestamp': '20200809', 'price': '1002'},
                                           source="my_dir/will_20200809_1002.csv", data_asset_name="test_asset_0"),
                                 ]
    # THIS IS NECESSARY TO GET RID OF THE CACHE :
    # we should try and fix this
    my_files_data_connector.reset_partitions_cache()
    """
    # no data_asset_name means it is the stem of the filename
    returned_results = my_files_data_connector.get_available_partitions()
    assert returned_results ==  [Partition(name='abe-20200809-1040',
                                          definition={'name': 'abe', 'timestamp': '20200809', 'price': '1040'},
                                          source="my_dir/abe_20200809_1040.csv", data_asset_name="abe_20200809_1040"),
                                Partition(name='alex-20200819-1300',
                                          definition={'name': 'alex', 'timestamp': '20200819', 'price': '1300'},
                                          source="my_dir/alex_20200819_1300.csv", data_asset_name="alex_20200819_1300"),
                                Partition(name='alex-20200809-1000',
                                          definition={'name': 'alex', 'timestamp': '20200809', 'price': '1000'},
                                          source="my_dir/alex_20200809_1000.csv", data_asset_name="alex_20200809_1000"),
                                Partition(name='eugene-20201129-1900',
                                          definition={'name': 'eugene', 'timestamp': '20201129', 'price': '1900'},
                                          source="my_dir/eugene_20201129_1900.csv", data_asset_name="eugene_20201129_1900"),
                                Partition(name='eugene-20200809-1500',
                                          definition={'name': 'eugene', 'timestamp': '20200809', 'price': '1500'},
                                          source="my_dir/eugene_20200809_1500.csv", data_asset_name="eugene_20200809_1500"),
                                Partition(name='james-20200811-1009',
                                          definition={'name': 'james', 'timestamp': '20200811', 'price': '1009'},
                                          source="my_dir/james_20200811_1009.csv", data_asset_name="james_20200811_1009"),
                                Partition(name='james-20200810-1003',
                                          definition={'name': 'james', 'timestamp': '20200810', 'price': '1003'},
                                          source="my_dir/james_20200810_1003.csv", data_asset_name="james_20200810_1003"),
                                Partition(name='james-20200713-1567',
                                          definition={'name': 'james', 'timestamp': '20200713', 'price': '1567'},
                                          source="my_dir/james_20200713_1567.csv", data_asset_name="james_20200713_1567"),
                                Partition(name='will-20200810-1001',
                                          definition={'name': 'will', 'timestamp': '20200810', 'price': '1001'},
                                          source="my_dir/will_20200810_1001.csv", data_asset_name="will_20200810_1001"),
                                Partition(name='will-20200809-1002',
                                          definition={'name': 'will', 'timestamp': '20200809', 'price': '1002'},
                                          source="my_dir/will_20200809_1002.csv", data_asset_name="will_20200809_1002"),
                                ]

    my_files_data_connector.reset_partitions_cache()
    with pytest.raises(DataConnectorError):
        my_files_data_connector.get_available_partitions(data_asset_name="fake")

# <WILL> I know there is a better way to do this...
ref_periodic_table = ReferenceListForTests().ref_list

batch_paths: list = [
    "my_dir/Boron.csv",
    "my_dir/Oxygen.csv",
    "my_dir/Hydrogen.csv",
]

@pytest.fixture()
def mock_files_list_batch_paths(monkeypatch):
    # TODO: this needs to be turned into a Class method
    def mock_files(temp_self, data_asset_name=None):
        return batch_paths
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)


regex_partitioner_config = {
                            'my_new_part': {
                                'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
                                'class_name': 'RegexPartitioner',
                                'config_params': {},
                                'allow_multipart_partitions': False,
                                'sorters': [
                                    {
                                        'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                                        'class_name': 'CustomListSorter',
                                        "name": 'element',
                                        'orderby': 'asc',
                                        'reference_list': ref_periodic_table
                                    }
                                ],
                                }
                            }




def files_data_connector_custom_list_partitioner(data_context_with_data_connector_and_partitioner, mock_files_list_batch_paths):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    my_files_data_connector = FilesDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  config_params={"base_directory": "/my_dir"},
                                                  partitioners=pipeline_partitioner_config,
                                                  assets=None,
                                                  batch_definition_defaults=None,
                                                  )

    #my_periodic_table_partitioner = my_files_data_connector.get_partitioner("my_new_part")
    returned_partitions = my_files_data_connector.get_available_partitions()
    assert returned_partitions == [Partition(name='Boron',
                                          definition={'Boron': 'my_dir/Boron.csv'},
                                           source='my_dir/Boron.csv',
                                           data_asset_name="DEFAULT_DATA_ASSET"),
                                   Partition(name='Oxygen',
                                             definition={'Oxygen': 'my_dir/Oxygen.csv'},
                                             source='my_dir/Oxygen.csv',
                                             data_asset_name="DEFAULT_DATA_ASSET"),
                                   Partition(name='Hydrogen',
                                             definition={'Hydrogen': 'my_dir/Hydrogen.csv'},
                                             source='my_dir/Hydrogen.csv',
                                             data_asset_name="DEFAULT_DATA_ASSET"),
                                   ]

# slightly more complex test. Now the directory has an extra element, Vibranium
batch_path: list = [
    "my_dir/Boron.csv",
    "my_dir/Oxygen.csv",
    "my_dir/Hydrogen.csv",
    "my_dir/Vibranium.csv",
]

@pytest.fixture()
def mock_files_list_batch_paths_new(monkeypatch):
    # TODO: this needs to be turned into a Class method
    def mock_files(temp_self, data_asset_name=None):
        return batch_paths
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)


def files_data_connector_custom_list_partitioner_missing_in_reference_list(data_context_with_data_connector_and_partitioner, mock_files_list_batch_paths_new):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    my_files_data_connector = FilesDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  config_params={"base_directory": "/my_dir"},
                                                  partitioners=pipeline_partitioner_config,
                                                  assets=None,
                                                  batch_definition_defaults=None,
                                                  )

    my_files_data_connector.reset_partitions_cache()

    #with pytest.raises(ValueError):
    print(my_files_data_connector.get_available_partitions())




batch_paths: list = [
    "my_dir/Boron.csv",
    "my_dir/Oxygen.csv",
    "my_dir/Hydrogen.csv",
]

@pytest.fixture()
def mock_files_list_batch_paths(monkeypatch):
    def mock_files(temp_self, data_asset_name=None):
        return batch_paths
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)

def test_files_data_connector_custom_list_partitioner(data_context_with_data_connector_and_partitioner, mock_files_list_batch_paths):
    """
       TEST SCENARIO 3: Custom list partitioner (with Periodic Table list)
        Results :
            Sorted list of Partitions by periodic table elements
       """

    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    # reference list being loaded
    ref_periodic_table = ReferenceListForTests().ref_list

    assets_config = {
                    'test_asset_0': {
                                    'config_params': {
                                                    'glob_directive': '*',
                                                    },
                                    }
                    }

    my_partitioner_config = {
        'my_new_part': {
            'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
            'class_name': 'RegexPartitioner',
            'config_params': {},
            'allow_multipart_partitions': False,
            'sorters': [
                {
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'CustomListSorter',
                    "name": 'element',
                    'orderby': 'asc',
                    'reference_list': ref_periodic_table
                }
            ],
        }
    }

    my_files_data_connector = FilesDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  config_params={"base_directory": "/my_dir"},
                                                  partitioners=my_partitioner_config,
                                                  assets=assets_config,
                                                  batch_definition_defaults=None,
                                                  default_partitioner="my_new_part"
                                                  )


    #print(my_files_data_connector.get_partitioner_for_data_asset(data_asset_name="test_asset_0"))
    #returned_partitions = my_files_data_connector.get_available_partitions()
    #print(returned_partitions)
    """
    assert returned_partitions == [Partition(name='Boron',
                                          definition={'Boron': 'my_dir/Boron.csv'},
                                           source='my_dir/Boron.csv',
                                           data_asset_name="DEFAULT_DATA_ASSET"),
                                   Partition(name='Oxygen',
                                             definition={'Oxygen': 'my_dir/Oxygen.csv'},
                                             source='my_dir/Oxygen.csv',
                                             data_asset_name="DEFAULT_DATA_ASSET"),
                                   Partition(name='Hydrogen',
                                             definition={'Hydrogen': 'my_dir/Hydrogen.csv'},
                                             source='my_dir/Hydrogen.csv',
                                             data_asset_name="DEFAULT_DATA_ASSET"),
                                   ]
    """
# slightly more complex test. Now the directory has an extra element, Vibranium
batch_path_error: list = [
    "my_dir/Boron.csv",
    "my_dir/Oxygen.csv",
    "my_dir/Hydrogen.csv",
    "my_dir/Vibranium.csv",
]

@pytest.fixture()
def mock_files_list_batch_paths_new(monkeypatch):
    # TODO: this needs to be turned into a Class method
    def mock_files(temp_self, data_asset_name=None):
        return batch_path_error
    monkeypatch.setattr(FilesDataConnector, "_get_file_paths_for_data_asset", mock_files)


def files_data_connector_custom_list_partitioner_missing_in_reference_list(data_context_with_data_connector_and_partitioner, mock_files_list_batch_paths_new):
    ge_context = data_context_with_data_connector_and_partitioner
    my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")

    # reference list being loaded
    ref_periodic_table = ReferenceListForTests().ref_list

    assets_config = {
                    'test_asset_0': {
                                    'config_params': {
                                                    'glob_directive': '*',
                                                    },
                                    }
                    }
    my_partitioner_config = {
        'my_new_part': {
            'module_name': 'great_expectations.execution_environment.data_connector.partitioner',
            'class_name': 'RegexPartitioner',
            'config_params': {
                    'regex': {'pattern': '.+\\/(.+)\\.csv',
                              'group_names': ['name']}
            },
            'allow_multipart_partitions': False,
            'sorters': [
                {
                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter',
                    'class_name': 'CustomListSorter',
                    "name": 'element',
                    'orderby': 'asc',
                    'reference_list': ref_periodic_table
                }
            ],
        }
    }

    my_files_data_connector = FilesDataConnector(name="my_pipeline",
                                                  execution_environment=my_execution_environment,
                                                  config_params={"base_directory": "/my_dir"},
                                                  partitioners=my_partitioner_config,
                                                  assets=assets_config,
                                                  batch_definition_defaults=None,
                                                  )

    my_files_data_connector.reset_partitions_cache()

    #with pytest.raises(ValueError):
    print(my_files_data_connector.get_available_partitions())




