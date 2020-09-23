
import logging

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)


from great_expectations.execution_environment import ExecutionEnvironment
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.sorter import(
    DateTimeSorter,
    LexicographicSorter,
    CustomListSorter,
    NumericSorter
)

from great_expectations.data_context.data_context import (
    DataContextConfig,
    BaseDataContext,
)



action_list = [
            {
                'name': 'store_validation_result',
                'action': {
                    'class_name': 'StoreValidationResultAction'
                }
            }]

project_config = DataContextConfig(
            config_version=2,
            execution_environments={
                'my_test_execution_environment': {
                    'class_name': 'ExecutionEnvironment',
                    'execution_engine': {
                        'module_name': 'great_expectations.execution_engine',
                        'class_name': 'PandasExecutionEngine',
                        'caching': True,
                        'batch_spec_defaults': {}
                    },
                    'data_connectors': {
                        "default_data_connector": {
                            'partitioner_name': "my_standard_partitioner",
                            'module_name': 'great_expectations.execution_environment.data_connector',
                            'class_name': 'FilesDataConnector',
                            'config_params': {
                                'base_directory': '/Users/work/Development/GE_data/my_dir',
                                'glob_directive': '*',
                            },
                            'assets': {
                                'test_asset_0': {
                                    'config_params': {
                                        'glob_directive': 'alex*',
                                    },
                                    'partitioner': 'my_standard_partitioner',
                                }
                            },
                            'default_partitioner': 'my_standard_partitioner',
                            'partitioners': {
                                'my_standard_partitioner': {
                                    'module_name': 'great_expectations.execution_environment.data_connector.partitioner.regex_partitioner',
                                    'class_name': 'RegexPartitioner',
                                    'config_params': {
                                        'regex': r'.+\/(.+)_(.+)_(.+)\.csv',
                                        'allow_multifile_partitions': False,
                                    },
                                    'sorters': [
                                        {
                                            'name': 'name',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter',
                                            'class_name': 'LexicographicSorter',
                                            'orderby': 'asc',
                                        },
                                        {
                                            'name': 'timestamp',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter.date_time_sorter',
                                            'class_name': 'DateTimeSorter',
                                            'orderby': 'desc',
                                            'config_params': {
                                                'datetime_format': '%Y%m%d',
                                            }
                                        },
                                        {
                                            'name': 'price',
                                            'module_name': 'great_expectations.execution_environment.data_connector.partitioner.sorter.numeric_sorter',
                                            'class_name': 'NumericSorter',
                                            'orderby': 'desc',
                                        },
                                    ]
                                }
                            },

                        }
                    }
                }
            },
            config_variables_file_path=None,
            plugins_directory=None,
            validation_operators={
                'action_list_operator': {
                    'class_name': 'ActionListValidationOperator',
                    'action_list': action_list
                }
            },
    stores={
        'expectations': {
            'class_name': 'ExpectationsStore',
            'store_backend': {
                'class_name': 'TupleS3StoreBackend',
                'bucket': "fakebucket",
                'prefix': 'great_expectations/JSON/EXECUTION_ENVIRONMENT_ENGINE/ExpectationSuites'
            }
        },
        'validations': {
            'class_name': 'ValidationsStore',
            'store_backend': {
                'class_name': 'TupleS3StoreBackend',
                'bucket': "fakebucket",
                'prefix': 'great_expectations/JSON/EXECUTION_ENVIRONMENT_ENGINE/Validations'
            }
        },
        'evaluation_parameters': {
            'class_name': 'EvaluationParameterStore'
        }
    },
    expectations_store_name='expectations',
    validations_store_name='validations',
    evaluation_parameter_store_name='evaluation_parameters',
    data_docs_sites={
        "fake_site_name": {
            'class_name': 'SiteBuilder',
            'store_backend': {
                'class_name': 'TupleS3StoreBackend',
                'bucket': "fakebucket",
                'prefix': 'great_expectations/HTML/EXECUTION_ENVIRONMENT_ENGINE'
            },
            'site_index_builder': {
                'class_name': 'DefaultSiteIndexBuilder',
                'show_cta_footer': True
            }
        }
    }
)


ge_context = BaseDataContext(project_config=project_config)
my_execution_environment = ge_context.get_execution_environment("my_test_execution_environment")
my_data_connector = my_execution_environment.get_data_connector("default_data_connector")
my_partitioner = my_data_connector.get_partitioner("my_standard_partitioner")

def test_sorter_instantiation():
    my_partitioner = my_data_connector.get_partitioner("my_standard_partitioner")

    name_sorter = my_partitioner.get_sorter("name")
    date_sorter = my_partitioner.get_sorter("timestamp")
    price_sorter = my_partitioner.get_sorter("price")

    all_sorters = my_partitioner.sorters
    assert len(all_sorters) == 3
    assert(all_sorters[0]) == name_sorter
    assert(all_sorters[1]) == date_sorter
    assert(all_sorters[2]) == price_sorter
