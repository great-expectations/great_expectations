import os
import shutil
from unittest.mock import patch, PropertyMock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path


def test_data_context_ge_cloud_mode_with_runtime_cloud_config(
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_account_id,
    ge_cloud_runtime_access_token,
    data_context_with_mocked_global_config_dirs
):
    context = data_context_with_mocked_global_config_dirs(
        ge_cloud_mode=True,
        ge_cloud_base_url=ge_cloud_runtime_base_url,
        ge_cloud_account_id=ge_cloud_runtime_account_id,
        ge_cloud_access_token=ge_cloud_runtime_access_token
    )
    expected_project_config_with_variables_substituted = {'anonymous_usage_statistics': {'data_context_id': 'a8a35168-68d5-4366-90ae-00647463d37e',
                                'enabled': False,
                                'usage_statistics_url': 'https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics/complete/version/1'},
 'checkpoint_store_name': 'default_checkpoint_store',
 'config_variables_file_path': None,
 'config_version': 3.0,
 'data_docs_sites': {'default_site': {'class_name': 'SiteBuilder',
                                      'show_how_to_buttons': True,
                                      'site_index_builder': {'class_name': 'DefaultSiteIndexBuilder'},
                                      'store_backend': {'class_name': 'GeCloudStoreBackend',
                                                        'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                        'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                                 'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                        'ge_cloud_resource_type': 'rendered_data_doc',
                                                        'suppress_store_backend_id': True}}},
 'datasources': {'default_pandas_datasource': {'class_name': 'Datasource',
                                               'data_connectors': {'default_runtime_data_connector': {'batch_identifiers': None,
                                                                                                      'class_name': 'RuntimeDataConnector'}},
                                               'execution_engine': {'class_name': 'PandasExecutionEngine',
                                                                    'module_name': 'great_expectations.execution_engine'},
                                               'module_name': 'great_expectations.datasource'},
                 'default_spark_datasource': {'class_name': 'Datasource',
                                              'data_connectors': {'default_runtime_data_connector': {'batch_identifiers': None,
                                                                                                     'class_name': 'RuntimeDataConnector'}},
                                              'execution_engine': {'class_name': 'SparkDFExecutionEngine',
                                                                   'module_name': 'great_expectations.execution_engine'},
                                              'module_name': 'great_expectations.datasource'}},
 'evaluation_parameter_store_name': 'default_evaluation_parameter_store',
 'expectations_store_name': 'default_expectations_store',
 'notebooks': None,
 'plugins_directory': '/Users/foo/bar/my/plugins/directory/complete/version/1',
 'stores': {'default_checkpoint_store': {'class_name': 'CheckpointStore',
                                         'store_backend': {'class_name': 'GeCloudStoreBackend',
                                                           'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                           'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                                    'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                           'ge_cloud_resource_type': 'contract',
                                                           'suppress_store_backend_id': True}},
            'default_evaluation_parameter_store': {'class_name': 'EvaluationParameterStore'},
            'default_expectations_store': {'class_name': 'ExpectationsStore',
                                           'store_backend': {'class_name': 'GeCloudStoreBackend',
                                                             'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                             'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                                      'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                             'ge_cloud_resource_type': 'expectation_suite',
                                                             'suppress_store_backend_id': True}},
            'default_validations_store': {'class_name': 'ValidationsStore',
                                          'store_backend': {'class_name': 'GeCloudStoreBackend',
                                                            'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                            'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                                     'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                            'ge_cloud_resource_type': 'suite_validation_result',
                                                            'suppress_store_backend_id': True}}},
 'validations_store_name': 'default_validations_store'}
    expected_ge_cloud_config = {
        'base_url': ge_cloud_runtime_base_url,
        'account_id': ge_cloud_runtime_account_id,
        'access_token': ge_cloud_runtime_access_token
    }
    assert context.project_config_with_variables_substituted.to_json_dict() == expected_project_config_with_variables_substituted
    assert context.ge_cloud_config.to_json_dict() == expected_ge_cloud_config


def test_data_context_ge_cloud_mode_with_env_var_cloud_config():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_with_conf_file_cloud_config():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_mixed_cloud_config_precedence():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_with_incomplete_cloud_config():
    context = DataContext(ge_cloud_mode=True)
