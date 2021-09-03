import os
import shutil
from unittest.mock import patch, PropertyMock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path


def test_data_context_ge_cloud_mode_with_runtime_cloud_config(ge_cloud_runtime_base_url, ge_cloud_runtime_account_id,
                                                              ge_cloud_runtime_access_token, tmp_path):
    with patch("great_expectations.data_context.data_context.BaseDataContext.GLOBAL_CONFIG_PATHS",
               new_callable=PropertyMock) as mock:
        mock_global_config_home_dir = tmp_path / ".great_expectations"
        mock_global_config_home_dir_file = mock_global_config_home_dir / "great_expectations.conf"
        mock_global_config_home_dir.mkdir(parents=True)
        mock_global_config_etc_dir = tmp_path / "etc"
        mock_global_config_etc_file = mock_global_config_etc_dir / "great_expectations.conf"
        mock_global_config_etc_dir.mkdir(parents=True)

        mock_global_config_paths = [
            str(mock_global_config_home_dir_file),
            str(mock_global_config_etc_file)
        ]
        mock.return_value = mock_global_config_paths

        shutil.copy(
            file_relative_path(__file__, "./fixtures/conf/great_expectations_cloud_config_complete.conf"),
            str(os.path.join(mock_global_config_home_dir, "great_expectations.conf"))
        )

        context = DataContext(
            ge_cloud_mode=True,
            ge_cloud_base_url=ge_cloud_runtime_base_url,
            ge_cloud_account_id=ge_cloud_runtime_account_id,
            ge_cloud_access_token=ge_cloud_runtime_access_token
        )
        expected_project_config_with_variables_substituted = {'config_variables_file_path': None, 'datasources': {
            'default_spark_datasource': {'module_name': 'great_expectations.datasource', 'data_connectors': {
                'default_runtime_data_connector': {'class_name': 'RuntimeDataConnector', 'batch_identifiers': None}},
                                         'class_name': 'Datasource',
                                         'execution_engine': {'module_name': 'great_expectations.execution_engine',
                                                              'class_name': 'SparkDFExecutionEngine'}},
            'default_pandas_datasource': {'module_name': 'great_expectations.datasource', 'data_connectors': {
                'default_runtime_data_connector': {'class_name': 'RuntimeDataConnector', 'batch_identifiers': None}},
                                          'class_name': 'Datasource',
                                          'execution_engine': {'module_name': 'great_expectations.execution_engine',
                                                               'class_name': 'PandasExecutionEngine'}}},
                           'notebooks': None,
                           'stores': {'default_evaluation_parameter_store': {'class_name': 'EvaluationParameterStore'},
                                      'default_expectations_store': {'class_name': 'ExpectationsStore',
                                                                     'store_backend': {
                                                                         'class_name': 'GeCloudStoreBackend',
                                                                         'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                                         'ge_cloud_resource_type': 'expectation_suite',
                                                                         'ge_cloud_credentials': {
                                                                             'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                             'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                                         'suppress_store_backend_id': True}},
                                      'default_validations_store': {'class_name': 'ValidationsStore', 'store_backend': {
                                          'class_name': 'GeCloudStoreBackend',
                                          'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                          'ge_cloud_resource_type': 'suite_validation_result',
                                          'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                   'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                          'suppress_store_backend_id': True}},
                                      'default_checkpoint_store': {'class_name': 'CheckpointStore', 'store_backend': {
                                          'class_name': 'GeCloudStoreBackend',
                                          'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                          'ge_cloud_resource_type': 'contract',
                                          'ge_cloud_credentials': {'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                                   'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                          'suppress_store_backend_id': True}}}, 'data_docs_sites': {
                'default_site': {'class_name': 'SiteBuilder', 'show_how_to_buttons': True,
                                 'store_backend': {'class_name': 'GeCloudStoreBackend',
                                                   'ge_cloud_base_url': 'https://api.dev.greatexpectations.io/runtime',
                                                   'ge_cloud_resource_type': 'rendered_data_doc',
                                                   'ge_cloud_credentials': {
                                                       'access_token': 'b17bc2539062410db0a30e28fb0ee930',
                                                       'account_id': 'a8a35168-68d5-4366-90ae-00647463d37e'},
                                                   'suppress_store_backend_id': True},
                                 'site_index_builder': {'class_name': 'DefaultSiteIndexBuilder'}}},
                           'plugins_directory': '/Users/foo/bar/my/plugins/directory', 'config_version': 3.0,
                           'anonymous_usage_statistics': {'data_context_id': 'a8a35168-68d5-4366-90ae-00647463d37e',
                                                          'enabled': False,
                                                          'usage_statistics_url': 'https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics'},
                           'expectations_store_name': 'default_expectations_store',
                           'checkpoint_store_name': 'default_checkpoint_store',
                           'evaluation_parameter_store_name': 'default_evaluation_parameter_store',
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
