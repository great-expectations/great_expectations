from unittest import mock

import pytest

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import DataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.exceptions import DataContextError, GeCloudError
from great_expectations.exceptions.exceptions import DatasourceInitializationError


@pytest.fixture
def ge_cloud_data_context_config(
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_runtime_access_token,
):
    """
    This fixture is used to replicate a response retrieved from a GE Cloud API request.
    The resulting data is packaged into a DataContextConfig.

    Please see DataContext._retrieve_data_context_config_from_ge_cloud for more details.
    """
    DEFAULT_GE_CLOUD_DATA_CONTEXT_CONFIG = f"""
    datasources:
      default_spark_datasource:
        execution_engine:
          module_name: great_expectations.execution_engine
          class_name: SparkDFExecutionEngine
        module_name: great_expectations.datasource
        class_name: Datasource
        data_connectors:
          default_runtime_data_connector:
            class_name: RuntimeDataConnector
            batch_identifiers:
                - timestamp
      default_pandas_datasource:
          execution_engine:
            module_name: great_expectations.execution_engine
            class_name: PandasExecutionEngine
          module_name: great_expectations.datasource
          class_name: Datasource
          data_connectors:
            default_runtime_data_connector:
              class_name: RuntimeDataConnector
              batch_identifiers:
                - timestamp

    stores:
      default_evaluation_parameter_store:
        class_name: EvaluationParameterStore

      default_expectations_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: GeCloudStoreBackend
          ge_cloud_base_url: {ge_cloud_runtime_base_url}
          ge_cloud_resource_type: expectation_suite
          ge_cloud_credentials:
            access_token: {ge_cloud_runtime_access_token}
            organization_id: {ge_cloud_runtime_organization_id}
          suppress_store_backend_id: True

      default_validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: GeCloudStoreBackend
          ge_cloud_base_url: {ge_cloud_runtime_base_url}
          ge_cloud_resource_type: suite_validation_result
          ge_cloud_credentials:
            access_token: {ge_cloud_runtime_access_token}
            organization_id: {ge_cloud_runtime_organization_id}
          suppress_store_backend_id: True

      default_checkpoint_store:
        class_name: CheckpointStore
        store_backend:
          class_name: GeCloudStoreBackend
          ge_cloud_base_url: {ge_cloud_runtime_base_url}
          ge_cloud_resource_type: contract
          ge_cloud_credentials:
            access_token: {ge_cloud_runtime_access_token}
            organization_id: {ge_cloud_runtime_organization_id}
          suppress_store_backend_id: True

    evaluation_parameter_store_name: default_evaluation_parameter_store
    expectations_store_name: default_expectations_store
    validations_store_name: default_validations_store
    checkpoint_store_name: default_checkpoint_store

    data_docs_sites:
      default_site:
        class_name: SiteBuilder
        show_how_to_buttons: true
        store_backend:
          class_name: GeCloudStoreBackend
          ge_cloud_base_url: {ge_cloud_runtime_base_url}
          ge_cloud_resource_type: rendered_data_doc
          ge_cloud_credentials:
            access_token: {ge_cloud_runtime_access_token}
            organization_id: {ge_cloud_runtime_organization_id}
          suppress_store_backend_id: True
        site_index_builder:
          class_name: DefaultSiteIndexBuilder
        site_section_builders:
          profiling: None

    anonymous_usage_statistics:
      enabled: true
      usage_statistics_url: https://dev.stats.greatexpectations.io/great_expectations/v1/usage_statistics
      data_context_id: {ge_cloud_data_context_config}
    """
    yaml = YAMLHandler()
    config = yaml.load(DEFAULT_GE_CLOUD_DATA_CONTEXT_CONFIG)
    return DataContextConfig(**config)


def test_data_context_ge_cloud_mode_with_incomplete_cloud_config_should_throw_error(
    ge_cloud_data_context_config,
    data_context_with_incomplete_global_config_in_dot_dir_only,
):
    # Don't want to make a real request in a unit test so we simply patch the config fixture
    with mock.patch(
        "great_expectations.data_context.DataContext._retrieve_data_context_config_from_ge_cloud",
        return_value=ge_cloud_data_context_config,
    ):
        with pytest.raises(DataContextError):
            DataContext(context_root_dir="/my/context/root/dir", ge_cloud_mode=True)


@mock.patch("requests.get")
def test_data_context_ge_cloud_mode_makes_successful_request_to_cloud_api(
    mock_request,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_runtime_access_token,
):
    # Ensure that the request goes through
    mock_request.return_value.status_code = 200
    try:
        DataContext(
            ge_cloud_mode=True,
            ge_cloud_base_url=ge_cloud_runtime_base_url,
            ge_cloud_organization_id=ge_cloud_runtime_organization_id,
            ge_cloud_access_token=ge_cloud_runtime_access_token,
        )
    except:  # Not concerned with constructor output (only evaluating interaction with requests during __init__)
        pass

    called_with_url = f"{ge_cloud_runtime_base_url}/organizations/{ge_cloud_runtime_organization_id}/data-context-configuration"
    called_with_header = {
        "headers": {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {ge_cloud_runtime_access_token}",
        }
    }

    # Only ever called once with the endpoint URL and auth token as args
    mock_request.assert_called_once()
    assert mock_request.call_args[0][0] == called_with_url
    assert mock_request.call_args[1] == called_with_header


@mock.patch("requests.get")
def test_data_context_ge_cloud_mode_with_bad_request_to_cloud_api_should_throw_error(
    mock_request,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_runtime_access_token,
):
    # Ensure that the request fails
    mock_request.return_value.status_code = 401

    with pytest.raises(GeCloudError):
        DataContext(
            ge_cloud_mode=True,
            ge_cloud_base_url=ge_cloud_runtime_base_url,
            ge_cloud_organization_id=ge_cloud_runtime_organization_id,
            ge_cloud_access_token=ge_cloud_runtime_access_token,
        )


def test_datasource_initialization_error_thrown_in_cloud_mode(
    ge_cloud_data_context_config: DataContextConfig,
    ge_cloud_runtime_base_url,
    ge_cloud_runtime_organization_id,
    ge_cloud_runtime_access_token,
):
    # normally the DataContext swallows exceptions when there is an error raised from get_datasource
    # (which is used during initialization). In cloud mode, we want a DatasourceInitializationError to
    # propogate.

    # normally in cloud mode configuration is retrieved from an endpoint; we're providing it here in-line
    with mock.patch(
        "great_expectations.data_context.DataContext._retrieve_data_context_config_from_ge_cloud",
        return_value=ge_cloud_data_context_config,
    ):
        # DataContext._init_datasources calls get_datasource, which may generate a DatasourceInitializationError
        # that normally gets swallowed.
        with mock.patch(
            "great_expectations.data_context.DataContext.get_datasource"
        ) as get_datasource:
            get_datasource.side_effect = DatasourceInitializationError(
                "mock_datasource", "mock_message"
            )
            with pytest.raises(DatasourceInitializationError):
                DataContext(
                    ge_cloud_mode=True,
                    ge_cloud_base_url=ge_cloud_runtime_base_url,
                    ge_cloud_organization_id=ge_cloud_runtime_organization_id,
                    ge_cloud_access_token=ge_cloud_runtime_access_token,
                )
