import datetime
import os
import uuid
from unittest import mock

import pytest

from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.store import (
    SuiteParameterStore,
    TupleAzureBlobStoreBackend,
    TupleGCSStoreBackend,
    TupleS3StoreBackend,
)
from great_expectations.data_context.types.resource_identifiers import (
    ValidationMetricIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config


@pytest.fixture(
    params=[
        {
            "class_name": "SuiteParameterStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "username": "postgres",
                    "password": "",
                    "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
                    "port": "5432",
                    "database": "test_ci",
                },
            },
        },
        {
            "class_name": "SuiteParameterStore",
            "module_name": "great_expectations.data_context.store",
        },
    ]
)
def param_store(request, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_environment={},
    )


@pytest.fixture(
    params=[
        {
            "class_name": "SuiteParameterStore",
            "store_backend": {
                "class_name": "InMemoryStoreBackend",
            },
        },
        {
            "class_name": "SuiteParameterStore",
            "module_name": "great_expectations.data_context.store",
        },
    ]
)
def in_memory_param_store(request, test_backends):
    if "postgresql" not in test_backends:
        pytest.skip("skipping fixture because postgresql not selected")

    return instantiate_class_from_config(
        config=request.param,
        config_defaults={
            "module_name": "great_expectations.data_context.store",
        },
        runtime_environment={},
    )


@pytest.mark.postgresql
def test_database_suite_parameter_store_basics(param_store):
    run_id = RunIdentifier(
        run_name=datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    )
    metric_identifier = ValidationMetricIdentifier(
        run_id=run_id,
        data_asset_name=None,
        expectation_suite_identifier="asset.warning",
        metric_name="expect_column_values_to_match_regex.result.unexpected_percent",
        metric_kwargs_id="column=mycol",
    )
    metric_value = 12.3456789

    param_store.set(metric_identifier, metric_value)
    value = param_store.get(metric_identifier)
    assert value == metric_value


@pytest.mark.postgresql
def test_database_suite_parameter_store_store_backend_id(in_memory_param_store):
    """
    What does this test and why?
    A Store should be able to report it's store_backend_id
    which is set when the StoreBackend is instantiated.
    """
    # Check that store_backend_id exists can be read
    assert in_memory_param_store.store_backend_id is not None
    # Check that store_backend_id is a valid UUID
    assert isinstance(in_memory_param_store.store_backend_id, uuid.UUID)


@mock.patch(
    "great_expectations.data_context.store.tuple_store_backend.TupleS3StoreBackend.list_keys"
)
@mock.patch("great_expectations.data_context.store.tuple_store_backend.TupleStoreBackend.list_keys")
@pytest.mark.cloud
def test_suite_parameter_store_calls_proper_cloud_tuple_store_methods(
    mock_parent_list_keys,
    mock_s3_list_keys,
):
    """
    What does this test and why?

    Demonstrate that SuiteParameterStore works as expected with TupleS3StoreBackend
    and that the store backend adheres to the Liskov substitution principle.
    """
    suite_parameter_store = SuiteParameterStore()
    s3_store = TupleS3StoreBackend(bucket="my_bucket")
    suite_parameter_store._store_backend = s3_store

    # Sanity check to ensure neither parent nor child method has been called
    assert not mock_s3_list_keys.called
    assert not mock_parent_list_keys.called


@mock.patch(
    "great_expectations.data_context.store.tuple_store_backend.TupleAzureBlobStoreBackend.list_keys"
)
@mock.patch("great_expectations.data_context.store.tuple_store_backend.TupleStoreBackend.list_keys")
@pytest.mark.big
def test_suite_parameter_store_calls_proper_azure_tuple_store_methods(
    mock_parent_list_keys,
    mock_azure_list_keys,
):
    """
    What does this test and why?

    Demonstrate that SuiteParameterStore works as expected with TupleAzureBlobStoreBackend
    and that the store backend adheres to the Liskov substitution principle.
    """
    suite_parameter_store = SuiteParameterStore()
    azure_store = TupleAzureBlobStoreBackend(
        container="my_container", connection_string="my_connection_string"
    )
    suite_parameter_store._store_backend = azure_store

    # Sanity check to ensure neither parent nor child method has been called
    assert not mock_azure_list_keys.called
    assert not mock_parent_list_keys.called


@mock.patch(
    "great_expectations.data_context.store.tuple_store_backend.TupleGCSStoreBackend.list_keys"
)
@mock.patch("great_expectations.data_context.store.tuple_store_backend.TupleStoreBackend.list_keys")
@pytest.mark.big
def test_suite_parameter_store_calls_proper_gcs_tuple_store_methods(
    mock_parent_list_keys,
    mock_gcs_list_keys,
):
    """
    What does this test and why?

    Demonstrate that SuiteParameterStore works as expected with TupleGCSStoreBackend
    and that the store backend adheres to the Liskov substitution principle.
    """
    suite_parameter_store = SuiteParameterStore()
    gcs_store = TupleGCSStoreBackend(bucket="my_bucket", project="my_project")
    suite_parameter_store._store_backend = gcs_store

    # Sanity check to ensure neither parent nor child method has been called
    assert not mock_gcs_list_keys.called
    assert not mock_parent_list_keys.called
