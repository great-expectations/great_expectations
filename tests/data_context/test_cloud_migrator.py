"""TODO: Add docstring"""
from unittest import mock

import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.data_context.cloud_migrator import CloudMigrator


@pytest.fixture
def mock_context_with_disabled_usage_stats() -> mock.MagicMock:
    context = mock.MagicMock()
    variables = context.variables
    usage_stats = variables.anonymous_usage_statistics
    usage_stats.enabled = False
    return context


@pytest.fixture
def migrator_with_mock_context(
    mock_context_with_disabled_usage_stats: mock.MagicMock,
) -> CloudMigrator:
    ge_cloud_base_url = ""
    ge_cloud_access_token = ""
    ge_cloud_organization_id = ""

    return CloudMigrator(
        context=mock_context_with_disabled_usage_stats,
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_access_token=ge_cloud_access_token,
        ge_cloud_organization_id=ge_cloud_organization_id,
    )


def test_cloud_migrator_test_migrate_true(empty_data_context: DataContext):
    """TODO: Test is a placeholder."""

    with pytest.raises(NotImplementedError):
        gx.CloudMigrator.migrate(test_migrate=True, context=empty_data_context)


@pytest.mark.unit
@pytest.mark.cloud
def test_cloud_migrator_test_migrate_true_emits_warnings(
    migrator_with_mock_context: CloudMigrator,
):
    migrator = migrator_with_mock_context

    with pytest.warns() as record:
        migrator._migrate_to_cloud(test_migrate=True)

    assert len(record) == 2
    assert "Please pass `test_migrate=False` to begin the actual migration" in str(
        record[0].message
    )
    assert (
        "Please note that by migrating your context to GX Cloud your new Cloud Data Context will emit usage statistics"
        in str(record[1].message)
    )
