"""TODO: Add docstring"""
import great_expectations as gx
from great_expectations import DataContext


def test_cloud_migrator_test_migrate_true(empty_data_context: DataContext):
    """TODO: Test not completed."""

    gx.CloudMigrator.migrate(test_migrate=True, context=empty_data_context)
