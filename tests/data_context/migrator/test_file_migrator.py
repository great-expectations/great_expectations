import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.migrator.file_migrator import FileMigrator


@pytest.fixture
def file_migrator(in_memory_runtime_context: EphemeralDataContext):
    context = in_memory_runtime_context
    return FileMigrator(
        primary_stores=context.stores,
        datasource_store=context._datasource_store,
        variables=context.variables,
    )


def test_migrate(file_migrator: FileMigrator):
    file_migrator.migrate()
    pass
