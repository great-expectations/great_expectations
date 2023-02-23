import py
import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from tests.test_utils import working_directory


@pytest.mark.integration
def test_convert_to_file_context(
    tmpdir: py.path.local,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    context = ephemeral_context_with_defaults

    datasource_name = "my_pandas"
    context.sources.add_pandas(datasource_name)

    d = tmpdir.mkdir("tmp")
    with working_directory(str(d)):
        migrated_context = context.convert_to_file_context()

    assert isinstance(migrated_context, FileDataContext)
    assert (
        len(migrated_context.datasources) == 1
        and datasource_name in migrated_context.datasources
    )
