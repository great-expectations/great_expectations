import pathlib

import pytest

from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from tests.test_utils import working_directory


@pytest.mark.filesystem
def test_convert_to_file_context(
    tmp_path: pathlib.Path,
    ephemeral_context_with_defaults: EphemeralDataContext,
):
    context = ephemeral_context_with_defaults

    datasource_name = "my_pandas"
    context.data_sources.add_pandas(datasource_name)

    tmp_path.mkdir(exist_ok=True)
    with working_directory(str(tmp_path)):
        migrated_context = context.convert_to_file_context()

    assert isinstance(migrated_context, FileDataContext)
    assert (
        len(migrated_context.data_sources.all()) == 1
        and datasource_name in migrated_context.data_sources.all()
    )
