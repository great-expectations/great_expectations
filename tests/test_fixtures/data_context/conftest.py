import pytest

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path


@pytest.fixture()
def taxicab_context():
    return DataContext(
        context_root_dir=file_relative_path(
            __file__, "./configs/great_expectations_taxicab_context.yml"
        )
    )
