import shutil

import pytest

from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidConfigError
from great_expectations.util import get_context


@pytest.mark.filesystem
def test_incomplete_uncommitted(tmp_path):
    """
    When a project is shared between users, it is common to have an incomplete
    uncommitted directory present. We should fail gracefully when config
    variables are missing.
    """
    local_dir = tmp_path / "root"
    fixture_path = file_relative_path(
        __file__,
        "./fixtures/contexts/incomplete_uncommitted/great_expectations",
    )
    shutil.copytree(fixture_path, local_dir)

    with pytest.raises(InvalidConfigError) as exc:
        _ = get_context(context_root_dir=local_dir)
        assert (
            "Unable to find match for config variable my_postgres_db. See "
            "https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials"
            in exc.value.message
        )
