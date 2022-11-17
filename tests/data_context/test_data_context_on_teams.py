import pytest

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidConfigError


def test_incomplete_uncommitted():
    """
    When a project is shared between users, it is common to have an incomplete
    uncommitted directory present. We should fail gracefully when config
    variables are missing.
    """
    with pytest.raises(InvalidConfigError) as exc:
        _ = DataContext(
            file_relative_path(
                __file__,
                "./fixtures/contexts/incomplete_uncommitted/great_expectations",
            )
        )
        assert (
            "Unable to find match for config variable my_postgres_db. See "
            "https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials"
            in exc.value.message
        )
