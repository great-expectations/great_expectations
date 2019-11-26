import pytest

from great_expectations import DataContext
from great_expectations.exceptions import InvalidConfigError


def test_incomplete_uncommitted():
    # When a project is shared between users, it is common to have an incomplete uncommitted directory present.
    # We should fail gracefully when config variables are missing.
    with pytest.raises(InvalidConfigError) as exc:
        _ = DataContext('./tests/data_context/fixtures/contexts/incomplete_uncommitted/great_expectations')
    assert exc.value.message == "Unable to find match for config variable my_postgres_db"
