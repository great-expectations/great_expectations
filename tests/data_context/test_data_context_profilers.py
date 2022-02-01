from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext


def test_list_profilers_raises_configuration_error(empty_data_context: DataContext):
    with mock.patch(
        "great_expectations.data_context.DataContext.profiler_store",
    ) as mock_profiler_store:
        mock_profiler_store.__get__ = mock.Mock(return_value=None)
        with pytest.raises(ge_exceptions.StoreConfigurationError) as e:
            empty_data_context.list_profilers()

    assert "not a configured store" in str(e.value)
