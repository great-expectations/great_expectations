import pandas as pd

from great_expectations.data_context.data_context.lite_data_context import (
    LiteDataContext,
)
from great_expectations.data_context.util import file_relative_path
from tests.test_utils import _get_batch_request_from_validator


def test_LiteDataContext_read_csv_with_default_pandas_reader():
    ldc = LiteDataContext()
    my_validator = ldc.datasources["default_pandas_reader"].read_csv(
        file_relative_path(__file__, "../datasource/fixtures/example_1.csv"),
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)

    assert isinstance(
        my_batch_request["runtime_parameters"]["batch_data"], pd.DataFrame
    )
    assert my_batch_request["runtime_parameters"]["batch_data"].to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }


def test_LiteDataContext_read_csv_with_default_pandas_reader_using_dot_notation():
    ldc = LiteDataContext()
    my_validator = ldc.datasources.default_pandas_reader.read_csv(
        file_relative_path(__file__, "../datasource/fixtures/example_1.csv"),
    )
    my_batch_request = _get_batch_request_from_validator(my_validator)

    assert isinstance(
        my_batch_request["runtime_parameters"]["batch_data"], pd.DataFrame
    )
    assert my_batch_request["runtime_parameters"]["batch_data"].to_dict() == {
        "a": {0: 1, 1: 4},
        "b": {0: 2, 1: 5},
        "c": {0: 3, 1: 6},
    }


def test_LiteDataContext_sources():
    ldc = LiteDataContext()
    assert ldc.sources.default_pandas_reader == ldc.datasources.default_pandas_reader
