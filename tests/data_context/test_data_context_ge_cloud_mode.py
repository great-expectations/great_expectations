import os

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path


def test_data_context_ge_cloud_mode_runtime_config():
    pass
    # context = DataContext(
    #     ge_cloud_mode=True,
    #     ge_cloud_base_url=,
    #     ge_cloud_account_id=
    # )
