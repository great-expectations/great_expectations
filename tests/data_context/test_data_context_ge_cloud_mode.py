import os

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context import DataContext
from great_expectations.data_context.util import file_relative_path


def test_data_context_ge_cloud_mode_with_runtime_cloud_config():
    context = DataContext(
        ge_cloud_mode=True,
        ge_cloud_base_url="",
        ge_cloud_account_id=""
    )


def test_data_context_ge_cloud_mode_with_env_var_cloud_config():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_with_conf_file_cloud_config():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_mixed_cloud_config_precedence():
    context = DataContext(ge_cloud_mode=True)


def test_data_context_ge_cloud_mode_with_incomplete_cloud_config():
    context = DataContext(ge_cloud_mode=True)
