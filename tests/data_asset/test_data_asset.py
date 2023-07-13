import os
import shutil

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations import __version__ as ge_version
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def data_context_simple_expectation_suite_with_custom_pandas_dataset(tmp_path_factory):
    """
    This data_context is *manually* created to have the config we want, vs
    created with DataContext.create()
    """
    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    fixture_dir = file_relative_path(__file__, "../test_fixtures")
    os.makedirs(
        os.path.join(asset_config_path, "my_dag_node"),
        exist_ok=True,
    )
    shutil.copy(
        os.path.join(
            fixture_dir, "great_expectations_basic_with_custom_pandas_dataset.yml"
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        os.path.join(
            fixture_dir,
            "rendering_fixtures/expectations_suite_1.json",
        ),
        os.path.join(asset_config_path, "default.json"),
    )
    os.makedirs(os.path.join(context_path, "plugins"), exist_ok=True)
    shutil.copy(
        os.path.join(fixture_dir, "custom_pandas_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_pandas_dataset.py")),
    )
    shutil.copy(
        os.path.join(fixture_dir, "custom_sparkdf_dataset.py"),
        str(os.path.join(context_path, "plugins", "custom_sparkdf_dataset.py")),
    )
    return gx.get_context(context_root_dir=context_path)


@pytest.mark.filesystem
def test_data_asset_expectation_suite(empty_data_context_stats_enabled):
    context: DataContext = empty_data_context_stats_enabled
    asset = DataAsset()
    default_suite = ExpectationSuite(
        expectation_suite_name="default",
        data_asset_type="DataAsset",
        meta={"great_expectations_version": ge_version},
        expectations=[],
        data_context=context,
    )

    # We should have a default-initialized suite stored internally and available for getting
    assert asset._expectation_suite == default_suite
    assert asset.get_expectation_suite() == default_suite


@pytest.mark.filesystem
def test_custom_expectation_default_arg_values_set(
    data_context_simple_expectation_suite_with_custom_pandas_dataset,
):
    # this test ensures that default arg values in custom expectations are being set properly
    context = data_context_simple_expectation_suite_with_custom_pandas_dataset

    df = pd.DataFrame({"a": [1, None, 1, 1], "b": [None, 1, 1, 1], "c": [1, 1, 1, 1]})

    batch_kwargs = {
        "data_asset_name": "multicolumn_ignore_row_if",
        "datasource": "mycustomdatasource",
        "dataset": df,
    }
    batch = context.get_batch(batch_kwargs, expectation_suite_name="default")
    # this expectation has a declared default arg value `ignore_row_if="any_value_is_missing"`
    # which overrides an internal default of "all_values_are_missing"
    # can only pass if the declared default is set properly
    result = batch.expect_column_sum_equals_3(column_list=["a", "b", "c"])
    assert result.success
