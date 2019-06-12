import pytest

import json
import os
import shutil

from great_expectations.profile.base import DataSetProfiler
from great_expectations.profile.pseudo_pandas_profiling import PseudoPandasProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.data_context import DataContext
from great_expectations.util import safe_mmkdir


# Tests to write:
# test_cli_method_works  -> test_cli
# test context-based profile methods
# test class-based profile methods


def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DataSetProfiler.validate_dataset(1) == False
    assert DataSetProfiler.validate_dataset(toy_dataset)

    with pytest.raises(NotImplementedError) as e_info:
        DataSetProfiler.profile(toy_dataset)


def test_ColumnsExistProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    results = ColumnsExistProfiler.profile(toy_dataset)

    print(json.dumps(results, indent=2))

    assert results == {
        "data_asset_name": None,
        "data_asset_type": "Dataset",
        "meta": {
            "great_expectations.__version__": "0.7.0-beta"
        },
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {
                    "column": "x"
                }
            }
        ]
    }


def test_PseudoPandasProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) == 0

    results = PseudoPandasProfiler.profile(toy_dataset)

    print(json.dumps(results, indent=2))

    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) > 0


# @pytest.fixture()
# def parameterized_config_data_context(tmp_path_factory):
#     context_path = tmp_path_factory.mktemp("empty_context_dir")
#     context_path = str(context_path)
#     asset_config_path = os.path.join(
#         context_path, "great_expectations/expectations")
#     safe_mmkdir(asset_config_path, exist_ok=True)
#     shutil.copy("./tests/test_fixtures/great_expectations_basic.yml",
#                 str(context_path))
#     shutil.copy("./tests/test_fixtures/expectations/parameterized_expectations_config_fixture.json",
#                 str(asset_config_path))
#     return DataContext(context_path)


# parameterized_config_data_context):

@pytest.fixture()
def filesystem_csv_2(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp('test_files')
    base_dir = str(base_dir)

    # Put a file in the directory
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    toy_dataset.to_csv(os.path.join(base_dir, "f1.csv"), index=None)
    return base_dir


def test_context_profiler(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))

    assert empty_data_context.list_expectations_configs() == []
    empty_data_context.profile_datasource("my_datasource")

    print(empty_data_context.list_expectations_configs())
    assert empty_data_context.list_expectations_configs() != []

    profiled_expectations = empty_data_context.get_expectations('f1')
    print(json.dumps(profiled_expectations, indent=2))

    assert len(profiled_expectations["expectations"]) > 0
    # assert False
