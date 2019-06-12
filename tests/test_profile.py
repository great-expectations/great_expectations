import pytest

import json
import os
import shutil

from great_expectations.profile.base import DataSetProfiler
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
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
    expectations_config, evr_config = ColumnsExistProfiler.profile(toy_dataset)

    print(json.dumps(expectations_config, indent=2))

    # assert expectations_config == {
    #     "data_asset_name": None,
    #     "data_asset_type": "Dataset",
    #     "meta": {
    #         "great_expectations.__version__": "0.7.0-beta",
    #         "ColumnsExistProfiler": {
    #             "created_by": "BasicDatasetProfiler",
    #             "created_at": 0,
    #         },
    #     },
    #     "expectations": [
    #         {
    #             "expectation_type": "expect_column_to_exist",
    #             "kwargs": {
    #                 "column": "x"
    #             }
    #         }
    #     ]
    # }


def test_BasicDatasetProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})
    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) == 0

    expectations_config, evr_config = BasicDatasetProfiler.profile(toy_dataset)

    # print(json.dumps(expectations_config, indent=2))

    assert len(toy_dataset.get_expectations(
        suppress_warnings=True)["expectations"]) > 0

    # We should add an additional test that instantiates the batch via context, so the data_asset_name will be populated.
    assert expectations_config["data_asset_name"] == None
    assert "BasicDatasetProfiler" in expectations_config["meta"]
    # We should add an additional test that instantiates the batch via context, so that batch_kwargs will be populated.
    assert set(expectations_config["meta"]["BasicDatasetProfiler"].keys()) == {
        "created_by", "created_at"
    }
    # for exp in expectations_config["expectations"]:
    #     assert "BasicDatasetProfiler" in exp["meta"]
    #     assert exp["meta"]["BasicDatasetProfiler"] == {
    #         "confidence": "very low"
    #     }

    # Example:
    # {
    #     "data_asset_name": "notable_works_by_charles_dickens",
    #     "meta": {
    #         "great_expectations.__version__": "0.7.0-beta",
    #         "BasicDatasetProfiler": {
    #             "created_by": "BasicDatasetProfiler",
    #             "created_at": 0,
    #             "batch_kwargs": {},
    #         },
    #     },
    #     "expectations": [
    #         {
    #             "expectation_type": "expect_column_to_exist",
    #             "meta": {
    #                 "BasicDatasetProfiler": {
    #                     "confidence": "very low"
    #                 }
    #             }
    #         }]
    # }


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


# FIXME: This test needs a different home.
# def test_validate_on_a_context_loaded_batch(empty_data_context, filesystem_csv_2):
#     toy_dataset = PandasDataset({"x": [1, 2, 3]})
#     toy_dataset.validate()

#     empty_data_context.add_datasource(
#         "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
#     not_so_empty_data_context = empty_data_context

#     # my_ds = not_so_empty_data_context.get_datasource("my_datasource")
#     # print(my_ds.list_available_data_asset_names())

#     my_batch = not_so_empty_data_context.get_batch("my_datasource", "f1")
#     my_batch.validate()
