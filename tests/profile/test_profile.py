import pytest

import json
from collections import OrderedDict

from great_expectations.profile.base import DatasetProfiler
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.columns_exist import ColumnsExistProfiler
from great_expectations.dataset.pandas_dataset import PandasDataset
import great_expectations as ge
from ..test_utils import assertDeepAlmostEqual
from six import PY2

# Tests to write:
# test_cli_method_works  -> test_cli
# test context-based profile methods
# test class-based profile methods


# noinspection PyPep8Naming
def test_DataSetProfiler_methods():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    assert DatasetProfiler.validate(1) == False
    assert DatasetProfiler.validate(toy_dataset)

    with pytest.raises(NotImplementedError) as e_info:
        DatasetProfiler.profile(toy_dataset)


# noinspection PyPep8Naming
def test_ColumnsExistProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]})

    expectations_config, evr_config = ColumnsExistProfiler.profile(toy_dataset)

    assert len(expectations_config["expectations"]) == 1
    assert expectations_config["expectations"][0]["expectation_type"] == "expect_column_to_exist"
    assert expectations_config["expectations"][0]["kwargs"]["column"] == "x"


# noinspection PyPep8Naming
def test_BasicDatasetProfiler():
    toy_dataset = PandasDataset({"x": [1, 2, 3]}, data_asset_name="toy_dataset")
    assert len(toy_dataset.get_expectation_suite(
        suppress_warnings=True)["expectations"]) == 0

    expectations_config, evr_config = BasicDatasetProfiler.profile(toy_dataset)

    # print(json.dumps(expectations_config, indent=2))

    assert len(toy_dataset.get_expectation_suite(
        suppress_warnings=True)["expectations"]) > 0

    assert expectations_config["data_asset_name"] == "toy_dataset"
    assert "BasicDatasetProfiler" in expectations_config["meta"]

    assert set(expectations_config["meta"]["BasicDatasetProfiler"].keys()) == {
        "created_by", "created_at"
    }

    assert "notes" in expectations_config["meta"]
    assert set(expectations_config["meta"]["notes"].keys()) == {"format", "content"}
    assert "To add additional notes" in expectations_config["meta"]["notes"]["content"][0]

    added_expectations = set()
    for exp in expectations_config["expectations"]:
        added_expectations.add(exp["expectation_type"])
        assert "BasicDatasetProfiler" in exp["meta"]
        assert "confidence" in exp["meta"]["BasicDatasetProfiler"]

    expected_expectations = {
        'expect_table_row_count_to_be_between',
        'expect_table_columns_to_match_ordered_list',
        'expect_column_values_to_be_in_set',
        'expect_column_unique_value_count_to_be_between',
        'expect_column_proportion_of_unique_values_to_be_between',
        'expect_column_values_to_not_be_null',
        'expect_column_values_to_be_in_type_list',
        'expect_column_values_to_be_unique'}

    assert expected_expectations.issubset(added_expectations)


def test_BasicDatasetProfiler_null_column():
    """
    The profiler should determine that null columns are of null cardinality and of null type and
    not to generate expectations specific to types and cardinality categories.

    We verify this by running the basic profiler on a Pandas dataset with an empty column
    and asserting the number of successful results for the empty columns.
    """
    toy_dataset = PandasDataset({"x": [1, 2, 3], "y": [None, None, None]}, data_asset_name="toy_dataset")
    assert len(toy_dataset.get_expectation_suite(
        suppress_warnings=True)["expectations"]) == 0

    expectations_config, evr_config = BasicDatasetProfiler.profile(toy_dataset)

    # TODO: assert set - specific expectations
    assert len([result for result in evr_config['results'] if
     result['expectation_config']['kwargs'].get('column') == 'y' and result['success']]) == 4


    assert len([result for result in evr_config['results'] if
                result['expectation_config']['kwargs'].get('column') == 'y' and result['success']]) < \
           len([result for result in evr_config['results'] if
                result['expectation_config']['kwargs'].get('column') == 'x' and result['success']])


def test_BasicDatasetProfiler_partially_null_column(dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a partially null column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.

    "nulls" is the partially null column in the fixture dataset
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(dataset)

    assert set(["expect_column_to_exist", "expect_column_values_to_be_in_type_list", "expect_column_unique_value_count_to_be_between", "expect_column_proportion_of_unique_values_to_be_between", "expect_column_values_to_not_be_null", "expect_column_values_to_be_in_set", "expect_column_values_to_be_unique"]) == \
           set([expectation['expectation_type'] for expectation in expectations_config["expectations"] if expectation["kwargs"].get("column") == "nulls"])

def test_BasicDatasetProfiler_non_numeric_low_cardinality(non_numeric_low_card_dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a low cardinality
    non numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(non_numeric_low_card_dataset)

    assert set(["expect_column_to_exist", "expect_column_values_to_be_in_type_list", "expect_column_unique_value_count_to_be_between", "expect_column_proportion_of_unique_values_to_be_between", "expect_column_values_to_not_be_null", "expect_column_values_to_be_in_set", "expect_column_values_to_not_match_regex"]) == \
           set([expectation['expectation_type'] for expectation in expectations_config["expectations"] if expectation["kwargs"].get("column") == "lowcardnonnum"])

def test_BasicDatasetProfiler_non_numeric_high_cardinality(non_numeric_high_card_dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a high cardinality
    non numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(non_numeric_high_card_dataset)

    assert set(["expect_column_to_exist", "expect_column_values_to_be_in_type_list", "expect_column_unique_value_count_to_be_between", "expect_column_proportion_of_unique_values_to_be_between", "expect_column_values_to_not_be_null", "expect_column_values_to_be_in_set", "expect_column_values_to_not_match_regex"]) == \
           set([expectation['expectation_type'] for expectation in expectations_config["expectations"] if expectation["kwargs"].get("column") == "highcardnonnum"])

def test_BasicDatasetProfiler_numeric_high_cardinality(numeric_high_card_dataset):
    """
    Unit test to check the expectations that BasicDatasetProfiler creates for a high cardinality
    numeric column.
    The test is executed against all the backends (Pandas, Spark, etc.), because it uses
    the fixture.
    """
    expectations_config, evr_config = BasicDatasetProfiler.profile(numeric_high_card_dataset)

    assert set(["expect_column_to_exist", "expect_table_row_count_to_be_between", "expect_table_columns_to_match_ordered_list", "expect_column_values_to_be_in_type_list", "expect_column_unique_value_count_to_be_between", "expect_column_proportion_of_unique_values_to_be_between", "expect_column_values_to_not_be_null", "expect_column_values_to_be_in_set", "expect_column_values_to_be_unique"]) == set([expectation['expectation_type'] for expectation in expectations_config["expectations"]])


# noinspection PyPep8Naming
def test_BasicDatasetProfiler_with_context(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    not_so_empty_data_context.create_expectation_suite("my_datasource/f1", "default")
    batch_kwargs = not_so_empty_data_context.yield_batch_kwargs("my_datasource/f1")
    batch = not_so_empty_data_context.get_batch("my_datasource/f1", "default", batch_kwargs)
    expectations_config, validation_results = BasicDatasetProfiler.profile(
        batch)

    # print(batch.get_batch_kwargs())
    # print(json.dumps(expectations_config, indent=2))

    assert expectations_config["data_asset_name"] == "my_datasource/default/f1"
    assert expectations_config["expectation_suite_name"] == "default"
    assert "BasicDatasetProfiler" in expectations_config["meta"]
    assert set(expectations_config["meta"]["BasicDatasetProfiler"].keys()) == {
        "created_by", "created_at", "batch_kwargs"
    }

    for exp in expectations_config["expectations"]:
        assert "BasicDatasetProfiler" in exp["meta"]
        assert "confidence" in exp["meta"]["BasicDatasetProfiler"]

    assert validation_results["meta"]["data_asset_name"] == "my_datasource/default/f1"
    assert set(validation_results["meta"].keys()) == {
        "great_expectations.__version__", "data_asset_name", "expectation_suite_name", "run_id", "batch_kwargs"
    }


# noinspection PyPep8Naming
def test_context_profiler(empty_data_context, filesystem_csv_2):
    """This just validates that it's possible to profile using the datasource hook, and have
    validation results available in the DataContext"""
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    assert not_so_empty_data_context.list_expectation_suite_keys() == []
    not_so_empty_data_context.profile_datasource("my_datasource", profiler=BasicDatasetProfiler)

    assert len(not_so_empty_data_context.list_expectation_suite_keys()) == 1

    profiled_expectations = not_so_empty_data_context.get_expectation_suite('f1', "BasicDatasetProfiler")

    print(json.dumps(profiled_expectations, indent=2))
    for exp in profiled_expectations["expectations"]:
        assert "BasicDatasetProfiler" in exp["meta"]
        assert "confidence" in exp["meta"]["BasicDatasetProfiler"]

    assert profiled_expectations["data_asset_name"] == "my_datasource/default/f1"
    assert profiled_expectations["expectation_suite_name"] == "BasicDatasetProfiler"
    assert "batch_kwargs" in profiled_expectations["meta"]["BasicDatasetProfiler"]

    assert len(profiled_expectations["expectations"]) > 0


# noinspection PyPep8Naming
def test_BasicDatasetProfiler_on_titanic():
    """
    A snapshot test for BasicDatasetProfiler.
    We are running the profiler on the Titanic dataset
    and comparing the EVRs to ones retrieved from a
    previously stored file.
    """
    df = ge.read_csv("./tests/test_sets/Titanic.csv")
    suite, evrs = df.profile(BasicDatasetProfiler)

    # Check to make sure BasicDatasetProfiler is adding meta.columns with a single "description" field for each column
    print(json.dumps(suite["meta"], indent=2))
    assert "columns" in suite["meta"]
    for k,v in suite["meta"]["columns"].items():
        assert v == {"description": ""}

    # Note: the above already produces an EVR; rerunning isn't strictly necessary just for EVRs
    evrs = df.validate(result_format="SUMMARY")  # ["results"]

    # with open('tests/test_sets/expected_evrs_BasicDatasetProfiler_on_titanic.json', 'w+') as file:
    #     file.write(json.dumps(evrs, indent=2))
    #
    # with open('tests/render/fixtures/BasicDatasetProfiler_evrs.json', 'w+') as file:
    #     file.write(json.dumps(evrs, indent=2))

    with open('tests/test_sets/expected_evrs_BasicDatasetProfiler_on_titanic.json', 'r') as file:
        expected_evrs = json.load(file, object_pairs_hook=OrderedDict)

    expected_evrs.pop("meta")
    evrs.pop("meta")

    # We know that python 2 does not guarantee the order of value_counts, which causes a different
    # order for items in the partial_unexpected_value_counts list
    # Remove those before test.
    for result in evrs["results"]:
        if "partial_unexpected_counts" in result["result"]:
            result["result"].pop("partial_unexpected_counts")

    for result in expected_evrs["results"]:
        if "partial_unexpected_counts" in result["result"]:
            result["result"].pop("partial_unexpected_counts")

    # DISABLE TEST IN PY2 BECAUSE OF ORDER ISSUE AND NEAR-EOL
    if not PY2:
        assertDeepAlmostEqual(expected_evrs, evrs)
