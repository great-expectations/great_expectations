from __future__ import annotations

import copy
import logging
import pathlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Tuple, cast

import pytest

import great_expectations.exceptions as ge_exceptions
import great_expectations.expectations as gxe
from great_expectations.alias_types import PathStr
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.pyspark import types as pyspark_types
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
    FileNamePartitionerMonthly,
    FileNamePartitionerYearly,
)
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.csv_asset import (
    CSVAsset,
    DirectoryCSVAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.delta_asset import (
    DeltaAsset,
    DirectoryDeltaAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.json_asset import (
    DirectoryJSONAsset,
    JSONAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.orc_asset import (
    DirectoryORCAsset,
    ORCAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.parquet_asset import (
    DirectoryParquetAsset,
    ParquetAsset,
)
from great_expectations.datasource.fluent.data_asset.path.spark.text_asset import (
    DirectoryTextAsset,
    TextAsset,
)
from great_expectations.datasource.fluent.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    _SparkFilePathDatasource,
)
from great_expectations.datasource.fluent.spark_filesystem_datasource import (
    SparkFilesystemDatasource,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import BatchSlice


logger = logging.getLogger(__name__)


@pytest.fixture
def spark_filesystem_datasource(empty_data_context, test_backends) -> SparkFilesystemDatasource:
    base_directory_rel_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    base_directory_abs_path = (
        pathlib.Path(__file__).parent.joinpath(base_directory_rel_path).resolve(strict=True)
    )
    spark_filesystem_datasource = SparkFilesystemDatasource(
        name="spark_filesystem_datasource",
        base_directory=base_directory_abs_path,
    )
    spark_filesystem_datasource._data_context = empty_data_context

    # Verify test directory has files we expect
    years = ["2018", "2019", "2020"]
    file_name: PathStr
    all_files: List[str] = [
        file_name.stem
        for file_name in list(pathlib.Path(spark_filesystem_datasource.base_directory).iterdir())
    ]
    # assert there are 12 files for each year
    for year in years:
        files_for_year = [
            file_name
            for file_name in all_files
            if file_name.find(f"yellow_tripdata_sample_{year}") == 0
        ]
        assert len(files_for_year) == 12

    return spark_filesystem_datasource


@pytest.mark.spark
def test_add_csv_asset_to_datasource(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    assert asset.name == "csv_asset"


if pyspark_types:
    spark_schema = pyspark_types.StructType(
        [pyspark_types.StructField("f1", pyspark_types.StringType(), True)]
    )
    additional_params = {"spark_schema": spark_schema}
else:
    additional_params = {}

add_csv_asset_all_params = {
    "sep": "sep",
    "encoding": "encoding",
    "quote": "quote",
    "escape": "escape",
    "comment": "comment",
    "header": "header",
    "infer_schema": "infer_schema",
    "ignore_leading_white_space": "ignore_leading_white_space",
    "ignore_trailing_white_space": "ignore_trailing_white_space",
    "null_value": "null_value",
    "nan_value": "nan_value",
    "positive_inf": "positive_inf",
    "negative_inf": "negative_inf",
    "date_format": "date_format",
    "timestamp_format": "timestamp_format",
    "max_columns": "max_columns",
    "max_chars_per_column": "max_chars_per_column",
    "max_malformed_log_per_partition": "max_malformed_log_per_partition",
    "mode": "PERMISSIVE",
    "column_name_of_corrupt_record": "column_name_of_corrupt_record",
    "multi_line": "multi_line",
    "char_to_escape_quote_escaping": "char_to_escape_quote_escaping",
    "sampling_ratio": "sampling_ratio",
    "enforce_schema": "enforce_schema",
    "empty_value": "empty_value",
    "locale": "locale",
    "line_sep": "line_sep",
    "path_glob_filter": "path_glob_filter",
    "recursive_file_lookup": "recursive_file_lookup",
    "modified_before": "modified_before",
    "modified_after": "modified_after",
    "unescaped_quote_handling": "STOP_AT_CLOSING_QUOTE",
}

add_csv_asset = [
    pytest.param(
        "add_csv_asset",
        {},
        id="csv_min_params",
    ),
    pytest.param(
        "add_csv_asset",
        {
            **add_csv_asset_all_params,
            **additional_params,
        },
        id="csv_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_csv_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="csv_fail_extra_params",
    ),
]

add_parquet_asset_all_params = {
    "merge_schema": "merge_schema",
    "datetime_rebase_mode": "EXCEPTION",
    "int_96_rebase_mode": "EXCEPTION",
    "path_glob_filter": "path_glob_filter",
    "recursive_file_lookup": "recursive_file_lookup",
    "modified_before": "modified_before",
    "modified_after": "modified_after",
}

add_parquet_asset = [
    pytest.param(
        "add_parquet_asset",
        {},
        id="parquet_min_params",
    ),
    pytest.param(
        "add_parquet_asset",
        add_parquet_asset_all_params,
        id="parquet_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_parquet_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="parquet_fail_extra_params",
    ),
]

add_orc_asset_all_params = {
    "merge_schema": "merge_schema",
    "path_glob_filter": "path_glob_filter",
    "recursive_file_lookup": "recursive_file_lookup",
    "modified_before": "modified_before",
    "modified_after": "modified_after",
}

add_orc_asset = [
    pytest.param(
        "add_orc_asset",
        {},
        id="orc_min_params",
    ),
    pytest.param(
        "add_orc_asset",
        add_orc_asset_all_params,
        id="orc_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_orc_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="orc_fail_extra_params",
    ),
]

add_json_asset_all_params = {
    "primitives_as_string": "primitives_as_string",
    "prefers_decimal": "prefers_decimal",
    "allow_comments": "allow_comments",
    "allow_unquoted_field_names": "allow_unquoted_field_names",
    "allow_single_quotes": "allow_single_quotes",
    "allow_numeric_leading_zero": "allow_numeric_leading_zero",
    "allow_backslash_escaping_any_character": "allow_backslash_escaping_any_character",
    "mode": "PERMISSIVE",
    "column_name_of_corrupt_record": "column_name_of_corrupt_record",
    "date_format": "date_format",
    "timestamp_format": "timestamp_format",
    "multi_line": "multi_line",
    "allow_unquoted_control_chars": "allow_unquoted_control_chars",
    "line_sep": "line_sep",
    "sampling_ratio": "sampling_ratio",
    "drop_field_if_all_null": "drop_field_if_all_null",
    "encoding": "encoding",
    "locale": "locale",
    "path_glob_filter": "path_glob_filter",
    "recursive_file_lookup": "recursive_file_lookup",
    "modified_before": "modified_before",
    "modified_after": "modified_after",
    "allow_non_numeric_numbers": "allow_non_numeric_numbers",
}

add_json_asset = [
    pytest.param(
        "add_json_asset",
        {},
        id="json_min_params",
    ),
    pytest.param(
        "add_json_asset",
        {
            **add_json_asset_all_params,
            **additional_params,
        },
        id="json_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_json_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="json_fail_extra_params",
    ),
]

add_text_asset_all_params = {
    "wholetext": True,
    "line_sep": "line_sep",
    "path_glob_filter": "path_glob_filter",
    "recursive_file_lookup": "recursive_file_lookup",
    "modified_before": "modified_before",
    "modified_after": "modified_after",
}

add_text_asset = [
    pytest.param(
        "add_text_asset",
        {},
        id="text_min_params",
    ),
    pytest.param(
        "add_text_asset",
        add_text_asset_all_params,
        id="text_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_text_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="text_fail_extra_params",
    ),
]

add_delta_asset_all_params = {
    "timestamp_as_of": "timestamp_as_of",
    "version_as_of": "version_as_of",
}

add_delta_asset = [
    pytest.param(
        "add_delta_asset",
        {},
        id="delta_min_params",
    ),
    pytest.param(
        "add_delta_asset",
        add_delta_asset_all_params,
        id="delta_all_params_20230512",
    ),
    pytest.param(
        "add_delta_asset",
        {
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="delta_fail_extra_params",
    ),
]

add_asset_test_params = []
add_asset_test_params += add_csv_asset
add_asset_test_params += add_parquet_asset
add_asset_test_params += add_orc_asset
add_asset_test_params += add_json_asset
add_asset_test_params += add_text_asset
add_asset_test_params += add_delta_asset


@pytest.mark.unit
@pytest.mark.parametrize(
    "add_method_name,add_method_params",
    add_asset_test_params,
)
def test_add_asset_with_asset_specific_params(
    spark_filesystem_datasource: SparkFilesystemDatasource,
    add_method_name: str,
    add_method_params: dict,
):
    asset = getattr(spark_filesystem_datasource, add_method_name)(
        name="asset_name", **add_method_params
    )
    assert asset.name == "asset_name"
    for param, value in add_method_params.items():
        if param == "spark_schema":
            struct_type = cast(pyspark_types.StructType, value)
            assert getattr(asset, param) == struct_type.jsonValue()
        else:
            assert getattr(asset, param) == value


_SPARK_ASSET_TYPES = [
    (CSVAsset, {"name": "asset_name"}),
    (DirectoryCSVAsset, {"name": "asset_name", "data_directory": "data_directory"}),
    (ParquetAsset, {"name": "asset_name"}),
    (DirectoryParquetAsset, {"name": "asset_name", "data_directory": "data_directory"}),
    (ORCAsset, {"name": "asset_name"}),
    (DirectoryORCAsset, {"name": "asset_name", "data_directory": "data_directory"}),
    (JSONAsset, {"name": "asset_name"}),
    (DirectoryJSONAsset, {"name": "asset_name", "data_directory": "data_directory"}),
    (TextAsset, {"name": "asset_name"}),
    (DirectoryTextAsset, {"name": "asset_name", "data_directory": "data_directory"}),
    (DeltaAsset, {"name": "asset_name"}),
    (DirectoryDeltaAsset, {"name": "asset_name", "data_directory": "data_directory"}),
]


@pytest.mark.unit
def test_all_spark_file_path_asset_types_tested():
    """Make sure all the available assets are contained in the fixture used for other tests."""
    asset_types_in_fixture = {a[0] for a in _SPARK_ASSET_TYPES}
    assert asset_types_in_fixture == set(_SparkFilePathDatasource.asset_types)


@pytest.mark.unit
@pytest.mark.parametrize(
    "asset_type,required_fields",
    [
        pytest.param(
            asset_type,
            required_fields,
            id=asset_type.__name__,
        )
        for (asset_type, required_fields) in _SPARK_ASSET_TYPES
    ],
)
def test__get_reader_options_include(asset_type: PathDataAsset, required_fields: dict):
    """Make sure options are in fields."""
    fields = set(asset_type.__fields__.keys())
    asset = asset_type.validate(required_fields)
    for option in asset._get_reader_options_include():
        assert option in fields


add_directory_csv_asset = [
    pytest.param(
        "add_directory_csv_asset",
        {"data_directory": "some_directory"},
        id="directory_csv_min_params",
    ),
    pytest.param(
        "add_directory_csv_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_csv_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_csv_asset",
        {
            **add_csv_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
            **additional_params,
        },
        id="directory_csv_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_csv_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_csv_fail_extra_params",
    ),
]

add_directory_parquet_asset = [
    pytest.param(
        "add_directory_parquet_asset",
        {"data_directory": "some_directory"},
        id="directory_parquet_min_params",
    ),
    pytest.param(
        "add_directory_parquet_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_parquet_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_parquet_asset",
        {
            **add_parquet_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
        },
        id="directory_parquet_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_parquet_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_parquet_fail_extra_params",
    ),
]


add_directory_orc_asset = [
    pytest.param(
        "add_directory_orc_asset",
        {"data_directory": "some_directory"},
        id="directory_orc_min_params",
    ),
    pytest.param(
        "add_directory_orc_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_orc_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_orc_asset",
        {
            **add_orc_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
        },
        id="directory_orc_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_orc_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_orc_fail_extra_params",
    ),
]


add_directory_json_asset = [
    pytest.param(
        "add_directory_json_asset",
        {"data_directory": "some_directory"},
        id="directory_json_min_params",
    ),
    pytest.param(
        "add_directory_json_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_json_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_json_asset",
        {
            **add_json_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
            **additional_params,
        },
        id="directory_json_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_json_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_json_fail_extra_params",
    ),
]


add_directory_text_asset = [
    pytest.param(
        "add_directory_text_asset",
        {"data_directory": "some_directory"},
        id="directory_text_min_params",
    ),
    pytest.param(
        "add_directory_text_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_text_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_text_asset",
        {
            **add_text_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
        },
        id="directory_text_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_text_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_text_fail_extra_params",
    ),
]


add_directory_delta_asset = [
    pytest.param(
        "add_directory_delta_asset",
        {"data_directory": "some_directory"},
        id="directory_delta_min_params",
    ),
    pytest.param(
        "add_directory_delta_asset",
        {"data_directory": pathlib.Path("some_directory")},
        id="directory_delta_min_params_pathlib",
    ),
    pytest.param(
        "add_directory_delta_asset",
        {
            **add_delta_asset_all_params,
            **{
                "data_directory": "some_directory",
            },
        },
        id="directory_delta_all_params_pyspark_3_4_0",
    ),
    pytest.param(
        "add_directory_delta_asset",
        {
            "data_directory": "some_directory",
            "this_param_does_not_exist": "param_does_not_exist",
        },
        marks=pytest.mark.xfail(
            reason="param_does_not_exist",
            strict=True,
            raises=pydantic.ValidationError,
        ),
        id="directory_delta_fail_extra_params",
    ),
]

add_directory_asset_test_params = []
add_directory_asset_test_params += add_directory_csv_asset
add_directory_asset_test_params += add_directory_parquet_asset
add_directory_asset_test_params += add_directory_orc_asset
add_directory_asset_test_params += add_directory_json_asset
add_directory_asset_test_params += add_directory_text_asset
add_directory_asset_test_params += add_directory_delta_asset


@pytest.mark.unit
@pytest.mark.parametrize(
    "add_method_name,add_method_params",
    add_directory_asset_test_params,
)
def test_add_directory_asset_with_asset_specific_params(
    spark_filesystem_datasource: SparkFilesystemDatasource,
    add_method_name: str,
    add_method_params: dict,
):
    asset = getattr(spark_filesystem_datasource, add_method_name)(
        name="asset_name", **add_method_params
    )
    assert asset.name == "asset_name"
    for param, value in add_method_params.items():
        if param == "data_directory":
            assert getattr(asset, param) == pathlib.Path(str(value))
        elif param == "spark_schema":
            struct_type = cast(pyspark_types.StructType, value)
            assert getattr(asset, param) == struct_type.jsonValue()
        else:
            assert getattr(asset, param) == value


@pytest.mark.unit
def test_add_csv_asset_with_batching_regex_to_datasource(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
def test_construct_csv_asset_directly():
    asset = CSVAsset(
        name="csv_asset",
    )
    assert asset.name == "csv_asset"


@pytest.mark.unit
def test_csv_asset_with_batching_regex_named_parameters(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batching_regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="batch def", regex=batching_regex)
    options = asset.get_batch_parameters_keys(partitioner=batch_def.partitioner)
    assert options == ("path", "year", "month")


@pytest.mark.unit
def test_csv_asset_with_non_string_batching_regex_named_parameters(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    with pytest.raises(ge_exceptions.InvalidBatchRequestError):
        # year is an int which will raise an error
        asset.build_batch_request({"year": 2018, "month": "04"})


@pytest.mark.spark
@pytest.mark.parametrize(
    "path",
    [
        pytest.param("samples_2020", id="str"),
        pytest.param(pathlib.Path("samples_2020"), id="pathlib.Path"),
    ],
)
def test_get_batch_identifiers_list_from_directory_one_batch(
    path: PathStr,
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    """What does this test and why?

    A "directory" asset should only have a single batch."""
    asset = spark_filesystem_datasource.add_directory_csv_asset(
        name="csv_asset",
        data_directory=path,
        header=True,
        infer_schema=True,
    )
    batch_def = asset.add_batch_definition_whole_directory(name="test batch def")
    request = batch_def.build_batch_request()
    batches = asset.get_batch_identifiers_list(request)
    assert len(batches) == 1


@pytest.mark.spark
@pytest.mark.parametrize(
    "path",
    [
        pytest.param("samples_2020", id="str"),
        pytest.param(pathlib.Path("samples_2020"), id="pathlib.Path"),
    ],
)
def test_get_batch_list_from_directory_merges_files(
    path: PathStr,
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    """What does this test and why?

    Adding a "directory" asset should only add a single batch merging all files into one dataframe.

    Marked as an integration test since this uses the execution engine to actually load the files.
    """
    asset = spark_filesystem_datasource.add_directory_csv_asset(
        name="csv_asset",
        data_directory=path,
        header=True,
        infer_schema=True,
    )
    batch_def = asset.add_batch_definition_whole_directory("test batch def")
    request = batch_def.build_batch_request()
    batch = asset.get_batch(request)
    batch_data = batch.data
    # The directory contains 12 files with 10,000 records each so the batch data
    # (spark dataframe) should contain 120,000 records:
    assert batch_data.dataframe.count() == 12 * 10000  # type: ignore[attr-defined]


@pytest.mark.spark
def test_get_batch_list_from_fully_specified_batch_request(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batch_def = asset.add_batch_definition_monthly(
        name="Fully Specified Batch Test",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch = batch_def.get_batch(batch_parameters={"year": "2018", "month": "04"})
    assert batch.batch_request.datasource_name == spark_filesystem_datasource.name
    assert batch.batch_request.data_asset_name == asset.name

    path = "yellow_tripdata_sample_2018-04.csv"
    assert batch.batch_request.options == {"path": path, "year": "2018", "month": "04"}
    assert batch.metadata == {"path": path, "year": "2018", "month": "04"}

    assert batch.id == "spark_filesystem_datasource-csv_asset-year_2018-month_04"


@pytest.mark.spark
def test_get_batch_list_from_partially_specified_batch_request(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    # Verify test directory has files that don't match what we will query for
    file_name: PathStr
    all_files: List[str] = [
        file_name.stem
        for file_name in list(pathlib.Path(spark_filesystem_datasource.base_directory).iterdir())
    ]
    # assert there are files that are not csv files
    assert any(not file_name.endswith("csv") for file_name in all_files)
    # assert there are 12 files from 2018
    files_for_2018 = [file_name for file_name in all_files if file_name.find("2018") >= 0]
    assert len(files_for_2018) == 12

    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    batch_def = asset.add_batch_definition_monthly(
        name="Partially Specified Batch Test",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )

    request = batch_def.build_batch_request(batch_parameters={"year": "2018"})

    batch_identifiers_list = asset.get_batch_identifiers_list(request)
    assert (len(batch_identifiers_list)) == 12
    batch_filenames = [pathlib.Path(batch["path"]).stem for batch in batch_identifiers_list]
    assert set(files_for_2018) == set(batch_filenames)

    @dataclass(frozen=True)
    class YearMonth:
        year: str
        month: str

    expected_year_month = {YearMonth(year="2018", month=format(m, "02d")) for m in range(1, 13)}
    batch_year_month = {
        YearMonth(year=batch["year"], month=batch["month"]) for batch in batch_identifiers_list
    }
    assert expected_year_month == batch_year_month


@pytest.mark.spark
@pytest.mark.parametrize(
    "sort_ascending",
    [pytest.param(True), pytest.param(False)],
)
def test_spark_sorter(spark_filesystem_datasource: SparkFilesystemDatasource, sort_ascending: bool):
    # arrange
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    regex = re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
    batch_def = asset.add_batch_definition_monthly(
        name="test-batch-def", regex=regex, sort_ascending=sort_ascending
    )
    batch_request = batch_def.build_batch_request()

    # act
    batches = asset.get_batch_identifiers_list(batch_request)

    # assert
    assert (len(batches)) == 36

    reverse = sort_ascending is False
    sorted_batches = sorted(
        batches,
        key=lambda batch: (batch.get("year"), batch.get("month")),
        reverse=reverse,
    )
    assert sorted_batches == batches


@pytest.mark.spark
@pytest.mark.parametrize(
    "batch_slice, expected_batch_count",
    [
        ("[-3:]", 3),
        ("[5:9]", 4),
        ("[:10:2]", 5),
        (slice(-3, None), 3),
        (slice(5, 9), 4),
        (slice(0, 10, 2), 5),
        ("-5", 1),
        ("-1", 1),
        (11, 1),
        (0, 1),
        ([3], 1),
        (None, 12),
        ("", 12),
    ],
)
def test_spark_slice_batch_count(
    spark_filesystem_datasource: SparkFilesystemDatasource,
    batch_slice: BatchSlice,
    expected_batch_count: int,
) -> None:
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
    )
    batching_regex = re.compile(r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
    batch_request = asset.build_batch_request(
        options={"year": "2019"},
        batch_slice=batch_slice,
        partitioner=FileNamePartitionerMonthly(regex=batching_regex),
    )
    batches = asset.get_batch_identifiers_list(batch_request=batch_request)
    assert len(batches) == expected_batch_count


def bad_batching_regex_config(
    csv_path: pathlib.Path,
) -> tuple[re.Pattern, TestConnectionError]:
    batching_regex = re.compile(r"green_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv")
    test_connection_error = TestConnectionError(
        f"""No file at base_directory path "{csv_path.resolve()}" matched regular expressions pattern "{batching_regex.pattern}" and/or glob_directive "**/*" for DataAsset "csv_asset"."""  # noqa: E501
    )
    return batching_regex, test_connection_error


@pytest.fixture(params=[bad_batching_regex_config])
def datasource_test_connection_error_messages(
    csv_path: pathlib.Path,
    spark_filesystem_datasource: SparkFilesystemDatasource,
    request,
) -> tuple[SparkFilesystemDatasource, TestConnectionError]:
    _, test_connection_error = request.param(csv_path=csv_path)
    csv_asset = CSVAsset(  # type: ignore[call-arg] # missing args
        name="csv_asset",
    )
    csv_asset._datasource = spark_filesystem_datasource
    spark_filesystem_datasource.assets = [
        csv_asset,
    ]
    csv_asset._data_connector = FilesystemDataConnector(
        datasource_name=spark_filesystem_datasource.name,
        data_asset_name=csv_asset.name,
        base_directory=spark_filesystem_datasource.base_directory,
        data_context_root_directory=spark_filesystem_datasource.data_context_root_directory,
    )
    csv_asset._test_connection_error_message = test_connection_error
    return spark_filesystem_datasource, test_connection_error


@pytest.mark.spark
def test_get_batch_identifiers_list_does_not_modify_input_batch_request(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="Test Batch Definition", regex=regex)
    request = batch_def.build_batch_request({"year": "2018"})
    request_before_call = copy.deepcopy(request)
    batches = asset.get_batch_identifiers_list(request)
    # We assert the request before the call to get_batch_identifiers_list is equal to the request after the  # noqa: E501
    # call. This test exists because this call was modifying the request.
    assert request == request_before_call
    # We get all 12 batches, one for each month of 2018.
    assert len(batches) == 12


@pytest.mark.spark
def test_get_batch_does_not_modify_input_batch_request(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
    )
    regex = r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
    batch_def = asset.add_batch_definition_monthly(name="Test Batch Definition", regex=regex)
    request = batch_def.build_batch_request({"year": "2018"})
    request_before_call = copy.deepcopy(request)
    _ = asset.get_batch(request)
    # We assert the request before the call to get_batch is equal to the request after the  # noqa: E501
    # call. This test exists because this call was modifying the request.
    assert request == request_before_call


@pytest.mark.spark
def test_add_csv_asset_with_batch_metadata(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    asset_specified_metadata = {"asset_level_metadata": "my_metadata"}
    asset = spark_filesystem_datasource.add_csv_asset(
        name="csv_asset",
        header=True,
        infer_schema=True,
        batch_metadata=asset_specified_metadata,
    )
    batch_def = asset.add_batch_definition_monthly(
        name="Test Batch Metadata",
        regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
    )
    batch_parameters = {"year": "2018", "month": "05"}

    request = batch_def.build_batch_request(batch_parameters)
    batch = asset.get_batch(request)
    assert batch.metadata == {
        "path": "yellow_tripdata_sample_2018-05.csv",
        **batch_parameters,
        **asset_specified_metadata,
    }


@pytest.fixture
def directory_asset_with_no_partitioner(
    spark_filesystem_datasource: SparkFilesystemDatasource,
) -> DirectoryCSVAsset:
    asset = spark_filesystem_datasource.add_directory_csv_asset(
        name="directory_csv_asset_no_partitioner",
        data_directory="first_ten_trips_in_each_file",
        header=True,
        infer_schema=True,
    )
    return asset


@pytest.fixture
def expected_num_records_directory_asset_no_partitioner_2020_passenger_count_2(
    directory_asset_with_no_partitioner: DirectoryCSVAsset,
) -> int:
    pre_partitioner_batch = directory_asset_with_no_partitioner.get_batch(
        directory_asset_with_no_partitioner.build_batch_request()
    )
    pre_partitioner_batch_data = pre_partitioner_batch.data
    expected_num_records = pre_partitioner_batch_data.dataframe.filter(  # type: ignore[attr-defined]
        F.col("pickup_datetime").contains("2018-01-11")
    ).count()
    assert expected_num_records == 3, "Check that the referenced data hasn't changed"
    return expected_num_records


@pytest.fixture
def directory_asset(
    spark_filesystem_datasource: SparkFilesystemDatasource,
) -> DirectoryCSVAsset:
    asset = spark_filesystem_datasource.add_directory_csv_asset(
        name="directory_csv_asset_with_partitioner",
        data_directory="first_ten_trips_in_each_file",
        header=True,
        infer_schema=True,
    )
    return asset


@pytest.fixture
def daily_partitioner():
    return ColumnPartitionerDaily(column_name="pickup_datetime")


@pytest.fixture
def daily_batch_parameters_and_expected_result() -> Tuple[Dict[str, int], int]:
    batch_parameters = {"year": 2018, "month": 1, "day": 11}
    expected_result = 3
    return batch_parameters, expected_result


@pytest.fixture
def monthly_batch_parameters_and_expected_result() -> Tuple[Dict[str, int], int]:
    batch_parameters = {"year": 2018, "month": 1}
    expected_result = 10
    return batch_parameters, expected_result


@pytest.fixture
def yearly_batch_parameters_and_expected_result() -> Tuple[Dict[str, int], int]:
    batch_parameters = {"year": 2018}
    expected_result = 120
    return batch_parameters, expected_result


@pytest.fixture
def whole_directory_batch_parameters_and_expected_result() -> Tuple[Dict[str, int], int]:
    batch_parameters: Dict[str, int] = {}
    expected_result = 360
    return batch_parameters, expected_result


class TestPartitionerDirectoryAsset:
    @pytest.mark.spark
    def test_daily_batch_definition_workflow(
        self,
        directory_asset: DirectoryCSVAsset,
    ):
        batch_parameters = {"year": 2018, "month": 1, "day": 11}
        expected_result = 3
        batch_def = directory_asset.add_batch_definition_daily(
            name="daily", column="pickup_datetime"
        )
        batch = batch_def.get_batch(batch_parameters=batch_parameters)
        assert batch.validate(gxe.ExpectTableRowCountToEqual(value=expected_result)).success

    @pytest.mark.spark
    def test_monthly_batch_definition_workflow(
        self,
        directory_asset: DirectoryCSVAsset,
    ):
        batch_parameters = {"year": 2018, "month": 1}
        expected_result = 10
        batch_def = directory_asset.add_batch_definition_monthly(
            name="monthly", column="pickup_datetime"
        )
        batch = batch_def.get_batch(batch_parameters=batch_parameters)
        assert batch.validate(gxe.ExpectTableRowCountToEqual(value=expected_result)).success

    @pytest.mark.spark
    def test_yearly_batch_definition_workflow(
        self,
        directory_asset: DirectoryCSVAsset,
        yearly_batch_parameters_and_expected_result: Tuple[Dict[str, int], int],
    ):
        batch_parameters = {"year": 2018}
        expected_result = 120
        batch_def = directory_asset.add_batch_definition_yearly(
            name="yearly", column="pickup_datetime"
        )
        batch = batch_def.get_batch(batch_parameters=batch_parameters)
        assert batch.validate(gxe.ExpectTableRowCountToEqual(value=expected_result)).success

    @pytest.mark.spark
    def test_whole_table_batch_definition_workflow(
        self,
        directory_asset: DirectoryCSVAsset,
        whole_directory_batch_parameters_and_expected_result: Tuple[Dict[str, int], int],
    ):
        batch_parameters: Dict[str, int] = {}
        expected_result = 360
        batch_def = directory_asset.add_batch_definition_whole_directory(name="whole directory")
        batch = batch_def.get_batch(batch_parameters=batch_parameters)
        assert batch.validate(gxe.ExpectTableRowCountToEqual(value=expected_result)).success

    @pytest.mark.spark
    @pytest.mark.parametrize(
        "partitioner,expected_keys",
        [
            pytest.param(
                ColumnPartitionerDaily(column_name="foo"),
                ("path", "year", "month", "day"),
                id="Daily Partitioner",
            ),
            pytest.param(
                ColumnPartitionerMonthly(column_name="foo"),
                (
                    "path",
                    "year",
                    "month",
                ),
                id="Monthly Partitioner",
            ),
            pytest.param(
                ColumnPartitionerYearly(column_name="foo"),
                (
                    "path",
                    "year",
                ),
                id="Yearly Partitioner",
            ),
            pytest.param(None, ("path",), id="No Partitioner"),
        ],
    )
    def test_get_batch_parameters_keys_with_partitioner(
        self,
        directory_asset: DirectoryCSVAsset,
        partitioner: ColumnPartitioner,
        expected_keys: Tuple[str, ...],
    ):
        assert directory_asset.get_batch_parameters_keys(partitioner=partitioner) == expected_keys


@pytest.fixture
def file_asset_with_no_partitioner(
    spark_filesystem_datasource: SparkFilesystemDatasource,
) -> CSVAsset:
    asset = spark_filesystem_datasource.add_csv_asset(
        name="file_csv_asset_no_partitioner",
        header=True,
        infer_schema=True,
    )
    return asset


@pytest.fixture
def expected_num_records_file_asset_no_partitioner_2020_10_passenger_count_2(
    file_asset_with_no_partitioner: CSVAsset,
) -> int:
    single_batch_batch_request = file_asset_with_no_partitioner.build_batch_request(
        {"year": "2020", "month": "11"}
    )
    batch = file_asset_with_no_partitioner.get_batch(single_batch_batch_request)
    pre_partitioner_batch_data = batch.data
    expected_num_records = pre_partitioner_batch_data.dataframe.filter(  # type: ignore[attr-defined]
        F.col("passenger_count") == 2
    ).count()
    assert expected_num_records == 2, "Check that the referenced data hasn't changed"
    return expected_num_records


@pytest.fixture
def expected_num_records_file_asset_no_partitioner_2020_10(
    file_asset_with_no_partitioner: CSVAsset,
) -> int:
    single_batch_batch_request = file_asset_with_no_partitioner.build_batch_request(
        {"year": "2020", "month": "11"},
        partitioner=FileNamePartitionerMonthly(
            regex=re.compile(
                r"first_ten_trips_in_each_file/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
            )
        ),
    )
    batch = file_asset_with_no_partitioner.get_batch(single_batch_batch_request)

    pre_partitioner_batch_data = batch.data

    expected_num_records = (
        pre_partitioner_batch_data.dataframe.filter(  # type: ignore[attr-defined]
            F.year(F.col("pickup_datetime")) == 2020
        )
        .filter(F.month(F.col("pickup_datetime")) == 11)
        .count()
    )
    assert expected_num_records == 10, "Check that the referenced data hasn't changed"
    return expected_num_records


@pytest.fixture
def file_asset(
    spark_filesystem_datasource: SparkFilesystemDatasource,
) -> CSVAsset:
    asset = spark_filesystem_datasource.add_csv_asset(
        name="file_csv_asset_with_partitioner",
        header=True,
        infer_schema=True,
    )
    return asset


class TestPartitionerFileAsset:
    @pytest.mark.spark
    @pytest.mark.xfail(strict=True, reason="Will fix or refactor as part of V1-306")
    def test_parameter_keys_with_partitioner_file_asset_batch_parameters(
        self, file_asset, daily_partitioner
    ):
        assert file_asset.get_batch_parameters_keys(partitioner=daily_partitioner) == (
            "year",
            "month",
            "path",
            "passenger_count",
        )

    @pytest.mark.spark
    @pytest.mark.xfail(strict=True, reason="Will fix or refactor as part of V1-306")
    def test_get_batch_with_partitioner_file_asset_one_batch_size(
        self,
        file_asset,
        daily_partitioner,
        expected_num_records_file_asset_no_partitioner_2020_10_passenger_count_2: int,
    ):
        post_partitioner_batch_request = file_asset.build_batch_request(
            options={"year": "2020", "month": "11", "passenger_count": 2},
            partitioner=daily_partitioner,
        )
        post_partitioner_batch_list = file_asset.get_batch(post_partitioner_batch_request)

        # Make sure we only have passenger_count == 2 in our batch data
        post_partitioner_batch_data = post_partitioner_batch_list.data

        assert (
            post_partitioner_batch_data.dataframe.filter(F.col("passenger_count") == 2).count()
            == expected_num_records_file_asset_no_partitioner_2020_10_passenger_count_2
        )
        assert (
            post_partitioner_batch_data.dataframe.filter(F.col("passenger_count") != 2).count() == 0
        )

    @pytest.mark.spark
    @pytest.mark.xfail(strict=True, reason="Will fix or refactor as part of V1-306")
    def test_add_file_csv_asset_with_partitioner_conflicting_identifier_batch_parameters(
        self, file_asset_with_no_partitioner: CSVAsset
    ):
        regex = re.compile(
            r"first_ten_trips_in_each_file/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
        )
        asset_with_conflicting_partitioner = file_asset_with_no_partitioner
        partitioner = FileNamePartitionerYearly(regex=regex)
        assert asset_with_conflicting_partitioner.get_batch_parameters_keys(
            partitioner=partitioner
        ) == (
            "year",
            "month",
            "path",
        )

    @pytest.mark.spark
    def test_add_file_csv_asset_with_partitioner_conflicting_identifier_gets_a_batch(
        self, file_asset_with_no_partitioner: CSVAsset
    ):
        regex = re.compile(
            r"first_ten_trips_in_each_file/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
        )

        asset = file_asset_with_no_partitioner
        partitioner = FileNamePartitionerMonthly(regex=regex)

        post_partitioner_batch_request = asset.build_batch_request(
            options={"year": "2020", "month": "11"}, partitioner=partitioner
        )
        asset.get_batch(post_partitioner_batch_request)
        # no errors!

    @pytest.mark.spark
    def test_add_file_csv_asset_with_partitioner_conflicting_identifier_gets_correct_data(
        self,
        file_asset_with_no_partitioner: CSVAsset,
        expected_num_records_file_asset_no_partitioner_2020_10: int,
    ):
        regex = re.compile(
            r"first_ten_trips_in_each_file/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"
        )
        asset = file_asset_with_no_partitioner
        partitioner = FileNamePartitionerMonthly(regex=regex)

        post_partitioner_batch_request = asset.build_batch_request(
            options={"year": "2020", "month": "11"}, partitioner=partitioner
        )
        post_partitioner_batch = asset.get_batch(post_partitioner_batch_request)
        post_partitioner_batch_data = post_partitioner_batch.data

        assert (
            post_partitioner_batch_data.dataframe.count()  # type: ignore[attr-defined]
            == expected_num_records_file_asset_no_partitioner_2020_10
        )
