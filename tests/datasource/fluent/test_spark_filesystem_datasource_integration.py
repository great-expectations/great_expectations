
from __future__ import annotations

import copy
import logging
import pathlib
import re
from dataclasses import dataclass
from typing import List

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.alias_types import PathStr
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.datasource.fluent.interfaces import (
    SortersDefinition,
    TestConnectionError,
)
from great_expectations.datasource.fluent.spark_file_path_datasource import (
    CSVAsset,
)
from great_expectations.datasource.fluent.spark_filesystem_datasource import (
    SparkFilesystemDatasource,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def spark_filesystem_datasource(
    empty_data_context, test_backends
) -> SparkFilesystemDatasource:
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")

    base_directory_rel_path = pathlib.Path(
        "..", "..", "test_sets", "taxi_yellow_tripdata_samples"
    )
    base_directory_abs_path = (
        pathlib.Path(__file__)
        .parent.joinpath(base_directory_rel_path)
        .resolve(strict=True)
    )
    spark_filesystem_datasource = SparkFilesystemDatasource(
        name="spark_filesystem_datasource",
        base_directory=base_directory_abs_path,
    )
    spark_filesystem_datasource._data_context = empty_data_context
    return spark_filesystem_datasource

@pytest.mark.integration
def test_add_directory_csv_asset_with_splitter(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    source = spark_filesystem_datasource

    # TODO: Starting with integration test for PoC, but the filesystem should be mocked

    ############# First get one large batch using directory_csv_asset
    # 1. source.add_directory_csv_asset()
    asset = source.add_directory_csv_asset(
        name="directory_csv_asset",
        data_directory="samples_2020",
        header=True,
        infer_schema=True,
    )
    assert len(source.assets) == 1
    assert asset == source.assets[0]

    # Before applying the splitter, there should be one batch
    pre_splitter_batch_request = asset.build_batch_request()
    pre_splitter_batches = asset.get_batch_list_from_batch_request(
        pre_splitter_batch_request
    )
    pre_splitter_expected_num_batches = 1
    assert len(pre_splitter_batches) == pre_splitter_expected_num_batches

    # 3. Assert asset
    assert asset.name == "directory_csv_asset"
    assert asset.data_directory == pathlib.Path("samples_2020")
    assert asset.datasource == source
    assert asset.batch_request_options == ("path",)
    # 4. Assert batch request
    assert pre_splitter_batch_request.datasource_name == source.name
    assert pre_splitter_batch_request.data_asset_name == asset.name
    assert pre_splitter_batch_request.options == {}

    pre_splitter_batch_data = pre_splitter_batches[0].data
    # The directory contains 12 files with 10,000 records each so the batch data
    # (spark dataframe) should contain 120,000 records:
    assert pre_splitter_batch_data.dataframe.count() == 12 * 10000  # type: ignore[attr-defined]

    ############# Now add a splitter to the asset and get one batch (that should be only one month after the split)

    # TODO: For now I think we need to split on something other than year and month just to
    #  be certain that this is working as expected and that we aren't just getting the data from one month file
    #  This whole test is a PoC and should be cleaned up / parametrized.

    # Make sure our data is as expected
    passenger_counts = sorted(
        [
            pc.passenger_count
            for pc in pre_splitter_batch_data.dataframe.select("passenger_count")
            .dropna(subset=["passenger_count"])
            .distinct()
            .collect()
        ]
    )
    assert passenger_counts == [0, 1, 2, 3, 4, 5, 6]

    asset_with_passenger_count_splitter = asset.add_splitter_column_value(
        column_name="passenger_count"
    )
    assert asset_with_passenger_count_splitter.batch_request_options == (
        "path",
        "passenger_count",
    )
    post_passenger_count_splitter_batch_request = (
        asset_with_passenger_count_splitter.build_batch_request({"passenger_count": 2})
    )
    post_passenger_count_splitter_batch_list = (
        asset_with_passenger_count_splitter.get_batch_list_from_batch_request(
            post_passenger_count_splitter_batch_request
        )
    )
    post_splitter_expected_num_batches = 1
    # TODO: For some reason we are not getting a batch from post_splitter_batch_request
    # breakpoint()
    assert (
        len(post_passenger_count_splitter_batch_list)
        == post_splitter_expected_num_batches
    )

    ###################### TODO: replace the below with passenger_count

    # 2. asset.add_splitter_year_and_month()
    asset_with_splitter = asset.add_splitter_year_and_month(
        column_name="pickup_datetime"
    )
    # post_splitter_batch_request = asset.build_batch_request()
    # post_splitter_batch_request = asset.build_batch_request({"year": 2020})
    post_splitter_batch_request = asset_with_splitter.build_batch_request(
        {"year": 2020, "month": 10}
    )
    # TODO: How do I get all batches after applying splitter?
    #  Looks like we dont get all batches for spark / file based https://docs.greatexpectations.io/docs/0.15.50/guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_a_file_system_or_blob_store/

    # 3. Assert asset
    assert asset_with_splitter.name == "directory_csv_asset"
    assert asset_with_splitter.data_directory == pathlib.Path("samples_2020")
    assert asset_with_splitter.datasource == source
    assert asset_with_splitter.batch_request_options == ("path", "year", "month")
    # 4. Assert batch request
    assert post_splitter_batch_request.datasource_name == source.name
    assert post_splitter_batch_request.data_asset_name == asset_with_splitter.name
    assert post_splitter_batch_request.options == {"year": 2020, "month": 10}

    # 5. Assert num batches
    post_splitter_batches = asset_with_splitter.get_batch_list_from_batch_request(
        post_splitter_batch_request
    )
    post_splitter_expected_num_batches = 1
    # TODO: For some reason we are not getting a batch from post_splitter_batch_request
    # breakpoint()
    assert len(post_splitter_batches) == post_splitter_expected_num_batches

    # Make sure amount of data in batches is as expected
    post_splitter_batch_data = post_splitter_batches[0].data
    # The directory contains 12 files with 10,000 records each so the batch data
    # (spark dataframe) should contain 10,000 records after splitting by month:
    from great_expectations.compatibility.pyspark import (
        functions as F,
    )

    num_records_should_be = (
        pre_splitter_batch_data.dataframe.filter(
            F.year(F.col("pickup_datetime")) == 2020
        )
        .filter(F.month(F.col("pickup_datetime")) == 10)
        .count()
    )
    assert num_records_should_be == 10001

    assert post_splitter_batch_data.dataframe.count() == num_records_should_be  # type: ignore[attr-defined]



@pytest.mark.integration
def test_add_file_csv_asset_with_splitter(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    source = spark_filesystem_datasource

    # TODO: Starting with integration test for PoC, but the filesystem should be mocked

    ############# First get one large batch using directory_csv_asset
    # 1. source.add_directory_csv_asset()
    asset = source.add_csv_asset(
        name="file_csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        header=True,
        infer_schema=True,
    )
    assert len(source.assets) == 1
    assert asset == source.assets[0]

    # Before applying the splitter, there should be one batch
    # pre_splitter_batch_request = asset.build_batch_request()
    # pre_splitter_batches = asset.get_batch_list_from_batch_request(
    #     pre_splitter_batch_request
    # )
    # pre_splitter_expected_num_batches = 36
    # assert len(pre_splitter_batches) == pre_splitter_expected_num_batches

    # 3. Assert asset
    assert asset.name == "file_csv_asset"
    assert asset.datasource == source
    assert asset.batch_request_options == (
        "year",
        "month",
        "path",
    )
    # 4. Assert batch request
    # assert pre_splitter_batch_request.datasource_name == source.name
    # assert pre_splitter_batch_request.data_asset_name == asset.name
    # assert pre_splitter_batch_request.options == {}

    # pre_splitter_batch_data = pre_splitter_batches[0].data
    # # The directory contains 12 files with 10,000 records each so the batch data
    # # (spark dataframe) should contain 120,000 records:
    # assert pre_splitter_batch_data.dataframe.count() == 10000  # type: ignore[attr-defined]

    # breakpoint()
    single_batch_batch_request = asset.build_batch_request(
        {"year": "2020", "month": "10"}
    )
    single_batch_list = asset.get_batch_list_from_batch_request(
        single_batch_batch_request
    )
    assert len(single_batch_list) == 1

    pre_splitter_batch_data = single_batch_list[0].data

    ############# Now add a splitter to the asset and get one batch (that should be only one month after the split)

    # TODO: For now I think we need to split on something other than year and month just to
    #  be certain that this is working as expected and that we aren't just getting the data from one month file
    #  This whole test is a PoC and should be cleaned up / parametrized.

    # Make sure our data is as expected
    passenger_counts = sorted(
        [
            pc.passenger_count
            for pc in pre_splitter_batch_data.dataframe.select("passenger_count")
            .dropna(subset=["passenger_count"])
            .distinct()
            .collect()
        ]
    )
    assert passenger_counts == [0, 1, 2, 3, 4, 5, 6]

    asset_with_passenger_count_splitter = asset.add_splitter_column_value(
        column_name="passenger_count"
    )
    assert asset_with_passenger_count_splitter.batch_request_options == (
        "year",
        "month",
        "path",
        "passenger_count",
    )

    post_passenger_count_splitter_batch_request = (
        asset_with_passenger_count_splitter.build_batch_request(
            {"year": "2020", "month": "10", "passenger_count": 2}
        )
    )
    post_passenger_count_splitter_batch_list = (
        asset_with_passenger_count_splitter.get_batch_list_from_batch_request(
            post_passenger_count_splitter_batch_request
        )
    )
    post_splitter_expected_num_batches = 1
    # TODO: For some reason we are not getting a batch from post_splitter_batch_request
    assert (
        len(post_passenger_count_splitter_batch_list)
        == post_splitter_expected_num_batches
    )

    # Make sure we only have passenger_count == 2 in our batch data
    post_splitter_batch_data = post_passenger_count_splitter_batch_list[0].data
    from great_expectations.compatibility.pyspark import functions as F

    assert (
        post_splitter_batch_data.dataframe.filter(F.col("passenger_count") == 2).count()
        == 1258
    )
    assert (
        post_splitter_batch_data.dataframe.filter(F.col("passenger_count") != 2).count()
        == 0
    )

    ###################### TODO: does this work with conflicting splitter kwargs and regex (year and month)
    ###################### TODO: does this work with conflicting splitter kwargs and regex change to splitter on month only with a different month

    # breakpoint()
    # 2. asset.add_splitter_year_and_month()
    asset_with_splitter = asset.add_splitter_year_and_month(
        column_name="pickup_datetime"
    )
    # post_splitter_batch_request = asset.build_batch_request()
    # post_splitter_batch_request = asset.build_batch_request({"year": 2020})
    post_splitter_batch_request = asset_with_splitter.build_batch_request(
        {"year": "2020", "month": "10"}
    )
    # TODO: How do I get all batches after applying splitter?
    #  Looks like we dont get all batches for spark / file based https://docs.greatexpectations.io/docs/0.15.50/guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_a_file_system_or_blob_store/

    # 3. Assert asset
    assert asset_with_splitter.name == "file_csv_asset"
    assert asset_with_splitter.datasource == source
    assert asset_with_splitter.batch_request_options == (
        "year",
        "month",
        "path",
        "year",
        "month",
    )
    # 4. Assert batch request
    assert post_splitter_batch_request.datasource_name == source.name
    assert post_splitter_batch_request.data_asset_name == asset_with_splitter.name
    assert post_splitter_batch_request.options == {"year": "2020", "month": "10"}

    # 5. Assert num batches
    post_splitter_batches = asset_with_splitter.get_batch_list_from_batch_request(
        post_splitter_batch_request
    )
    post_splitter_expected_num_batches = 1
    # TODO: For some reason we are not getting a batch from post_splitter_batch_request
    # breakpoint()
    assert len(post_splitter_batches) == post_splitter_expected_num_batches

    # Make sure amount of data in batches is as expected
    post_splitter_batch_data = post_splitter_batches[0].data
    # The directory contains 12 files with 10,000 records each so the batch data
    # (spark dataframe) should contain 10,000 records after splitting by month:
    from great_expectations.compatibility.pyspark import (
        functions as F,
    )

    num_records_should_be = (
        pre_splitter_batch_data.dataframe.filter(
            F.year(F.col("pickup_datetime")) == 2020
        )
        .filter(F.month(F.col("pickup_datetime")) == 10)
        .count()
    )
    assert num_records_should_be == 10000

    assert post_splitter_batch_data.dataframe.count() == num_records_should_be  # type: ignore[attr-defined]



@pytest.mark.integration
def test_add_file_csv_asset_with_splitter_conflicting_identifier(
    spark_filesystem_datasource: SparkFilesystemDatasource,
):
    source = spark_filesystem_datasource

    # TODO: Starting with integration test for PoC, but the filesystem should be mocked

    ############# First get one large batch using directory_csv_asset
    # 1. source.add_directory_csv_asset()
    asset = source.add_csv_asset(
        name="file_csv_asset",
        batching_regex=r"yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv",
        header=True,
        infer_schema=True,
    )
    assert len(source.assets) == 1
    assert asset == source.assets[0]

    single_batch_batch_request = asset.build_batch_request(
        {"year": "2020", "month": "10"}
    )
    single_batch_list = asset.get_batch_list_from_batch_request(
        single_batch_batch_request
    )
    assert len(single_batch_list) == 1

    pre_splitter_batch_data = single_batch_list[0].data

    ###################### TODO: does this work with conflicting splitter kwargs and regex (year and month)
    ###################### TODO: does this work with conflicting splitter kwargs and regex change to splitter on month only with a different month

    # breakpoint()
    # 2. asset.add_splitter_year_and_month()
    asset_with_splitter = asset.add_splitter_year_and_month(
        column_name="pickup_datetime"
    )
    # post_splitter_batch_request = asset.build_batch_request()
    # post_splitter_batch_request = asset.build_batch_request({"year": 2020})
    post_splitter_batch_request = asset_with_splitter.build_batch_request(
        {"year": "2020", "month": "10"}
    )
    # TODO: How do I get all batches after applying splitter?
    #  Looks like we dont get all batches for spark / file based https://docs.greatexpectations.io/docs/0.15.50/guides/connecting_to_your_data/advanced/how_to_configure_a_dataconnector_for_splitting_and_sampling_a_file_system_or_blob_store/

    # 3. Assert asset
    assert asset_with_splitter.name == "file_csv_asset"
    assert asset_with_splitter.datasource == source
    assert asset_with_splitter.batch_request_options == (
        "year",
        "month",
        "path",
        "year",
        "month",
    )
    # 4. Assert batch request
    assert post_splitter_batch_request.datasource_name == source.name
    assert post_splitter_batch_request.data_asset_name == asset_with_splitter.name
    assert post_splitter_batch_request.options == {"year": "2020", "month": "10"}

    # 5. Assert num batches
    post_splitter_batches = asset_with_splitter.get_batch_list_from_batch_request(
        post_splitter_batch_request
    )
    post_splitter_expected_num_batches = 1
    # TODO: For some reason we are not getting a batch from post_splitter_batch_request
    # breakpoint()
    assert len(post_splitter_batches) == post_splitter_expected_num_batches

    # Make sure amount of data in batches is as expected
    post_splitter_batch_data = post_splitter_batches[0].data
    # The directory contains 12 files with 10,000 records each so the batch data
    # (spark dataframe) should contain 10,000 records after splitting by month:
    from great_expectations.compatibility.pyspark import (
        functions as F,
    )

    num_records_should_be = (
        pre_splitter_batch_data.dataframe.filter(
            F.year(F.col("pickup_datetime")) == 2020
        )
        .filter(F.month(F.col("pickup_datetime")) == 10)
        .count()
    )
    assert num_records_should_be == 10000

    assert post_splitter_batch_data.dataframe.count() == num_records_should_be  # type: ignore[attr-defined]


# TODO: Add test for using directory as part of regex:
# example = """
#     logs/
#     logs / 2023 - 05 - 03 /
#     logs/2023-05-04/
#     logs/2023-05-04/parquet.1
#     logs/2023-05-04/parquet.2
#     logs/2023-05-04/parquet.3
# """
#
# regex = "logs/{year}-{month}-{day}/"
