from __future__ import annotations

import copy
import json
import logging
import pathlib
from typing import TYPE_CHECKING

import pydantic
import pytest

from great_expectations.datasource.fluent.spark_datasource import (
    DataFrameAsset,
    SparkConfig,
)
from great_expectations.util import is_candidate_subset_of_target

if TYPE_CHECKING:
    import pandas as pd

    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__file__)


@pytest.fixture
def valid_file_path(csv_path: pathlib.Path) -> pathlib.Path:
    return csv_path / "yellow_tripdata_sample_2018-03.csv"


def test_dataframe_asset(
    empty_data_context: AbstractDataContext,
    spark_session,
    spark_df_from_pandas_df,
    test_df_pandas,
):
    # validates that a dataframe object is passed
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = DataFrameAsset(name="malformed_asset", dataframe={})

    errors_dict = exc_info.value.errors()[0]
    assert errors_dict["loc"][0] == "dataframe"

    datasource = empty_data_context.sources.add_spark(name="my_spark_datasource")

    pandas_df = test_df_pandas
    spark_df = spark_df_from_pandas_df(spark_session, pandas_df)

    dataframe_asset = datasource.add_dataframe_asset(
        name="my_dataframe_asset",
    )
    assert isinstance(dataframe_asset, DataFrameAsset)
    assert dataframe_asset.name == "my_dataframe_asset"
    assert len(datasource.assets) == 1

    _ = dataframe_asset.build_batch_request(dataframe=spark_df)

    dataframe_asset = datasource.add_dataframe_asset(
        name="my_second_dataframe_asset",
    )
    assert len(datasource.assets) == 2

    _ = dataframe_asset.build_batch_request(dataframe=spark_df)

    assert all(
        asset.dataframe is not None and asset.dataframe.toPandas().equals(pandas_df)
        for asset in datasource.assets
    )


def test_spark_data_asset_batch_metadata(
    empty_data_context: AbstractDataContext,
    valid_file_path: pathlib.Path,
    test_df_pandas: pd.DataFrame,
    spark_session,
    spark_df_from_pandas_df,
):
    my_config_variables = {"pipeline_filename": __file__}
    empty_data_context.config_variables.update(my_config_variables)

    spark_df = spark_df_from_pandas_df(spark_session, test_df_pandas)

    spark_datasource = empty_data_context.sources.add_spark("my_spark_datasource")

    batch_metadata = {
        "no_curly_pipeline_filename": "$pipeline_filename",
        "curly_pipeline_filename": "${pipeline_filename}",
        "pipeline_step": "transform_3",
    }

    dataframe_asset = spark_datasource.add_dataframe_asset(
        name="my_dataframe_asset",
        batch_metadata=batch_metadata,
    )
    assert dataframe_asset.batch_metadata == batch_metadata

    batch_list = dataframe_asset.get_batch_list_from_batch_request(
        dataframe_asset.build_batch_request(dataframe=spark_df)
    )
    assert len(batch_list) == 1
    substituted_batch_metadata = copy.deepcopy(batch_metadata)
    substituted_batch_metadata.update(
        {
            "no_curly_pipeline_filename": __file__,
            "curly_pipeline_filename": __file__,
        }
    )
    assert batch_list[0].metadata == substituted_batch_metadata


def test_spark_config_passed_to_execution_engine(
    empty_data_context: AbstractDataContext,
    spark_session,
):
    spark_config: SparkConfig | None = {
        "spark.sql.catalogImplementation": "in-memory",
        "spark.app.name": "gx_spark_fluent_datasource_test",
        "spark.default.parallelism": 4,
        "spark.driver.memory": "16g",
        "spark.executor.memory": 471859200,
        "spark.master": "local[*]",
    }
    datasource = empty_data_context.sources.add_spark(
        name="my_spark_datasource",
        spark_config=spark_config,
        force_reuse_spark_context=False,
    )
    spark_config = json.loads(json.dumps(spark_config), parse_int=str, parse_float=str)
    assert is_candidate_subset_of_target(
        candidate=spark_config,
        target=datasource.get_execution_engine().config["spark_config"],
    )


def test_build_batch_request_raises_if_missing_dataframe(
    empty_data_context: AbstractDataContext,
    spark_session,
):
    dataframe_asset = empty_data_context.sources.add_spark(
        name="my_spark_datasource"
    ).add_dataframe_asset(name="my_dataframe_asset")

    with pytest.raises(ValueError) as e:
        dataframe_asset.build_batch_request()

    assert "Cannot build batch request for dataframe asset without a dataframe" in str(
        e.value
    )
