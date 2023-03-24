from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pydantic
import pytest

from great_expectations.datasource.fluent.spark_datasource import (
    DataFrameAsset,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__file__)


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
        name="my_dataframe_asset", dataframe=spark_df
    )
    assert isinstance(dataframe_asset, DataFrameAsset)
    assert dataframe_asset.name == "my_dataframe_asset"
    assert len(datasource.assets) == 1

    _ = datasource.add_dataframe_asset(
        name="my_second_dataframe_asset", dataframe=spark_df
    )
    assert len(datasource.assets) == 2

    assert all(
        [
            asset.dataframe.toPandas().equals(pandas_df)
            for asset in datasource.assets.values()
        ]
    )
