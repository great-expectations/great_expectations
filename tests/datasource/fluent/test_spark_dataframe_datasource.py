from __future__ import annotations

import inspect
import logging
import pathlib
from pprint import pformat as pf
from typing import TYPE_CHECKING, Any, Callable, Type

import pandas as pd
import pydantic
import pytest
from pytest import MonkeyPatch, param

import great_expectations.execution_engine.pandas_execution_engine
from great_expectations.datasource.fluent import PandasDatasource
from great_expectations.datasource.fluent.dynamic_pandas import PANDAS_VERSION
from great_expectations.datasource.fluent.spark_dataframe_datasource import (
    DataFrameAsset,
)
from great_expectations.datasource.fluent.sources import (
    DEFAULT_PANDAS_DATA_ASSET_NAME,
    DEFAULT_PANDAS_DATASOURCE_NAME,
    DefaultPandasDatasourceError,
    _get_field_details,
)
from great_expectations.util import camel_to_snake
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext


logger = logging.getLogger(__file__)


def test_dataframe_asset(
    empty_data_context: AbstractDataContext,
    spark_session,
    spark_df_from_pandas_df,
    test_df,
):
    # validates that a dataframe object is passed
    with pytest.raises(pydantic.ValidationError) as exc_info:
        _ = DataFrameAsset(name="malformed_asset", dataframe={})

    errors_dict = exc_info.value.errors()[0]
    assert errors_dict["loc"][0] == "dataframe"

    datasource = empty_data_context.sources.add_spark_dataframe(
        name="my_spark_dataframe_datasource"
    )

    pandas_df = test_df
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
            asset.dataframe.toPandas().equals(pandas_df)  # type: ignore[attr-defined]
            for asset in datasource.assets.values()
        ]
    )
