from __future__ import annotations

import copy
import logging
from decimal import Decimal
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from great_expectations.compatibility import pyspark
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    AbstractConfig,
    AssetConfig,
    DataConnectorConfig,
    assetConfigSchema,
    dataConnectorConfigSchema,
)
from great_expectations.util import (
    convert_to_json_serializable,
    deep_filter_properties_iterable,
    requires_lossy_conversion,
)

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from marshmallow import Schema


@pytest.fixture
def spark_schema(spark_session: pyspark.SparkSession) -> pyspark.types.StructType:
    return pyspark.types.StructType(
        [
            pyspark.types.StructField("a", pyspark.types.IntegerType(), True, None),
            pyspark.types.StructField("b", pyspark.types.IntegerType(), True, None),
        ]
    )


@pytest.fixture
def data_connector_config_spark(spark_session) -> DataConnectorConfig:
    return DataConnectorConfig(
        class_name="ConfiguredAssetFilesystemDataConnector",
        module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
        batch_spec_passthrough={
            "reader_options": {"header": True},
        },
    )


@pytest.fixture
def datas_connector_config_with_schema_spark(spark_session, spark_schema) -> DataConnectorConfig:
    return DataConnectorConfig(
        class_name="ConfiguredAssetFilesystemDataConnector",
        module_name="great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",
        batch_spec_passthrough={
            "reader_options": {"header": True, "schema": spark_schema},
        },
    )


@pytest.fixture
def asset_config_spark(spark_session) -> AssetConfig:
    return AssetConfig(
        class_name="Asset",
        module_name="great_expectations.datasource.data_connector.asset",
        batch_spec_passthrough={
            "reader_options": {"header": True},
        },
    )


@pytest.fixture
def asset_config_with_schema_spark(spark_session, spark_schema) -> AssetConfig:
    return AssetConfig(
        class_name="Asset",
        module_name="great_expectations.datasource.data_connector.asset",
        batch_spec_passthrough={
            "reader_options": {"header": True, "schema": spark_schema},
        },
    )


@pytest.mark.unit
def test_lossy_serialization_warning(caplog):
    caplog.set_level(logging.WARNING, logger="great_expectations.core")

    d = Decimal("12345.678901234567890123456789")

    convert_to_json_serializable(d)
    assert len(caplog.messages) == 1
    assert caplog.messages[0].startswith(
        "Using lossy conversion for decimal 12345.678901234567890123456789"
    )

    caplog.clear()
    d = Decimal("0.1")
    convert_to_json_serializable(d)
    print(caplog.messages)
    assert len(caplog.messages) == 0


@pytest.mark.unit
def test_lossy_conversion():
    d = Decimal("12345.678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.67890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("12345.6789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.12345678901234567890123456789")
    assert requires_lossy_conversion(d)

    d = Decimal("0.1234567890123456")
    assert requires_lossy_conversion(d)

    d = Decimal("0.123456789012345")
    assert not requires_lossy_conversion(d)

    d = Decimal("0.1")
    assert not requires_lossy_conversion(d)


# TODO add unittests for convert_to_json_serializable() and ensure_json_serializable()
@pytest.mark.spark
def test_serialization_of_spark_df(spark_session):
    df = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3]}

    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    sdf = spark_session.createDataFrame(df)
    assert convert_to_json_serializable(sdf) == {"a": [1, 2, 3], "b": [4, 5, 6]}


@pytest.mark.unit
def test_batch_request_deepcopy():
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    batch_request: RuntimeBatchRequest = RuntimeBatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "my_runtime_data_connector",
            "data_asset_name": "default_data_asset_name",
            "batch_identifiers": {
                "pipeline_stage_name": "core_processing",
                "airflow_run_id": 1234567890,
            },
            "runtime_parameters": {"batch_data": test_df},
        }
    )

    batch_request_copy: RuntimeBatchRequest = copy.deepcopy(batch_request)
    assert deep_filter_properties_iterable(
        properties=batch_request_copy.to_dict(),
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=batch_request.to_dict(),
        clean_falsy=True,
    )


@pytest.mark.parametrize(
    "data_connector_config,expected_serialized_data_connector_config",
    [
        pytest.param(
            "data_connector_config_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                    }
                },
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",  # noqa: E501
            },
            id="data_connector_with_schema",
        ),
        pytest.param(
            "datas_connector_config_with_schema_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                        "schema": {
                            "fields": [
                                {
                                    "metadata": {},
                                    "name": "a",
                                    "nullable": True,
                                    "type": "integer",
                                },
                                {
                                    "metadata": {},
                                    "name": "b",
                                    "nullable": True,
                                    "type": "integer",
                                },
                            ],
                            "type": "struct",
                        },
                    }
                },
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector.configured_asset_filesystem_data_connector",  # noqa: E501
            },
            id="data_connector_without_schema",
        ),
    ],
)
@pytest.mark.spark
def test_data_connector_and_nested_objects_are_serialized_spark(
    data_connector_config: DataConnectorConfig,
    expected_serialized_data_connector_config: dict,
    spark_session: pyspark.SparkSession,
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(data_connector_config, str):
        data_connector_config = request.getfixturevalue(data_connector_config)

    observed_dump = dataConnectorConfigSchema.dump(obj=data_connector_config)
    assert observed_dump == expected_serialized_data_connector_config
    observed_load = dataConnectorConfigSchema.load(observed_dump)
    assert dataConnectorConfigSchema.dump(observed_load) == dataConnectorConfigSchema.dump(
        data_connector_config
    )


@pytest.mark.parametrize(
    "asset_config,expected_serialized_asset_config",
    [
        pytest.param(
            "asset_config_with_schema_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {
                        "header": True,
                        "schema": {
                            "fields": [
                                {
                                    "metadata": {},
                                    "name": "a",
                                    "nullable": True,
                                    "type": "integer",
                                },
                                {
                                    "metadata": {},
                                    "name": "b",
                                    "nullable": True,
                                    "type": "integer",
                                },
                            ],
                            "type": "struct",
                        },
                    }
                },
                "class_name": "Asset",
                "module_name": "great_expectations.datasource.data_connector.asset",
            },
            id="asset_serialized_with_schema",
        ),
        pytest.param(
            "asset_config_spark",
            {
                "batch_spec_passthrough": {
                    "reader_options": {"header": True},
                },
                "class_name": "Asset",
                "module_name": "great_expectations.datasource.data_connector.asset",
            },
            id="asset_serialized_without_schema",
        ),
    ],
)
@pytest.mark.spark
def test_asset_and_nested_objects_are_serialized_spark(
    asset_config: AssetConfig,
    expected_serialized_asset_config: dict,
    spark_session: pyspark.SparkSession,
    request: FixtureRequest,
):
    # when using a fixture value in a parmeterized test, we need to call
    # request.getfixturevalue()
    if isinstance(asset_config, str):
        asset_config = request.getfixturevalue(asset_config)

    observed_dump = assetConfigSchema.dump(obj=asset_config)
    assert observed_dump == expected_serialized_asset_config
    observed_load = assetConfigSchema.load(observed_dump)
    assert assetConfigSchema.dump(observed_load) == assetConfigSchema.dump(asset_config)


def generic_config_serialization_assertions(
    config: AbstractConfig,
    schema: Schema,
    expected_serialized_config: dict,
    expected_roundtrip_config: dict,
) -> None:
    """Generic assertions for testing configuration serialization.

    Args:
        config: config object to check serialization.
        schema: Marshmallow schema used for serializing / deserializing config object.
        expected_serialized_config: expected when serializing a config via dump
        expected_roundtrip_config: expected config after loading the dump

    Returns:
        None

    Raises:
        AssertionError
    """

    observed_dump = schema.dump(config)
    assert observed_dump == expected_serialized_config

    loaded_data = schema.load(observed_dump)
    assert loaded_data.to_json_dict() == expected_roundtrip_config
