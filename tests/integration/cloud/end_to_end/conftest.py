from __future__ import annotations

import os
import pathlib
import uuid
from typing import TYPE_CHECKING, Iterator

import numpy as np
import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import CloudDataContext
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.compatibility import pyspark
    from great_expectations.core import ExpectationSuite
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.validator import Validator


@pytest.fixture(scope="package")
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context


@pytest.fixture(scope="module")
def datasource_name(
    context: CloudDataContext,
) -> Iterator[str]:
    datasource_name = f"ds_{uuid.uuid4().hex}"
    yield datasource_name
    # if the test was skipped, we may not have a datasource to clean up
    # in that case, we create one simply to test get and delete
    try:
        _ = context.get_datasource(datasource_name=datasource_name)
    except ValueError:
        _ = context.sources.add_pandas(name=datasource_name)
        context.get_datasource(datasource_name=datasource_name)
    context.delete_datasource(datasource_name=datasource_name)
    with pytest.raises(ValueError):
        _ = context.get_datasource(datasource_name=datasource_name)


@pytest.fixture(scope="module")
def expectation_suite(
    context: CloudDataContext,
) -> Iterator[ExpectationSuite]:
    expectation_suite_name = f"es_{uuid.uuid4().hex}"
    expectation_suite = context.add_expectation_suite(
        expectation_suite_name=expectation_suite_name,
    )
    assert len(expectation_suite.expectations) == 0
    yield expectation_suite
    expectation_suite = context.add_or_update_expectation_suite(expectation_suite=expectation_suite)
    assert len(expectation_suite.expectations) > 0
    _ = context.suites.get(name=expectation_suite_name)
    context.delete_expectation_suite(expectation_suite_name=expectation_suite_name)
    with pytest.raises(gx_exceptions.DataContextError):
        _ = context.suites.get(name=expectation_suite_name)


@pytest.fixture(scope="module")
def validator(
    context: CloudDataContext,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
) -> Iterator[Validator]:
    validator: Validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite.name,
    )
    validator.head()
    yield validator
    validator.save_expectation_suite()
    expectation_suite = validator.get_expectation_suite()
    _ = context.suites.get(
        name=expectation_suite.name,
    )


@pytest.fixture(scope="module")
def checkpoint(
    context: CloudDataContext,
    batch_request: BatchRequest,
    expectation_suite: ExpectationSuite,
) -> Iterator[Checkpoint]:
    checkpoint_name = f"{batch_request.data_asset_name} | {expectation_suite.name}"

    validation_definitions: list[ValidationDefinition] = []
    checkpoint = Checkpoint(name=checkpoint_name, validation_definitions=validation_definitions)
    checkpoint = context.checkpoints.add(checkpoint)
    yield checkpoint
    context.checkpoints.delete(checkpoint)

    with pytest.raises(gx_exceptions.DataContextError):
        context.checkpoints.get(name=checkpoint_name)


@pytest.fixture(scope="module")
def tmp_path(tmp_path_factory) -> pathlib.Path:
    return tmp_path_factory.mktemp("project")


@pytest.fixture(scope="package")
def get_missing_data_asset_error_type() -> type[Exception]:
    return LookupError


@pytest.fixture(scope="package")
def in_memory_batch_request_missing_dataframe_error_type() -> type[Exception]:
    return ValueError


@pytest.fixture(scope="module")
def spark_df_from_pandas_df():
    """
    Construct a spark dataframe from pandas dataframe.
    Returns:
        Function that can be used in your test e.g.:
        spark_df = spark_df_from_pandas_df(spark_session, pandas_df)
    """

    def _construct_spark_df_from_pandas(
        spark_session,
        pandas_df,
    ):
        spark_df = spark_session.createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in pandas_df.to_records(index=False)
            ],
            pandas_df.columns.tolist(),
        )
        return spark_df

    return _construct_spark_df_from_pandas


@pytest.fixture(scope="module")
def spark_session() -> pyspark.SparkSession:
    from great_expectations.compatibility import pyspark

    if pyspark.SparkSession:  # type: ignore[truthy-function]
        return SparkDFExecutionEngine.get_or_create_spark_session()

    raise ValueError("spark tests are requested, but pyspark is not installed")
