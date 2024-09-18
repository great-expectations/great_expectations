import logging
from typing import Any

import pytest

import great_expectations as gx
from great_expectations.compatibility.pyspark import ConnectDataFrame, Row, SparkConnectSession
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.exceptions.exceptions import BuildBatchRequestError

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.spark

DATAFRAME_VALUES = [1, 2, 3]


@pytest.fixture
def spark_validation_definition(
    ephemeral_context_with_defaults: AbstractDataContext,
) -> ValidationDefinition:
    context = ephemeral_context_with_defaults
    bd = (
        context.data_sources.add_spark(name="spark-connect-ds")
        .add_dataframe_asset(name="spark-connect-asset")
        .add_batch_definition_whole_dataframe(name="spark-connect-bd")
    )
    suite = context.suites.add(
        gx.ExpectationSuite(
            name="spark-connect-suite",
            expectations=[
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column="column", value_set=DATAFRAME_VALUES
                ),
            ],
        )
    )
    return context.validation_definitions.add(
        gx.ValidationDefinition(name="spark-connect-vd", suite=suite, data=bd)
    )


@pytest.mark.spark
def test_spark_connect(
    spark_connect_session: SparkConnectSession,
    spark_validation_definition: ValidationDefinition,
):
    df = spark_connect_session.createDataFrame(
        [Row(column=x) for x in DATAFRAME_VALUES],
    )
    assert isinstance(df, ConnectDataFrame)

    results = spark_validation_definition.run(batch_parameters={"dataframe": df})

    assert results.success


@pytest.mark.parametrize("not_a_dataframe", [None, 1, "string", 1.0, True])
def test_error_messages_if_we_get_an_invalid_dataframe(
    not_a_dataframe: Any,
    spark_validation_definition: ValidationDefinition,
):
    with pytest.raises(
        BuildBatchRequestError, match="Cannot build batch request without a Spark DataFrame."
    ):
        spark_validation_definition.run(batch_parameters={"dataframe": not_a_dataframe})


@pytest.mark.xfail(
    reason="If we use the factory method on SparkConnectSession to create the session, we fail",
    strict=False,
)
def test_spark_connect_with_bad_factory_method(
    spark_validation_definition: ValidationDefinition,
):
    """The purpose of this test is to document an issue with using SparkConnectSession's
    factory method to create the session.

    Unfortunately, because spark sessions are persisted across tests, we can't run this
    with other spark tests, and therefore can't even put `strict=True`.

    Running this test in isolation will fail, but running it in the suite will pass. :(
    """
    spark_connect_session = SparkConnectSession.builder.remote("sc://localhost:15002").getOrCreate()
    assert isinstance(spark_connect_session, SparkConnectSession)
    df = spark_connect_session.createDataFrame(
        [Row(column=x) for x in DATAFRAME_VALUES],
    )

    results = spark_validation_definition.run(batch_parameters={"dataframe": df})

    assert results.success
