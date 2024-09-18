import logging

import pytest
from pyspark.sql import Row

import great_expectations as gx

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.spark


def test_spark_connect(spark_connect_session, ephemeral_context_with_defaults):
    context = ephemeral_context_with_defaults
    df = spark_connect_session.createDataFrame(
        [
            Row(column=1),
            Row(column=2),
            Row(column=3),
        ]
    )

    bd = (
        context.data_sources.add_spark(name="spark-connect-ds")
        .add_dataframe_asset(name="spark-connect-asset")
        .add_batch_definition_whole_dataframe(name="spark-connect-bd")
    )
    suite = context.suites.add(
        gx.ExpectationSuite(
            name="spark-connect-suite",
            expectations=[
                gx.expectations.ExpectColumnValuesToBeInSet(column="column", value_set=[1, 2, 3]),
            ],
        )
    )

    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="spark-connect-vd", suite=suite, data=bd)
    )

    results = vd.run(batch_parameters={"dataframe": df})

    assert results.success
