import os
import pandas as pd
import pytest
from decimal import Decimal
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.expectations.core.expect_column_mean_to_be_between import (
    ExpectColumnMeanToBeBetween,
)


# <snippet name="tests/expectations/core/test_expect_column_mean_to_be_positive.py ExpectColumnMeanToBePositive_class_def">
class ExpectColumnMeanToBePositive(ExpectColumnMeanToBeBetween):
    """Expects the mean of values in this column to be positive"""

    default_kwarg_values = {
        "min_value": 0,
        "strict_min": True,
    }
    # </snippet>
    # <snippet name="tests/expectations/core/test_expect_column_mean_to_be_positive.py validate_config">
    def validate_configuration(self, configuration):
        super().validate_configuration(configuration)
        assert "min_value" not in configuration.kwargs, "min_value cannot be altered"
        assert "max_value" not in configuration.kwargs, "max_value cannot be altered"
        assert "strict_min" not in configuration.kwargs, "strict_min cannot be altered"
        assert "strict_max" not in configuration.kwargs, "strict_max cannot be altered"

    # </snippet>
    # <snippet name="tests/expectations/core/test_expect_column_mean_to_be_positive.py library_metadata">
    library_metadata = {"tags": ["basic stats"], "contributors": ["@joegargery"]}


#     </snippet>


def test_expect_column_mean_to_be_positive(data_context_with_datasource_pandas_engine):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": [0, 1, 3, 4, 5]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_mean_to_be_positive(column="a")

    assert result.success is True

def test_DX_506_sparkDF_mean_between_with_decimal_type(
    data_context_parameterized_expectation_suite, tmp_path_factory, test_backends
):
    """
    Ensure that an external sparkSession can be reused by specifying the
    force_reuse_spark_context argument.
    """
    if "SparkDFDataset" not in test_backends:
        pytest.skip("No spark backend selected.")
    from pyspark.sql import SparkSession  # isort:skip

    dataset_name = "test_spark_dataset"

    spark = SparkSession.builder.appName("local").master("local[1]").getOrCreate()
    data = [[Decimal(0.0123), 'a'], [Decimal(1.4352), 'b'], [Decimal(2.8732), 'c']]

    schema = StructType([
            StructField('col1', DecimalType(38, 10 )),
            StructField('col2', StringType())])
    spark_df = spark.createDataFrame(data, schema=schema)
    tmp_parquet_filename = os.path.join(  # noqa: PTH118
        tmp_path_factory.mktemp(dataset_name).as_posix(), dataset_name
    )
    spark_df.write.format("parquet").save(tmp_parquet_filename)

    data_context_parameterized_expectation_suite.add_datasource(
        dataset_name,
        class_name="SparkDFDatasource",
        force_reuse_spark_context=True,
        module_name="great_expectations.datasource",
        batch_kwargs_generators={},
    )

    df = spark.read.format("parquet").load(tmp_parquet_filename)

    data_context_parameterized_expectation_suite.add_expectation_suite(dataset_name)
    # suite = data_context_parameterized_expectation_suite.get_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request = RuntimeBatchRequest(
        datasource_name=dataset_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="your_data_asset_name",  
        runtime_parameters={}, 
        batch_identifiers={},
    )
    # batch_kwargs = {"dataset": df, "datasource": dataset_name}
    
    # batch = data_context_parameterized_expectation_suite.get_batch(
    #     batch_kwargs=batch_kwargs, expectation_suite_name=dataset_name
    # )
    validator = data_context_parameterized_expectation_suite.get_validator(
       batch_request=batch_request, expectation_suite_name=dataset_name
    )
    results = validator.expect_mean_to_be_positive("col1")
    assert results.success, "Failed to use external SparkSession"
    spark.stop()