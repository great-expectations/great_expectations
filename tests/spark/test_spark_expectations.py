import importlib.util

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from great_expectations.dataset.sparkdf_dataset import SparkDFDataset


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
@pytest.fixture
def test_dataframe(spark_session):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField(
                "address",
                StructType(
                    [
                        StructField("street", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("house_number", IntegerType(), True),
                    ]
                ),
                False,
            ),
            StructField("name_duplicate", StringType(), True),
            StructField("non.nested", StringType(), True),
        ]
    )
    l = [
        ("Alice", 1, ("Street 1", "Alabama", 10), "Alice", "a"),
        ("Bob", 2, ("Street 2", "Brooklyn", 11), "Bob", "b"),
        ("Charlie", 3, ("Street 3", "Alabama", 12), "Charlie", "c"),
    ]

    rdd = spark_session.sparkContext.parallelize(l)

    df = spark_session.createDataFrame(rdd, schema)
    return SparkDFDataset(df, persist=True)


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_values_to_be_of_type(spark_session, test_dataframe):
    """
    data asset expectation
    """
    assert test_dataframe.expect_column_values_to_be_of_type(
        "address.street", "StringType"
    ).success
    assert test_dataframe.expect_column_values_to_be_of_type(
        "non.nested", "StringType", non_nested=True
    ).success
    assert test_dataframe.expect_column_values_to_be_of_type(
        "name", "StringType"
    ).success
    with pytest.raises(AnalysisException):
        test_dataframe.expect_column_values_to_be_of_type("non.nested", "StringType")


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_pair_values_to_be_equal(spark_session, test_dataframe):
    """
    column_pair_map_expectation
    """
    assert test_dataframe.expect_column_pair_values_to_be_equal(
        "name", "name_duplicate"
    ).success
    assert not test_dataframe.expect_column_pair_values_to_be_equal(
        "name", "address.street"
    ).success
    assert not test_dataframe.expect_column_pair_values_to_be_equal(
        "name", "non.nested", non_nested_B=True
    ).success
    with pytest.raises(AnalysisException):
        test_dataframe.expect_column_pair_values_to_be_equal("name", "non.nested")


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_pair_values_A_to_be_greater_than_B(
    spark_session, test_dataframe
):
    """
    column_pair_map_expectation
    """
    assert test_dataframe.expect_column_pair_values_A_to_be_greater_than_B(
        "address.house_number", "age"
    ).success
    assert test_dataframe.expect_column_pair_values_A_to_be_greater_than_B(
        "age", "age", or_equal=True
    ).success


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_multicolumn_values_to_be_unique(spark_session, test_dataframe):
    """
    multicolumn_map_expectation
    """
    assert test_dataframe.expect_multicolumn_values_to_be_unique(
        ["name", "age"]
    ).success
    assert test_dataframe.expect_multicolumn_values_to_be_unique(
        ["address.street", "name"]
    ).success
    assert test_dataframe.expect_multicolumn_values_to_be_unique(
        ["address.street", "non.nested"], non_nested_column_list=["non.nested"]
    ).success
    with pytest.raises(AnalysisException):
        test_dataframe.expect_multicolumn_values_to_be_unique(
            ["address.street", "non.nested"]
        )


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_values_to_be_unique(spark_session, test_dataframe):
    """
    column_map_expectation
    """
    assert test_dataframe.expect_column_values_to_be_unique("name").success
    assert not test_dataframe.expect_column_values_to_be_unique("address.city").success
    assert test_dataframe.expect_column_values_to_be_unique(
        "non.nested", non_nested=True
    ).success
    with pytest.raises(AnalysisException):
        test_dataframe.expect_column_values_to_be_unique("non.nested")


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_value_lengths_to_be_between(spark_session, test_dataframe):
    """
    column_map_expectation
    """
    assert test_dataframe.expect_column_value_lengths_to_be_between(
        "name", 3, 7
    ).success
    assert test_dataframe.expect_column_value_lengths_to_be_between(
        "address.street", 1, 10
    ).success


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None, reason="requires the Spark library"
)
def test_expect_column_value_lengths_to_equal(spark_session, test_dataframe):
    """
    column_map_expectation
    """
    assert test_dataframe.expect_column_value_lengths_to_equal("age", 1).success
    assert test_dataframe.expect_column_value_lengths_to_equal(
        "address.street", 8
    ).success


# TODO: Get tests working for expect_column_values_to_be_in_type_list
