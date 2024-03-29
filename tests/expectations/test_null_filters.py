import pytest

from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)


@pytest.mark.spark
def test_spark_null_filters(spark_session):
    import pandas as pd
    import pyspark
    import pyspark.sql.functions as F

    df = spark_session.createDataFrame(
        pd.DataFrame({"a": [1.0, 2, 3, 4]}),
        schema=pyspark.sql.types.StructType(
            [pyspark.sql.types.StructField("a", pyspark.sql.types.FloatType())]
        ),
    )
    assert df.agg(F.max(F.col("a"))).collect()[0][0] == 4

    df = spark_session.createDataFrame(
        pd.DataFrame({"a": [1, 2, 3, 4]}),
        schema=pyspark.sql.types.StructType(
            [pyspark.sql.types.StructField("a", pyspark.sql.types.IntegerType())]
        ),
    )
    assert df.agg(F.max(F.col("a"))).collect()[0][0] == 4

    df = spark_session.createDataFrame(
        pd.DataFrame({"a": [1.0, 2, 3, None, None, 4]}),
        schema=pyspark.sql.types.StructType(
            [pyspark.sql.types.StructField("a", pyspark.sql.types.FloatType(), nullable=True)]
        ),
    )
    assert (
        df.agg(F.max(F.when(~F.isnull(F.col("a")) & ~F.isnan(F.col("a")), F.col("a")))).collect()[
            0
        ][0]
        == 4
    )

    df = spark_session.createDataFrame(
        pd.DataFrame({"a": [1.0, 2, 3, None, None, 4]}),
        schema=pyspark.sql.types.StructType(
            [pyspark.sql.types.StructField("a", pyspark.sql.types.FloatType())]
        ),
    )
    assert (
        df.agg(
            F.max(F.when(~F.isnull(F.col("a")) & ~F.isnan(F.col("a")), F.col("a")).otherwise(0))
        ).collect()[0][0]
        == 4
    )


@pytest.mark.filesystem
def test_sa_null_filters(sa):
    import pandas as pd

    eng = sa.create_engine("sqlite://")
    # Demonstrate that spark's max aggregate function can tolerate null values
    df = pd.DataFrame({"a": [1, 2, 3, None, None, 4]})
    add_dataframe_to_db(df=df, name="test", con=eng, index=False)
    with eng.begin() as connection:
        assert connection.execute(sa.text("SELECT MAX(a) FROM test;")).fetchone()[0] == 4
