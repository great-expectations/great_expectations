import datetime
import random

import boto3
import pandas as pd
import pytest
from moto import mock_s3

try:
    import pyspark

    # noinspection PyPep8Naming
    import pyspark.sql.functions as F
    from pyspark.sql.types import IntegerType, StringType
except ImportError:
    pyspark = None
    F = None
    IntegerType = None
    StringType = None


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_bucket(s3):
    bucket: str = "test_bucket"
    s3.create_bucket(Bucket=bucket)
    return bucket


@pytest.fixture
def test_sparkdf(spark_session):
    def generate_ascending_list_of_datetimes(
        n, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ):
        start_time = datetime.datetime(
            start_date.year, start_date.month, start_date.day
        )
        seconds_between_dates = (end_date - start_date).total_seconds()
        # noinspection PyUnusedLocal
        datetime_list = [
            start_time
            + datetime.timedelta(seconds=random.randrange(int(seconds_between_dates)))
            for i in range(n)
        ]
        datetime_list.sort()
        return datetime_list

    k = 120
    random.seed(1)
    timestamp_list = generate_ascending_list_of_datetimes(
        n=k, end_date=datetime.date(2020, 1, 31)
    )
    date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

    # noinspection PyUnusedLocal
    batch_ids = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()
    # noinspection PyUnusedLocal
    session_ids = [random.randint(2, 60) for i in range(k)]
    session_ids = [i - random.randint(0, 2) for i in session_ids]
    session_ids.sort()

    # noinspection PyUnusedLocal
    spark_df = spark_session.createDataFrame(
        data=pd.DataFrame(
            {
                "id": range(k),
                "batch_id": batch_ids,
                "date": date_list,
                "y": [d.year for d in date_list],
                "m": [d.month for d in date_list],
                "d": [d.day for d in date_list],
                "timestamp": timestamp_list,
                "session_ids": session_ids,
                "event_type": [
                    random.choice(["start", "stop", "continue"]) for i in range(k)
                ],
                "favorite_color": [
                    "#"
                    + "".join(
                        [random.choice(list("0123456789ABCDEF")) for j in range(6)]
                    )
                    for i in range(k)
                ],
            }
        )
    )
    spark_df = spark_df.withColumn(
        "timestamp", F.col("timestamp").cast(IntegerType()).cast(StringType())
    )
    return spark_df
