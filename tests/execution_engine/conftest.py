import datetime
import os
import random
from pathlib import Path
from typing import List

import pandas as pd
import pytest
from moto import mock_s3

from great_expectations.compatibility import aws, pyspark
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.batch_spec import AzureBatchSpec, GCSBatchSpec


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield aws.boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def s3_bucket(s3):
    bucket: str = "test_bucket"
    s3.create_bucket(Bucket=bucket)
    return bucket


@pytest.fixture
def test_df_small() -> pd.DataFrame:
    return pd.DataFrame(data={"col1": [1, 0, 505], "col2": [3, 4, 101]})


@pytest.fixture
def test_df_small_csv(test_df_small, tmpdir) -> bytes:
    path = Path(tmpdir) / "file.csv"
    test_df_small.to_csv(path, index=False)
    return path.read_bytes()


@pytest.fixture
def test_df_small_csv_compressed(test_df_small, tmpdir) -> bytes:
    path = Path(tmpdir) / "file.csv.gz"
    test_df_small.to_csv(path, index=False, compression="gzip")
    return path.read_bytes()


@pytest.fixture
def test_s3_files_parquet(tmpdir, s3, s3_bucket, test_df_small, test_df_small_csv):
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.parquet",
        "directory/B-2.parquet",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = Path(tmpdir) / "file.parquet"
    test_df_small.to_parquet(path)
    for key in keys:
        if key.endswith(".parquet"):
            with open(path, "rb") as f:
                s3.put_object(Bucket=s3_bucket, Body=f, Key=key)
        else:
            s3.put_object(Bucket=s3_bucket, Body=test_df_small_csv, Key=key)
    return s3_bucket, keys


@pytest.fixture
def test_s3_files_compressed(s3, s3_bucket, test_df_small_csv_compressed):
    keys: List[str] = [
        "path/A-100.csv.gz",
        "path/A-101.csv.gz",
        "directory/B-1.csv.gz",
        "directory/B-2.csv.gz",
    ]

    for key in keys:
        s3.put_object(
            Bucket=s3_bucket,
            Body=test_df_small_csv_compressed,
            Key=key,
        )
    return s3_bucket, keys


@pytest.fixture
def azure_batch_spec() -> AzureBatchSpec:
    container = "test_container"
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = keys[0]
    full_path = os.path.join(  # noqa: PTH118
        "mock_account.blob.core.windows.net", container, path
    )

    batch_spec = AzureBatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


@pytest.fixture
def gcs_batch_spec() -> GCSBatchSpec:
    bucket = "test_bucket"
    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
        "alpha-1.csv",
        "alpha-2.csv",
    ]
    path = keys[0]
    full_path = os.path.join("gs://", bucket, path)  # noqa: PTH118

    batch_spec = GCSBatchSpec(
        path=full_path,
        reader_method="read_csv",
        splitter_method="_split_on_whole_table",
    )
    return batch_spec


@pytest.fixture
def test_sparkdf(spark_session) -> pyspark.DataFrame:
    def generate_ascending_list_of_datetimes(
        n, start_date=datetime.date(2020, 1, 1), end_date=datetime.date(2020, 12, 31)
    ) -> List[datetime.datetime]:
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

    k: int = 120
    random.seed(1)
    timestamp_list: List[datetime.datetime] = generate_ascending_list_of_datetimes(
        n=k, end_date=datetime.date(2020, 1, 31)
    )
    date_list: List[datetime.date] = [
        datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list
    ]

    # noinspection PyUnusedLocal
    batch_ids: List[int] = [random.randint(0, 10) for i in range(k)]
    batch_ids.sort()
    # noinspection PyUnusedLocal
    session_ids: List[int] = [random.randint(2, 60) for i in range(k)]
    session_ids = [i - random.randint(0, 2) for i in session_ids]
    session_ids.sort()

    # noinspection PyUnusedLocal
    spark_df: pyspark.DataFrame = spark_session.createDataFrame(
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
        "timestamp",
        F.col("timestamp")
        .cast(pyspark.types.IntegerType())
        .cast(pyspark.types.StringType()),
    )
    return spark_df


@pytest.fixture
def test_folder_connection_path_tsv(tmp_path_factory) -> str:
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_tsv"))
    df1.to_csv(
        path_or_buf=os.path.join(path, "test.tsv"),  # noqa: PTH118
        sep="\t",
        index=False,
    )
    return str(path)


@pytest.fixture
def test_folder_connection_path_parquet(tmp_path_factory) -> str:
    df1 = pd.DataFrame({"col_1": [1, 2, 3, 4, 5], "col_2": ["a", "b", "c", "d", "e"]})
    path = str(tmp_path_factory.mktemp("test_folder_connection_path_parquet"))
    df1.to_parquet(path=os.path.join(path, "test.parquet"))  # noqa: PTH118
    return str(path)
