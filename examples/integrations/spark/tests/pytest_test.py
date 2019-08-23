import sys
import os
import shutil
import time
from pathlib import Path
from unittest import mock
from pyspark.ml import Pipeline
from pyspark.sql import (
    DataFrame,
    SparkSession
)
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType
)
from pyspark.sql.functions import (
    col,
    lit
)
from pathlib import Path
from typing import List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from common.utils import (
    local_path_to_hdfs_path,
    load_csv_file_into_data_frame
)


def test_can_run_pytest(
        spark_session: SparkSession
):
    print(f"{time.strftime('%H:%M:%S')}  Starting test")

    data_dir: str = Path(__file__).parent.joinpath('../../../data/')

    raw_data_csv_file_name: str = "Titanic.csv"
    raw_data_csv_file_path: str = local_path_to_hdfs_path(data_dir.joinpath(raw_data_csv_file_name))

    schema: StructType = StructType([
        StructField("", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("PClass", StringType(), True),
        StructField("Age", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Survived", StringType(), True),
        StructField("SecCode", StringType(), True)
    ])

    df: DataFrame = load_csv_file_into_data_frame(
        spark_session=spark_session,
        path_to_csv=raw_data_csv_file_path,
        schema=schema,
        delimiter=",",
        limit=-1,
        view=None
    )

    df.explain(extended=True)
    df.printSchema()
    df.collect()
    df.show(truncate=False)

    print(f'DataFrame contains "{df.count()}" rows.')

    print(f"{time.strftime('%H:%M:%S')}  Finished test")
