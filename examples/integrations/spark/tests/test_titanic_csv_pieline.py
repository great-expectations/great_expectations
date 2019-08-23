import os
import sys
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
from pipelines.titanic_csv_pipeline import TitanicCsvPipeline


def test_titanic_csv_pipeline(
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

    print(f'The original "Titanic" DataFrame (obtained from "{raw_data_csv_file_path}") contains {df.count()} rows with {len(df.columns)} columns in each row.')

    titanic_csv_pipeline: TitanicCsvPipeline = TitanicCsvPipeline(spark_session=spark_session)
    df_res: DataFrame = titanic_csv_pipeline.run(df=df)

    df_res.show(truncate=False)

    print(f'The processed "Titanic" DataFrame contains {df_res.count()} rows with {len(df_res.columns)} columns in each row.')

    print(f"{time.strftime('%H:%M:%S')}  Finished test")
