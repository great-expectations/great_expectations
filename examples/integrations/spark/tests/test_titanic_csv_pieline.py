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
from typing import List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

from common.utils import (
    local_path_to_hdfs_path,
    load_csv_file_into_data_frame,
    create_empty_data_frame
)
from pipelines.titanic_csv_pipeline import TitanicCsvPipeline


def test_titanic_csv_pipeline(
        spark_session: SparkSession
):
    print(f"{time.strftime('%H:%M:%S')}  Starting test")

    data_dir: str = Path(__file__).parent.joinpath('../../../data/')

    raw_data_csv_file_name: str = "Titanic.csv"
    raw_data_csv_file_path: str = local_path_to_hdfs_path(data_dir.joinpath(raw_data_csv_file_name))

    df: DataFrame = create_empty_data_frame(spark_session=spark_session)
    titanic_csv_pipeline: Pipeline = TitanicCsvPipeline(
        source_dataset_csv_file_path=raw_data_csv_file_path,
        spark_session=spark_session
    )
    df_res: DataFrame = titanic_csv_pipeline.run(df=df)

    df_res.show(truncate=False)

    print(f'The processed "Titanic" DataFrame contains {df_res.count()} rows with {len(df_res.columns)} columns in each row.')

    print(f"{time.strftime('%H:%M:%S')}  Finished test")
