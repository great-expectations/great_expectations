from pathlib import Path
from pyspark import keyword_only
from pyspark.sql.types import StructType
from pyspark.sql import (
    DataFrame,
    SparkSession
)


def local_path_to_hdfs_path(local_path: str) -> str:
    """
    :param local_path:
    :return:
    """
    return f'file://{local_path}'


def load_csv_file_into_data_frame(
    spark_session: SparkSession,
    path_to_csv: str,
    schema: StructType,
    delimiter=",",
    limit: int = -1,
    view: str = None
):
    if not path_to_csv:
        raise ValueError(f"path_to_csv is empty: {path_to_csv}")

    if path_to_csv.__contains__(":"):
        full_path_to_csv = path_to_csv
    else:
        data_dir: str = Path(__file__).parent.parent.joinpath('./')
        full_path_to_csv: str = f"file://{data_dir.joinpath(path_to_csv)}"

    print(f"Loading csv file: {full_path_to_csv}")

    # https://docs.databricks.com/spark/latest/data-sources/read-csv.html
    df = spark_session.read \
        .option("mode", "DROPMALFORMED") \
        .schema(schema) \
        .option("inferSchema", "false") \
        .format('com.databricks.spark.csv') \
        .options(header='true') \
        .option("delimiter", delimiter) \
        .load(full_path_to_csv)

    if limit and limit > -1:
        df = df.limit(limit)

    if view is not None:
        df.createOrReplaceTempView(view)

    print(f'Finished Loading csv file from "{path_to_csv}".')

    return df
