import json
from datetime import datetime
import tzlocal
import uuid  # used to generate run_id
from pathlib import Path
from io import TextIOWrapper
from pyspark import SparkContext
from pyspark.sql import (
    SparkSession,
    SQLContext,
    DataFrame
)
from pyspark.sql.functions import concat_ws
from pyspark.sql.types import StructType
import great_expectations as ge
from great_expectations import DataContext
# from great_expectations.dataset import SparkDFDataset
from great_expectations.dataset import Dataset
from typing import Dict


def local_path_to_hdfs_path(local_path: str) -> str:
    """
    :param local_path:
    :return:
    """
    return f'file://{local_path}'


def flatten(my_list: object) -> object:
    """
    :param my_list:
    :return:
    """
    if not my_list:
        return my_list
    if isinstance(my_list[0], list):
        return flatten(my_list[0]) + flatten(my_list[1:])
    return my_list[:1] + flatten(my_list[1:])


def load_csv_file_into_data_frame(
    spark_session: SparkSession,
    path_to_csv: str,
    schema: StructType,
    delimiter=",",
    limit: int = -1,
    view: str = None
) -> DataFrame:
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


def write_data_frame_csv_of_file(
    df: DataFrame,
    file: TextIOWrapper,
    separator: str = ",",
    limit: int = -1,
    gzip: bool = False
):
    """
    :param gzip:
    :param file:
    :param separator:
    :param df:
    :param limit:
    :return:
    """

    cols = df.columns

    if limit > 0:
        df = df.limit(limit)

    df = df.na.fill('""').na.fill(0)

    df_csv = df.select(concat_ws(separator, *cols).alias("contents"))

    result = df_csv.select("contents")

    rows = (f"{row['contents']}\n" for row in result.collect())

    if gzip:
        file.write(bytearray(separator.join(cols), encoding="utf-8"))
        file.write(bytearray("\n", encoding="utf-8"))
        for row in rows:
            file.write(bytearray(f"{row}", encoding="utf-8"))
    else:
        file.write(separator.join(cols))
        file.write("\n")
        file.writelines(rows)


def create_empty_data_frame(spark_session: SparkSession) -> DataFrame:
    """
    :param spark_session:
    :return:
    """
    schema = StructType([])
    df: DataFrame = spark_session.createDataFrame(
        data=spark_session.sparkContext.emptyRDD(),
        schema=schema
    )
    return df


def ge_tap(
    data_asset_name: str,
    df: DataFrame
) -> Dict[str, list]:
    ge_data_context: DataContext = ge.data_context.DataContext()
    expectation_suite_name: str = f'expectation_suite-{data_asset_name}'
    batch: Dataset = ge_data_context.get_batch(
        data_asset_name=data_asset_name,
        expectation_suite_name=expectation_suite_name,
        batch_kwargs=df
    )
    run_id: str = datetime.utcnow().isoformat().replace(":", "") + "Z"
    validation_result: Dict[str, str] = batch.validate(run_id=run_id)
    # if validation_result["success"]:
    #     print("This batch is valid for {0:s}".format(data_asset_name))
    # else:
    #     print("This batch is not valid for {0:s}".format(data_asset_name))
    return validation_result
