from unittest import mock

import pandas as pd
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset


def test_sparkdfdataset_persist(spark_session):
    df = pd.DataFrame({"a": [1, 2, 3]})
    sdf = spark_session.createDataFrame(df)
    sdf.persist = mock.MagicMock()
    _ = SparkDFDataset(sdf, persist=True)
    sdf.persist.assert_called_once()

    sdf = spark_session.createDataFrame(df)
    sdf.persist = mock.MagicMock()
    _ = SparkDFDataset(sdf, persist=False)
    sdf.persist.assert_not_called()

    sdf = spark_session.createDataFrame(df)
    sdf.persist = mock.MagicMock()
    _ = SparkDFDataset(sdf)
    sdf.persist.assert_called_once()
