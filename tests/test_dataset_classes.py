import unittest

import great_expectations as ge

import json
from great_expectations.dataset import SqlDataSet, SparkSqlDataSet

class TestSparkSqlDataFrame(unittest.TestCase):

    def test_SparkSqlDataFrame(self):
        #FIXME: Unsuppress.
        return

        my_df = ge.read_csv(
            "my_file.csv",
            SparkSqlDataSet,
            schema
        )
        # my_df.expect...


        my_df.validate()


class TestSqlDataFrame(unittest.TestCase):

    def test_SqlDataFrame(self):
        #FIXME: Unsuppress.
        return

        raise NotImplementedError()

