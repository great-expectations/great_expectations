#FIXME: Rename this file to test_connections.py

import unittest

import great_expectations as ge

import json
from great_expectations.connections import SqlConnection, SparkSqlConnection
# from pyspark import SparkContext

class TestConnections(unittest.TestCase):

    def test_SqlConnection(self):

        #Instantiate a connection
        my_conn = SqlConnection("sqlite:///tests/test_fixtures/chinook.db")

        #List tables
        print(my_conn.get_table_list())

        #Use the connection to fetch a dataset
        #FIXME: We should probably be consistent and call this `get_dataset`
        my_dataset = my_conn.get_table("albums")

        #This dataset is subclassed from sqlalchemy.sql.schema.Table, instead of pandas.DataFrame
        #It's also subclassed from ge.DataSet, so we can invoke expectations:
        my_dataset.expect_column_to_exist("AlbumId")

        #...and validation.
        print my_dataset.validate()


    def test_SparkSqlConnection(self):
        #FIXME: Unsuppress.
        return

        my_auth = json.loads(open(auth))
        my_conn = SparkSQLConnection(**my_auth)

        my_conn.list_tables()

        my_df = my_conn.get_table("albums")
        # my_df.expect...


        my_df.validate()
