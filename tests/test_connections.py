#FIXME: Rename this file to test_connections.py

import unittest

import great_expectations as ge

import json
from great_expectations.connections import SqlAlchemyConnection, SparkSqlConnection
# from pyspark import SparkContext

class TestConnections(unittest.TestCase):

    def test_SqlConnection(self):

        #Instantiate a connection
        my_conn = ge.get_connection("SqlAlchemy", connection_string="sqlite:///tests/test_fixtures/chinook.db")
        # my_conn = SqlConnection("sqlite:///tests/test_fixtures/chinook.db")

        #List tables
        print(my_conn.get_dataset_list())

        #Use the connection to fetch a dataset
        my_dataset = my_conn.get_dataset("albums")

        #This dataset is subclassed from sqlalchemy.sql.schema.Table, instead of pandas.DataFrame
        #It's also subclassed from ge.DataSet, so we can invoke expectations:
        my_dataset.expect_column_to_exist("AlbumId")

        #...and validation.
        print(my_dataset.validate())


    def test_FilepathConnection(self):

        #Instantiate a connection, with is_recursive=False
        my_conn = ge.get_connection(
            "Filepath",
            filepath="examples/data",
            is_recursive=False,
        )
        self.assertEqual(
            set(my_conn.get_dataset_list()),
            set(['Titanic.csv', 'FAO-Rice-Production-Asia.csv'])
        )


        #Instantiate a connection, with is_recursive=True
        my_conn = ge.get_connection(
            "Filepath",
            filepath="examples/data",
            is_recursive=True,
        )
        self.assertEqual(
            set(my_conn.get_dataset_list()),
            set(['Titanic.csv', 'FAO-Rice-Production-Asia.csv'])
        )


        #Instantiate a connection, with is_recursive=True
        my_conn = ge.get_connection(
            "Filepath",
            filepath="examples",
            is_recursive=True,
        )
        self.assertEqual(
            set(my_conn.get_dataset_list()),
            set(['data/Titanic.csv', 'data/FAO-Rice-Production-Asia.csv'])
        )

        # with self.assertRaises:


        #Use the connection to fetch a dataset
        my_dataset = my_conn.get_dataset("data/Titanic.csv")

        self.assertEqual(
            my_dataset.expect_column_to_exist("AlbumId")['success'],
            False
        )

        self.assertEqual(
            my_dataset.expect_column_to_exist("Age")['success'],
            True
        )

        #...and validation.
        print(my_dataset.validate())


    def test_SparkSqlConnection(self):
        #FIXME: Unsuppress.
        return

        my_auth = json.loads(open(auth))
        my_conn = SparkSQLConnection(**my_auth)

        my_conn.list_tables()

        my_df = my_conn.get_table("albums")
        # my_df.expect...


        my_df.validate()
