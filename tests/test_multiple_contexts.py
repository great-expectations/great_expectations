import unittest

import great_expectations as ge

import json
from great_expectations.connections import SqlConnection, SparkSqlConnection
# from pyspark import SparkContext

class TestConnections(unittest.TestCase):

    def test_SqlConnection(self):

        my_conn = SqlConnection("sqlite:///tests/test_fixtures/chinook.db")

        print(my_conn.get_table_list())

        my_df = my_conn.get_table("albums")
        print json.dumps(
            my_df.expect_column_to_exist("AlbumId", catch_exceptions=True),
            indent=2
        )

        print my_df.validate()

        # assert False

    def test_SparkSqlConnection(self):
        #FIXME: Unsuppress.
        return

        my_auth = json.loads(open(auth))
        my_conn = SparkSQLConnection(**my_auth)

        my_conn.list_tables()

        my_df = my_conn.get_table("albums")
        # my_df.expect...


        my_df.validate()
