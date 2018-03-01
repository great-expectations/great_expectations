import unittest

import great_expectations as ge

import json
from great_expectations.connections import SqlConnection, SparkSqlConnection
# from pyspark import SparkContext

class TestConnections(unittest.TestCase):

    def test_SqlConnection(self):

		my_auth = json.loads(open(auth))
		my_conn = SqlAlchemyConnection(**my_auth)

		my_conn.list_tables()

		my_df = my_conn.get_table("TableName")
		# my_df.expect...


		my_df.validate()

    def test_SparkSqlConnection(self):

		my_auth = json.loads(open(auth))
		my_conn = SparkSQLConnection(**my_auth)

		my_conn.list_tables()

		my_df = my_conn.get_table("TableName")
		# my_df.expect...


		my_df.validate()
