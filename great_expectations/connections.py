import json

import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData
# import pyspark

from great_expectations.dataset import SqlDataSet

class GreatExpectationsConnection(object):
	
	def list_tables(self):
		raise NotImplementedError()

	def get_table(self, table_name):
		raise NotImplementedError()


class SqlConnection(GreatExpectationsConnection):
	def __init__(self, connection_string=None):
		#FIXME: provide an additional API that allows connection strings to be generated from arguments.

		super(SqlConnection, self).__init__()

		if connection_string:
			self.engine = create_engine(connection_string)
			self.conn = self.engine.connect()

			self.metadata = MetaData(self.engine)
			self.metadata.reflect()

	def get_table_list(self):
		return self.engine.table_names()

	def get_table(self, table_name):
		table = SqlDataSet(
			table=self.metadata.tables[table_name]
		)
		return self.metadata.tables[table_name]


class SparkSqlConnection():
	pass


