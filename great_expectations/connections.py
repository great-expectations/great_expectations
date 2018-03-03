import json

import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData
# import pyspark

from great_expectations.dataset import SqlDataSet
import great_expectations as ge

class GreatExpectationsConnection(object):
	
	def get_dataset_list(self):
		raise NotImplementedError()

	def get_datasets(self, table_name):
		raise NotImplementedError()


class LocalFilePathConnection(GreatExpectationsConnection):
	def __init__(self, connection_string=None):
		super(LocalFilePathConnection, self).__init__()

		if connection_string:
			self.engine = create_engine(connection_string)
			self.conn = self.engine.connect()

			self.metadata = MetaData(self.engine)
			self.metadata.reflect()

	def get_dataset_list(self):
		return self.engine.table_names()

	def get_dataset(self, table_name, expectations_config=None):
		table = self.metadata.tables[table_name]
		table.__class__ = SqlDataSet
		table.initialize_expectations(expectations_config)

		return table


class SqlAlchemyConnection(GreatExpectationsConnection):
	def __init__(self, connection_string):
		super(SqlAlchemyConnection, self).__init__()

		self.engine = create_engine(connection_string)
		self.conn = self.engine.connect()

		self.metadata = MetaData(self.engine)
		self.metadata.reflect()

	def get_dataset_list(self):
		return self.engine.table_names()

	def get_dataset(self, table_name, expectations_config=None):
		table = self.metadata.tables[table_name]
		table.__class__ = SqlDataSet
		table.initialize_expectations(expectations_config)

		return table


class SparkSqlConnection():
	pass


