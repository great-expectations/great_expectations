import json
import sqlalchemy
# import pyspark

class GreatExpectationsConnection():
	
	def list_tables(self):
		raise NotImplementedError()

	def get_table(self, table_name):
		raise NotImplementedError()


class SqlConnection():
	pass

class SparkSqlConnection():
	pass

