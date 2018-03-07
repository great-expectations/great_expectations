import json
import glob
import os

import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy import MetaData
# import pyspark

from great_expectations.dataset import SqlDataSet
import great_expectations as ge

class GreatExpectationsConnection(object):
    
    def get_dataset_list(self):
        raise NotImplementedError()

    def get_datasets(self, dataset_name, expectations_config):
        raise NotImplementedError()


class FilepathConnection(GreatExpectationsConnection):
    def __init__(self, filepath, is_recursive=False, allowed_file_suffixes=['txt', 'csv']):
        super(FilepathConnection, self).__init__()

        self.filepath = filepath
        self.is_recursive = is_recursive
        self.allowed_file_suffixes = allowed_file_suffixes

    def get_dataset_list(self):
        if self.is_recursive:
            filename_list = []
            for root, dirs, files in os.walk(self.filepath):
                for file_ in files:
                    if file_.endswith(tuple(self.allowed_file_suffixes)):
                        new_filename = os.path.join(root, file_)
                        filename_list.append(
                            os.path.relpath(new_filename, self.filepath)
                        )
        
        else:
            filename_list = []
            for ext in self.allowed_file_suffixes:
                glob_str = self.filepath+'/*.'+ext
                filename_list.extend(
                    glob.glob(glob_str)
                )
            filename_list = [os.path.basename(path) for path in filename_list]
        
        return filename_list

    def get_dataset(self, dataset_name, expectations_config=None):
        df = ge.read_csv(self.filepath+"/"+dataset_name)
        return df

class SqlAlchemyConnection(GreatExpectationsConnection):
    def __init__(self, connection_string):
        super(SqlAlchemyConnection, self).__init__()

        self.engine = create_engine(connection_string)
        self.conn = self.engine.connect()

        self.metadata = MetaData(self.engine)
        self.metadata.reflect()

    def get_dataset_list(self):
        return self.engine.table_names()

    def get_dataset(self, dataset_name, expectations_config=None):
        table = self.metadata.tables[dataset_name]
        table.__class__ = SqlDataSet
        table.initialize_expectations(expectations_config)

        return table


class SparkSqlConnection():
    pass


