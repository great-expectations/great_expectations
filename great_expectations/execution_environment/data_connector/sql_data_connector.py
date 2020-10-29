import random
import datetime
from hashlib import md5
import sqlite3
import yaml
from typing import List, Dict

import pandas as pd

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)

class Splitter(object):
    def __init__(
        self,
        execution_engine,
    ):
        # TODO: Replace with a real execution_engine
        self.db = execution_engine
        # self._execution_engine = execution_engine


    @classmethod
    def get_splits():
        raise NotImplementedError

class ColumnValueSplitter(Splitter):
    def __init__(
        self,
        execution_engine,
        table_name: str,
        column_name: str,
        transformation_method: str = None,
        transformation_method_kwargs: dict = None,
    ):
        self.table_name = table_name
        self.column_name = column_name

        super().__init__(execution_engine)
    
    def get_splits(self):
        # TODO: Replace with real execution_engine methods
        splits = list(pd.read_sql(f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}", self.db)[self.column_name])

        return splits


class SqlDataConnector(DataConnector):
    def __init__(self,
        name:str,
        execution_environment_name: str,
        execution_engine,
        assets: List[Dict],
    ):
        self._assets = assets
        
        # TODO: Switch this over to use a real ExecutionEngine.
        self.db = execution_engine

        super(SqlDataConnector, self).__init__(
            name=name,
            execution_environment_name=execution_environment_name,
        )
    
    @property
    def assets(self) -> Dict[str, Asset]:
        return self._assets

    def refresh_data_references_cache(self):
        self._data_references_cache = {}
        
        for data_asset_name in self._assets:
            data_asset = self._assets[data_asset_name]
            if "table_name" in data_asset:
                table_name = data_asset["table_name"]
            else:
                table_name = data_asset_name
            
            _splitter = instantiate_class_from_config(
                config=data_asset["splitter"],
                runtime_environment={
                    "table_name": table_name,
                    "execution_engine": self.db,
                },
                config_defaults={
                    "module_name": "great_expectations.execution_environment.data_connector.sql_data_connector",
                }
            )

            splits = _splitter.get_splits()

            self._data_references_cache[data_asset_name] = splits
            
    def get_available_data_asset_names(self):
        return list(self.assets.keys())
    
    def get_unmatched_data_references(self):
        if self._data_references_cache is None:
            raise ValueError("_data_references_cache is None. Have you called refresh_data_references_cache yet?")

        return [k for k, v in self._data_references_cache.items() if v is None]        
    
    def get_batch_definition_list_from_batch_request(self, batch_request):
        batch_definition_list = []
        
        sub_cache = self._data_references_cache[batch_request.data_asset_name]
        for batch_definition in sub_cache:
            if self._batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)
  
        return batch_definition_list

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name:str) -> List[str]:
        return self._data_references_cache[data_asset_name]
