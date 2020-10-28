import random
import datetime
from hashlib import md5
import sqlite3
import yaml
from typing import List, Dict

import pandas as pd

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)

class SqlDataConnector(DataConnector):
    def __init__(self,
        name:str,
        execution_environment_name: str,
        execution_engine,
        assets: List[Dict],
    ):
        self._assets = assets
        
        # !!!
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
                
            splitter_column = data_asset["splitter"]["column_name"]
                
            splits = list(pd.read_sql(f"SELECT DISTINCT({splitter_column}) FROM {table_name}", self.db)[splitter_column])

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
