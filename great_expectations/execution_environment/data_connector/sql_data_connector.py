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

def generate_ascending_list_of_datetimes(
    k,
    start_date=datetime.date(2020,1,1),
    end_date=datetime.date(2020,12,31)
):
    start_time = datetime.datetime(start_date.year, start_date.month, start_date.day)
    days_between_dates = (end_date - start_date).total_seconds()
    
    datetime_list = [start_time + datetime.timedelta(seconds=random.randrange(days_between_dates)) for i in range(k)]
    datetime_list.sort()
    return datetime_list

class SqlDataConnector(DataConnector):
    def __init__(self,
        name:str,
        execution_environment_name: str,
        assets: List[Dict],
    ):
        self._assets = assets
        
        self.db = sqlite3.connect("file::memory:")

        k = 120
        random.seed(1)

        timestamp_list = generate_ascending_list_of_datetimes(k, end_date=datetime.date(2020,1,31))
        date_list = [datetime.date(ts.year, ts.month, ts.day) for ts in timestamp_list]

        batch_ids = [random.randint(0,10) for i in range(k)]
        batch_ids.sort()

        session_ids = [random.randint(2,60) for i in range(k)]
        session_ids.sort()
        session_ids = [i-random.randint(0,2) for i in session_ids]

        events_df = pd.DataFrame({
            "id" : range(k),
            "batch_id" : batch_ids,
            "date" : date_list,
            "y" : [d.year for d in date_list],
            "m" : [d.month for d in date_list],
            "d" : [d.day for d in date_list],
            "timestamp" : timestamp_list,
            "session_id" : session_ids,
            "event_type" : [random.choice(["start", "stop", "continue"]) for i in range(k)],
            "favorite_color" : ["#"+"".join([random.choice(list("0123456789ABCDEF")) for j in range(6)]) for i in range(k)]
        })

        # events_df.to_sql("events_df", self.db)

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
            # batch_definitions = []
            # for split in splits:
            #     batch_definitions.append(BatchDefinition(
            #         execution_environment_name=self.execution_environment_name,
            #         data_connector_name=self.name,
            #         data_asset_name=data_asset_name,
            #         partition_definition=PartitionDefinition({
            #             splitter_column: split,
            #         })
            #     ))
            
            # #Maybe make this a list of BatchRequests...?
            # self._data_references_cache[data_asset_name] = batch_definitions
            
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
