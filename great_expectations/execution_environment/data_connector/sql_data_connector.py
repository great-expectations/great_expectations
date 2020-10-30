import random
import datetime
from hashlib import md5
import sqlite3
import yaml
from typing import List, Dict

import pandas as pd
import sqlalchemy as sa

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

class SqlDataConnector(DataConnector):
    def __init__(self,
        name:str,
        execution_environment_name: str,
        execution_engine,
        assets: List[Dict],
    ):
        self._assets = assets
        
        super(SqlDataConnector, self).__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
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
            
            splitter_fn = getattr(self, data_asset["splitter_method"])
            split_query = splitter_fn(
                table_name=table_name,
                **data_asset["splitter_kwargs"]
            )

            rows = self._execution_engine.engine.execute(split_query).fetchall()

            # Zip up split parameters with column names
            column_names : List[str] = []
            if "column_names" in data_asset["splitter_kwargs"]:
                column_names = data_asset["splitter_kwargs"]["column_names"]
            elif "column_name" in data_asset["splitter_kwargs"]:
                column_names = [data_asset["splitter_kwargs"]["column_name"]]
            
            splits = [dict(zip(column_names, row))  for row in rows]

            # TODO Abe 20201029 : Apply sorters to splits here

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

    ### Splitter methods ###

    def _split_on_column_value(
        self,
        table_name: str,
        column_name: str,
    ):
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

        return sa.select([
            sa.func.distinct(
                sa.column(column_name)
            )
        ]).select_from(
            sa.text(table_name)
        )

    def _split_on_converted_datetime(
        self,
        table_name: str,
        column_name: str,
        date_format_string: str='%Y-%m-%d',
    ):
        # query = f"SELECT DISTINCT( strftime(\"{date_format_string}\", \"{self.column_name}\")) as my_var FROM {self.table_name}"

        return sa.select([
            sa.func.distinct(
                sa.func.strftime(
                    date_format_string,
                    sa.column(column_name),
                )
            )
        ]).select_from(
            sa.text(table_name)
        )

    def _split_on_divided_integer(
        self,
        table_name: str,
        column_name: str,
        divisor:int
    ):
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select([
            sa.func.distinct(
                sa.cast(
                    sa.column(column_name) / divisor,
                    sa.Integer
                )
            )
        ]).select_from(
            sa.text(table_name)
        )

    def _split_on_divided_integer(
        self,
        table_name: str,
        column_name: str,
        divisor:int
    ):
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select([
            sa.func.distinct(
                sa.cast(
                    sa.column(column_name) / divisor,
                    sa.Integer
                )
            )
        ]).select_from(
            sa.text(table_name)
        )

    def _split_on_multi_column_values(
        self,
        table_name: str,
        column_names: List[str],
    ):
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"
        # splits = list(pd.read_sql(query, self.db)[self.column_name])

        return sa.select([
                sa.column(column_name) for column_name in column_names
            ]).distinct().select_from(
                sa.text(table_name)
            )
        
    def _split_on_hashed_column(
        self,
        table_name: str,
        column_name: str,
        hash_digits: int,
    ):
        # query = f"SELECT MD5(\"{self.column_name}\") = {matching_hash}) AS hashed_var FROM {self.table_name}"

        return sa.select([
            sa.func.md5(
                sa.column(column_name)
            )
        ]).select_from(
            sa.text(table_name)
        )
