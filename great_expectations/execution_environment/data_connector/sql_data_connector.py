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

class Splitter(object):
    def __init__(
        self,
        execution_engine,
    ):
        # TODO: Replace with a real execution_engine
        # self.db = execution_engine
        self._execution_engine = execution_engine


    @classmethod
    def get_splits():
        raise NotImplementedError

class ColumnValueSplitter(Splitter):
    def __init__(
        self,
        execution_engine,
        table_name: str,
        column_name: str,
        transformation_method: str = "_vanilla_splitter",
        transformation_method_kwargs: dict = None,
    ):
        self.table_name = table_name
        self.column_name = column_name
        self.transformation_method = transformation_method
        self.transformation_method_kwargs = transformation_method_kwargs or {}

        super().__init__(execution_engine)
    
    def get_splits(self) -> List[str]:
        # NOTE: How do we feel about using getattr here?
        transformation_fn = getattr(self, self.transformation_method)
        splits = transformation_fn(**self.transformation_method_kwargs)
        return splits


    # Move these into the DataConnector, or into utility methods.
    # Don't use raw SQL --> use SQLalchemy methods

    def _vanilla_splitter(self):
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"
        # splits = list(pd.read_sql(query, self.db)[self.column_name])

        rows = self._execution_engine.engine.execute(
            sa.select([
                sa.func.distinct(
                    sa.column(self.column_name)
                )
            ]).select_from(
                sa.text(self.table_name)
            )
        ).fetchall()

        splits = [row[0] for row in rows]
        return splits

    def _convert_datetime_to_date(
        self,
        date_format_string: str='%Y-%m-%d',
    ):
        # TODO: Replace with real execution_engine methods
        # query = f"SELECT DISTINCT( strftime(\"{date_format_string}\", \"{self.column_name}\")) as my_var FROM {self.table_name}"
        # splits = list(pd.read_sql(query, self.db)["my_var"])

        rows = self._execution_engine.engine.execute(
            sa.select([
                sa.func.distinct(
                    sa.func.strftime(
                        date_format_string,
                        sa.column(self.column_name),
                    )
                )
            ]).select_from(
                sa.text(self.table_name)
            )
        ).fetchall()
        splits = [row[0] for row in rows]

        return splits

    def _divide_int(
        self,
        divisor:int
    ):
        # TODO: Replace with real execution_engine methods
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"
        # splits = list(pd.read_sql(query, self.db)["my_var"])

        rows = self._execution_engine.engine.execute(
            sa.select([
                sa.func.distinct(
                    sa.cast(
                        sa.column(self.column_name) / divisor,
                        sa.Integer
                    )
                )
            ]).select_from(
                sa.text(self.table_name)
            )
        ).fetchall()
        splits = [row[0] for row in rows]

        return splits

    def _random_hash(self, matching_hashes="0"):
        # TODO: Replace with real execution_engine methods
        query = f"SELECT MD5(\"{self.column_name}\") = {matching_hash}) AS hashed_var FROM {self.table_name}"
        print(query)
        splits = list(pd.read_sql(query, self.db)["hashed_var"])

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
