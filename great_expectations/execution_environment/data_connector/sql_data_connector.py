import random
import datetime
from hashlib import md5
import sqlite3
import yaml
from typing import List, Dict, Tuple, Any
import json

import pandas as pd
import sqlalchemy as sa

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    BatchSpec,
    BatchMarkers,
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

    def _refresh_data_references_cache(self):
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
            column_names = self._get_column_names_from_splitter_kwargs(data_asset["splitter_kwargs"])
            partition_definition_list = [dict(zip(column_names, row))  for row in rows]

            # TODO Abe 20201029 : Apply sorters to partition_definition_list here
            # TODO Will 20201102 : add sorting code here

            self._data_references_cache[data_asset_name] = partition_definition_list
    
    def _get_column_names_from_splitter_kwargs(
        self,
        splitter_kwargs
    ) -> List[str]:
        column_names : List[str] = []

        if "column_names" in splitter_kwargs:
            column_names = splitter_kwargs["column_names"]
        elif "column_name" in splitter_kwargs:
            column_names = [splitter_kwargs["column_name"]]
        
        return column_names

    def get_available_data_asset_names(self):
        return list(self.assets.keys())
    
    def get_unmatched_data_references(self):
        if self._data_references_cache is None:
            raise ValueError("_data_references_cache is None. Have you called _refresh_data_references_cache yet?")

        return [k for k, v in self._data_references_cache.items() if v is None]        
    
    def get_batch_definition_list_from_batch_request(self, batch_request):
        self._validate_batch_request(batch_request=batch_request)

        batch_definition_list = []
        
        try:
            sub_cache = self._data_references_cache[batch_request.data_asset_name]
        except KeyError as e:
            raise KeyError(f"{self.__class__.__name__}.get_batch_definition_list_from_batch_request can't handle a batch_request without a valid data_asset_name")

        for partition_definition in sub_cache:
            batch_definition = BatchDefinition(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=batch_request.data_asset_name,
                partition_definition=PartitionDefinition(partition_definition)
            )
            if batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)
  
        return batch_definition_list

    def _get_data_reference_list_from_cache_by_data_asset_name(self, data_asset_name:str) -> List[str]:
        return self._data_references_cache[data_asset_name]

    def build_batch_spec(
        self,
        batch_definition: BatchDefinition
    ):
        data_asset_name = batch_definition.data_asset_name
        batch_spec = BatchSpec({
            "table_name" : data_asset_name,
            "partition_definition": batch_definition.partition_definition,
            **self.assets[data_asset_name],
        })

        return batch_spec

    def self_check(
        self,
        pretty_print=True,
        max_examples=3
    ):
        return_object = super().self_check(
            pretty_print=pretty_print,
            max_examples=max_examples
        )

        # Choose an example data_reference
        if pretty_print:
            print("\n\tChoosing an example data reference...")

        example_data_reference =  None
        for data_asset_name, data_asset_return_obj in return_object["data_assets"].items():
            # print(data_asset_name)
            # print(json.dumps(data_asset_return_obj["example_data_references"], indent=2))
            if data_asset_return_obj["batch_definition_count"] > 0:
                example_data_reference = random.choice(data_asset_return_obj["example_data_references"])
                break

        if pretty_print:
            print(f"\t\tReference chosen: {example_data_reference}")

        #...and fetch it.
        if pretty_print:
            print(f"\n\t\tFetching batch data..")
        batch_data, batch_spec, batch_markers = self.get_batch_data_and_metadata(
            BatchDefinition(
                execution_environment_name=self.execution_environment_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
                partition_definition=PartitionDefinition(example_data_reference),
            )
        )
        rows = batch_data.fetchall()
        return_object["example_data_reference"] = {
            "batch_spec" : batch_spec,
            "n_rows" : len(rows),
        }

        if pretty_print:
            print(f"\n\t\tShowing 5 rows")
            print(pd.DataFrame(rows[:5]))
        
        return return_object

    ### Splitter methods for listing partitions ###

    def _split_on_whole_table(
        self,
        table_name: str,
        column_name: str,
    ):
        """'Split' by returning the whole table"""

        return [0]


    def _split_on_column_value(
        self,
        table_name: str,
        column_name: str,
    ):
        """Split using the values in the named column"""
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
        """Convert the values in the named column to the given date_format, and split on that"""
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
        """Divide the values in the named column by `divisor`, and split on that"""
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

    def _split_on_mod_integer(
        self,
        table_name: str,
        column_name: str,
        mod:int
    ):
        """Divide the values in the named column by `divisor`, and split on that"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select([
            sa.func.distinct(
                sa.cast(
                    sa.column(column_name) % mod,
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
        """Split on the joint values in the named columns"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

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
        """Note: this method is experimental. It does not work with all SQL dialects.
        """
        # query = f"SELECT MD5(\"{self.column_name}\") = {matching_hash}) AS hashed_var FROM {self.table_name}"

        return sa.select([
            sa.func.md5(
                sa.column(column_name)
            )
        ]).select_from(
            sa.text(table_name)
        )
