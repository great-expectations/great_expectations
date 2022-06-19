from typing import Dict, List, Optional

import pandas as pd
import sqlalchemy as sa

from great_expectations.core.batch import Batch
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.misc_types import (
    BatchIdentifiers,
    PassthroughParameters,
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.new_new_new_datasource import NewNewNewDatasource
from great_expectations.datasource.new_sqlalchemy_data_asset import ConfiguredNewSqlAlchemyDataAsset
from great_expectations.validator.validator import Validator


class NewSqlAlchemyDatasource(NewNewNewDatasource):
    def __init__(
        self,
        name: str,
        connection_string: str,
    ) -> None:

        super().__init__(name=name)

        self._connection_string = connection_string
        self._engine = None

        #!!! This is a hack
        self._execution_engine = instantiate_class_from_config(
            config={
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            runtime_environment={"concurrency": None},
            config_defaults={"module_name": "great_expectations.execution_engine"},
        )

    def _connect_engine(self):
        if self._engine == None:
            self._engine = sa.create_engine(self._connection_string)

    def get_table(
        self,
        table: str,
    ) -> Validator:
        batch_request = NewConfiguredBatchRequest(
            datasource_name=self._name,
            data_asset_name=table,
            batch_identifiers=BatchIdentifiers(),
            passthrough_parameters=PassthroughParameters(),
        )
        return self.get_validator(batch_request)

    def list_tables(
        self,
        schema: Optional[str]= None,
        return_as:str = "strings",
    ) -> List[str]:
        self._connect_engine()

        inspector = sa.inspect(self._engine)
        schemas = inspector.get_schema_names()

        if return_as == "strings":

            table_names = []
            for schema_name in schemas:
                for table_name in inspector.get_table_names(schema=schema):
                    table_names.append(f"{schema_name}.{table_name}")

            return table_names

        elif return_as == "assets":

            assets = []
            for schema_name in schemas:
                for table_name in inspector.get_table_names(schema=schema):
                    new_asset = ConfiguredNewSqlAlchemyDataAsset(
                        name=f"{schema_name}.{table_name}",
                        datasource=self,
                        batch_identifiers=[],
                        table_name=table_name,
                        schema_name=schema_name,
                    )
                    assets.append(new_asset)
            
            return assets

        else:
            raise ValueError(f'return_as got unknown value {return_as}. Expected "strings" or "assets"')

    def add_asset(
        self,
        name: str,
        table_name: Optional[str] = None,
        schema_name: Optional[str] = None,
    ) -> ConfiguredNewSqlAlchemyDataAsset:

        new_asset = ConfiguredNewSqlAlchemyDataAsset(
            datasource=self,
            name=name,
        )

        self._assets[name] = new_asset

        return new_asset

    def update_assets(self, assets:List[ConfiguredNewSqlAlchemyDataAsset]) -> None:
        for asset in assets:
            if asset.name in self.assets:
                self.assets.update_asset(asset)
            else:
                self._assets[asset.name] = asset

    def get_batch(self, batch_request: NewConfiguredBatchRequest) -> Batch:
        self._connect_engine()

        df = pd.read_sql_table(
            table_name=batch_request.data_asset_name,
            con=self._engine,
        )

        batch = Batch(
            data=df,
            batch_request=batch_request,
        )

        return batch

    def get_validator(self, batch_request: NewConfiguredBatchRequest) -> Batch:
        batch = self.get_batch(batch_request)
        return Validator(
            execution_engine=self._execution_engine,
            expectation_suite=None,  # expectation_suite,
            batches=[batch],
        )

    @property
    def assets(self) -> Dict[str, ConfiguredNewSqlAlchemyDataAsset]:
        return self._assets
