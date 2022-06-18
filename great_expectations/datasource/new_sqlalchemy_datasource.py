from typing import List

import pandas as pd
import sqlalchemy as sa

from great_expectations.core.batch import Batch
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.misc_types import (
    BatchSpecPassthrough,
    DataConnectorQuery,
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.data_connector.util import (
    convert_batch_identifiers_to_data_reference_string_using_regex,
)
from great_expectations.datasource.new_new_new_datasource import NewNewNewDatasource
from great_expectations.validator.validator import Validator


class NewSqlAlchemyDatasource(NewNewNewDatasource):
    def __init__(
        self,
        name: str,
        connection_string: str,
    ) -> None:
        self._name = name
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
            data_connector_query=DataConnectorQuery(),
            batch_spec_passthrough=BatchSpecPassthrough(),
        )
        return self.get_validator(batch_request)

    def list_tables(self) -> List[str]:
        self._connect_engine()

        inspector = sa.inspect(self._engine)
        schemas = inspector.get_schema_names()

        for schema in schemas:
            print("schema: %s" % schema)
            for table_name in inspector.get_table_names(schema=schema):
                print("Table: %s" % table_name)
                # for column in inspector.get_columns(table_name, schema=schema):
                #     print("Column: %s" % column)

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
