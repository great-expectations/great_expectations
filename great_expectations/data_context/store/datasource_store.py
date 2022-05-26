import random
import uuid
from typing import Union

from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)


class DatasourceStore(ConfigurationStore):
    """
    A DatasourceStore manages Datasources for the DataContext.
    """

    _configuration_class = DatasourceConfig

    def serialization_self_check(self, pretty_print: bool) -> None:
        """
        Fufills the abstract method defined by the parent class.
        See `ConfigurationStore` for more details.
        """
        test_datasource_name = f"datasource_{''.join([random.choice(list('0123456789ABCDEF')) for _ in range(20)])}"
        test_datasource_configuration = DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SqlAlchemyExecutionEngine",
                "credentials": {
                    "drivername": "postgresql+psycopg2",
                    "host": "localhost",
                    "port": "5432",
                    "username": "postgres",
                    "password": "postgres",
                    "database": "postgres",
                },
            },
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "name": "whole_table",
                },
            },
        )

        test_key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if self.ge_cloud_mode:
            test_key = self.key_class(
                resource_type="contract", ge_cloud_id=str(uuid.uuid4())
            )
        else:
            test_key = self.key_class(configuration_key=test_datasource_name)

        if pretty_print:
            print(f"Attempting to add a new test key {test_key} to Datasource store...")

        self.set(key=test_key, value=test_datasource_configuration)
        if pretty_print:
            print(f"\tTest key {test_key} successfully added to Datasource store.\n")
            print(
                f"Attempting to retrieve the test value associated with key {test_key} from Datasource store..."
            )

        test_value = self.get(key=test_key)
        if pretty_print:
            print(
                f"\tTest value successfully retrieved from Datasource store: {test_value}\n"
            )
            print(f"Cleaning up test key {test_key} and value from Datasource store...")

        test_value = self.remove_key(key=test_key)
        if pretty_print:
            print(
                f"\tTest key and value successfully removed from Datasource store: {test_value}\n"
            )
