import os

import pandas as pd
import pytest

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def titanic_df() -> pd.DataFrame:
    path = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_fixtures",
            "configuration_for_testing_v2_v3_migration",
            "data",
            "Titanic.csv",
        ),
    )
    df = pd.read_csv(path)
    return df


@pytest.fixture
def sqlite_runtime_batch_request() -> RuntimeBatchRequest:
    return RuntimeBatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="titanic",
        runtime_parameters={"query": "SELECT * FROM titanic LIMIT 100"},
        batch_identifiers={"default_identifier_name": "test_identifier"},
        batch_spec_passthrough={"create_temp_table": False},
    )


@pytest.fixture
def sqlite_batch_request() -> BatchRequest:
    return BatchRequest(
        datasource_name="my_sqlite_db_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="titanic",
        batch_spec_passthrough={"create_temp_table": False},
    )
