# todo(jdimatteo): add a performance test change log and only run performance test when that file changes.
#  include git describe output in json

import os

from pytest_benchmark.fixture import BenchmarkFixture

from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def test_bikeshare_trips_1_table(benchmark: BenchmarkFixture):
    checkpoint_name = "my_checkpoint"
    suite_and_asset_name = (
        "bikeshare_trips_1"  # The table name is used as the asset and suite name.
    )
    datasource_and_dataconnector_name = "my_datasource_and_dataconnector"

    context = _create_context(datasource_and_dataconnector_name, [suite_and_asset_name])
    _add_expectation_configuration(context=context, suite_name=suite_and_asset_name)
    _add_checkpoint(
        context,
        datasource_and_dataconnector_name,
        checkpoint_name,
        [suite_and_asset_name],
    )

    result: CheckpointResult = benchmark.pedantic(
        context.run_checkpoint,
        kwargs={"checkpoint_name": checkpoint_name},
        iterations=1,
        rounds=1,
    )
    assert result.success, result


def _add_checkpoint(
    context: BaseDataContext,
    datasource_and_dataconnector_name: str,
    checkpoint_name: str,
    suite_and_asset_names=[],
):
    validations = [
        {
            "expectation_suite_name": suite_and_asset_name,
            "batch_request": {
                "datasource_name": datasource_and_dataconnector_name,
                "data_connector_name": datasource_and_dataconnector_name,
                "data_asset_name": suite_and_asset_name,
                "batch_spec_passthrough": {"create_temp_table": False},
            },
        }
        for suite_and_asset_name in suite_and_asset_names
    ]
    context.add_checkpoint(
        name=checkpoint_name, class_name="SimpleCheckpoint", validations=validations
    )


def _add_expectation_configuration(context: BaseDataContext, suite_name: str):
    suite = context.create_expectation_suite(expectation_suite_name=suite_name)
    # todo(jdimatteo) add more expectations to be more representative of use case I want to optimize
    suite.add_expectation(
        expectation_configuration=ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "trip_id"},
        )
    )
    # Save the expectation suite or else it doesn't show up in the data docs.
    context.save_expectation_suite(
        expectation_suite=suite, expectation_suite_name=suite_name
    )


def _html_dir() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "html")


def _create_context(
    datasource_and_dataconnector_name: str, asset_names: list[str]
) -> BaseDataContext:
    store_backend = {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": _html_dir(),
    }
    data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": store_backend,
        }
    }
    bigquery_project = os.environ["GE_TEST_BIGQUERY_PROJECT"]
    bigquery_dataset = os.environ.get(
        "GE_TEST_BIGQUERY_PEFORMANCE_DATASET", "performance_ci"
    )

    data_context_config = DataContextConfig(
        store_backend_defaults=InMemoryStoreBackendDefaults(),
        data_docs_sites=data_docs_sites,
        anonymous_usage_statistics={"enabled": False},
        datasources={
            datasource_and_dataconnector_name: {
                "class_name": "Datasource",
                "execution_engine": {
                    "class_name": "SqlAlchemyExecutionEngine",
                    "connection_string": f"bigquery://{bigquery_project}/{bigquery_dataset}",
                },
                "data_connectors": {
                    datasource_and_dataconnector_name: {
                        "class_name": "ConfiguredAssetSqlDataConnector",
                        "name": "whole_table",
                        "assets": {asset_name: {} for asset_name in asset_names},
                    },
                },
            },
        },
    )
    return BaseDataContext(project_config=data_context_config)
