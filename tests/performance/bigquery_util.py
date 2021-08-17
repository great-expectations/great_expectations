import os

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)


def setup_checkpoint(number_of_tables: int, html_dir: str) -> SimpleCheckpoint:
    checkpoint_name = "my_checkpoint"
    datasource_and_dataconnector_name = "my_datasource_and_dataconnector"

    # These tables are created by "setup_bigquery_tables_for_performance_test.sh", with numbering from 1 to 100.
    assert 1 <= number_of_tables <= 100
    suite_and_asset_names = [
        f"bikeshare_trips_{i}" for i in range(1, number_of_tables + 1)
    ]

    context = _create_context(
        datasource_and_dataconnector_name, suite_and_asset_names, html_dir
    )
    for suite_name in suite_and_asset_names:
        _add_expectation_configuration(context=context, suite_name=suite_name)

    return _add_checkpoint(
        context,
        datasource_and_dataconnector_name,
        checkpoint_name,
        suite_and_asset_names,
    )


def expected_validation_results() -> list[dict]:
    return [
        {
            "meta": {},
            "expectation_config": {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "trip_id"},
                "meta": {},
            },
            "result": {
                "element_count": 1342066,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [],
            },
            "exception_info": {
                "raised_exception": False,
                "exception_traceback": None,
                "exception_message": None,
            },
            "success": True,
        }
    ]


def _create_context(
    datasource_and_dataconnector_name: str,
    asset_names: list[str],
    html_dir: str,
) -> BaseDataContext:
    store_backend = {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": html_dir,
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


def _add_checkpoint(
    context: BaseDataContext,
    datasource_and_dataconnector_name: str,
    checkpoint_name: str,
    suite_and_asset_names=[],
) -> SimpleCheckpoint:
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
    return context.add_checkpoint(
        name=checkpoint_name,
        class_name="SimpleCheckpoint",
        validations=validations,
        run_name_template="my_run_name",
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
