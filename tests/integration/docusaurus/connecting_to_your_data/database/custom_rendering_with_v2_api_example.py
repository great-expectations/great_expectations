"""
This tests custom expectation rendering with the V2 API as described at
https://docs.greatexpectations.io/en/latest/reference/spare_parts/data_docs_reference.html#customizing-data-docs.

TOOD(jdimatteo): run this test as part of Azure pipeline.
"""
import os
import shutil
from pathlib import Path

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig

CONNECTION_STRING = "sqlite:///data/yellow_tripdata.db"
html_directory = os.path.join(os.path.abspath(os.path.dirname(__file__)), "html")


def create_context():
    data_docs_sites = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": False,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": html_directory,
            },
            "site_section_builders": {
                "validations": {
                    "renderer": {
                        "module_name": "great_expectations.render.renderer",
                        "class_name": "ValidationResultsPageRenderer",
                        "column_section_renderer": {
                            "class_name": "ValidationResultsColumnSectionRenderer",
                            "table_renderer": {
                                "module_name": "custom_expectation_rendering_v2_api",
                                "class_name": "ValidationResultsTableContentBlockRendererWithCustomExpectations",
                            },
                        },
                    }
                }
            },
        }
    }
    database_path = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "../../../../../tests/test_sets/taxi_yellow_trip_data_samples/sqlite/yellow_tripdata.db",
    )
    datasources = {
        "my_sqlite_datasource": {
            "data_asset_type": {
                "module_name": "custom_expectation_rendering_v2_api",
                "class_name": "SqlAlchemyDatasetWithCustomExpectations",
            },
            "class_name": "SqlAlchemyDatasource",
            "module_name": "great_expectations.datasource",
            "credentials": {
                "url": f"sqlite:///file:{database_path}?mode=ro&uri=true&check_same_thread=false"
            },
        }
    }

    return BaseDataContext(
        project_config=DataContextConfig(
            data_docs_sites=data_docs_sites,
            anonymous_usage_statistics={"enabled": False},
            datasources=datasources,
            validation_operators={
                "my_validation_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
            stores={
                "my_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend",
                    },
                },
                "my_validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "InMemoryStoreBackend",
                    },
                },
                "my_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
            },
            config_version=2,
            expectations_store_name="my_expectations_store",
            validations_store_name="my_validations_store",
            evaluation_parameter_store_name="my_evaluation_parameter_store",
            plugins_directory=None,
        )
    )


def create_batch(context, suite):
    batch = context.get_batch(
        {
            "table": "yellow_tripdata_sample_2019_01",
            "datasource": "my_sqlite_datasource",
        },
        suite,
    )
    batch.expect_table_to_exist(table_name="yellow_tripdata_sample_2019_01")
    batch.expect_column_values_to_not_be_null(column="vendor_id")
    return batch


def assert_custom_rendering_present_in_html():
    html_file_paths = list(Path(html_directory).glob("validations/**/*.html"))
    assert len(html_file_paths) == 1
    with open(html_file_paths[0]) as html_file:
        html_file_contents = html_file.read()
        assert "**{" not in html_file_contents
        assert "Table found" in html_file_contents
        assert "Table is required" in html_file_contents


def main():
    if os.path.exists(html_directory):
        shutil.rmtree(html_directory)

    context = create_context()
    suite = context.create_expectation_suite(
        expectation_suite_name="my_expectation_suite", overwrite_existing=True
    )
    batch = create_batch(context, suite)
    context.run_validation_operator(
        "my_validation_operator", assets_to_validate=[batch]
    )
    context.open_data_docs()
    assert_custom_rendering_present_in_html()


if __name__ == "__main__":
    main()
