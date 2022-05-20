import os

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

def test_ge():


    CONNECTION_STRING = os.environ.get("DB_URL")

    context = ge.get_context()

    datasource_config = {
        "name": "my_redshift_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": CONNECTION_STRING,
            "create_temp_table": False
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetSqlDataConnector",
                "include_schema_name": True,
            },
        },
    }

    # Please note this override is only to provide good UX for docs and tests.
    # In normal usage you'd set your path directly in the yaml above.
    datasource_config["execution_engine"]["connection_string"] = CONNECTION_STRING

    context.test_yaml_config(yaml.dump(datasource_config))

    context.add_datasource(**datasource_config)

    # First test for RuntimeBatchRequest using a query
    batch_request = RuntimeBatchRequest(
        datasource_name="my_redshift_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="default_name",  # this can be anything that identifies this data
        runtime_parameters={"query": "SELECT * from date LIMIT 10"},
        batch_identifiers={"default_identifier_name": "default_identifier"},
        batch_spec_passthrough={
            "create_temp_table": False
        }
    )

    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    print(validator.head())

    # NOTE: The following code is only for testing and can be ignored by users.

    assert isinstance(validator, ge.validator.validator.Validator)
