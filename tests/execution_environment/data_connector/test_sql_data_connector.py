import yaml
import json
from great_expectations.execution_environment.data_connector import (
    SqlDataConnector,
)

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionDefinition,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config
)
from tests.test_utils import (
    create_fake_data_frame,
    create_files_in_directory,
)

from great_expectations.data_context.util import instantiate_class_from_config

def test_basic_self_check():
    my_data_connector = SqlDataConnector(**yaml.load("""
    name: my_sql_data_connector
    execution_environment_name: FAKE_EE_NAME

    # data_assets:
    assets:
        events_df:
            #table_name: events # If table_name is omitted, then the table_name defaults to the asset name
            splitter:
                column_name: date
    """, yaml.FullLoader))

    report = my_data_connector.self_check()
    print(json.dumps(report, indent=2))

    assert report == {
        "class_name": "SqlDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "events_df"
        ],
        "data_assets": {
            "events_df": {
                "batch_definition_count": 30,
                "example_data_references": [
                    "2020-01-01",
                    "2020-01-02",
                    "2020-01-03"
                ]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }