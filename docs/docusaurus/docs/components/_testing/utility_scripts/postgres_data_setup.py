"""
IntegrationTestFixture template:

    IntegrationTestFixture(
        # To test, run:
        # pytest --docs-tests -k "<name_here>" tests/integration/test_script_runner.py
        name="<name_here>",
        user_flow_script="",
        data_context_dir="docs/docusaurus/docs/components/_testing/create_datasource/great_expectations/",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="docs/docusaurus/docs/components/_testing/_utiity_scripts/_postgres_data_setup.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),

"""

from tests.integration.db.taxi_data_utils import load_data_into_test_database


def setup():
    # add test_data to database for testing
    load_data_into_test_database(
        table_name="postgres_taxi_data",
        csv_path="./data/yellow_tripdata_sample_2020-01.csv",
        convert_colnames_to_datetime=["pickup_datetime"],
        connection_string="postgresql+psycopg2://postgres:@localhost/test_ci",
    )
