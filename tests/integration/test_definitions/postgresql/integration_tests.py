from tests.integration.backend_dependencies import BackendDependencies
from tests.integration.integration_test_fixture import IntegrationTestFixture

postgresql_integration_tests = []


connecting_to_your_data = [
    IntegrationTestFixture(
        name="how_to_configure_credentials",
        user_flow_script="tests/integration/docusaurus/setup/configuring_data_contexts/how_to_configure_credentials.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="postgres_yaml_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/postgres_yaml_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="postgres_python_example",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/database/postgres_python_example.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

split_data = [
    IntegrationTestFixture(
        name="split_data_on_whole_table_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_whole_table.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_column_value_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_column_value.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_divided_integer_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_divided_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_mod_integer_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_mod_integer.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_hashed_column" for POSTGRESQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_hashed_column_postgres",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_hashed_column.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
    IntegrationTestFixture(
        name="split_data_on_multi_column_values_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_multi_column_values.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    IntegrationTestFixture(
        name="split_data_on_datetime_postgres",
        user_flow_script="tests/integration/db/test_sql_data_split_on_datetime_and_day_part.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    # TODO: <Alex>ALEX -- Uncomment next statement when "split_on_converted_datetime" for POSTGRESQL is implemented.</Alex>
    # IntegrationTestFixture(
    #     name="split_data_on_converted_datetime_postgres",
    #     user_flow_script="tests/integration/db/test_sql_data_split_on_converted_datetime.py",
    #     data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
    #     data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
    #     util_script="tests/test_utils.py",
    #     other_files=(
    #         (
    #             "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
    #             "connection_string.yml",
    #         ),
    #     ),
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
]

sample_data = [
    IntegrationTestFixture(
        name="sample_data_using_limit_postgres",
        user_flow_script="tests/integration/db/test_sql_data_sampling.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/",
        util_script="tests/test_utils.py",
        other_files=(
            (
                "tests/integration/fixtures/split_and_sample_data/postgres_connection_string.yml",
                "connection_string.yml",
            ),
        ),
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

migration_guide = [
    IntegrationTestFixture(
        name="migration_guide_postgresql_v3_api",
        user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_postgresql_v3_api.py",
        data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v3/great_expectations/",
        data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data/",
        util_script="tests/test_utils.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
    # There are some things using the old/bad "class_name: SqlAlchemyDataset"
    #   - docs/docusaurus/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials.md
    #   - tests/data_context/fixtures/contexts/incomplete_uncommitted/great_expectations/great_expectations.yml
    #   - tests/data_context/fixtures/version_zero/great_expectations.yml
    #   - tests/integration/docusaurus/miscellaneous/migration_guide_postgresql_v2_api.py
    #   - tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/great_expectations.yml
    #   - tests/test_fixtures/great_expectations_bad_datasource.yml
    #
    # great_expectations.data_context.util.instantiate_class_from_config(...) will be mad
    # on `class_ = load_class(class_name=class_name, module_name=module_name)` because
    # The module: `great_expectations.datasource` does not contain the class: `SqlAlchemyDatasource`.
    #
    # The great_expectations.data_context.types.base.DatasourceConfig.__init__ method has a check
    # that probably needs to change:
    #
    #       elif class_name == "SqlAlchemyDatasource":
    #           data_asset_type = {
    #               "class_name": "SqlAlchemyDataset",
    #               "module_name": "great_expectations.dataset",
    #           }
    # IntegrationTestFixture(
    #     name="migration_guide_postgresql_v2_api",
    #     user_flow_script="tests/integration/docusaurus/miscellaneous/migration_guide_postgresql_v2_api.py",
    #     data_context_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/postgresql/v2/great_expectations/",
    #     data_dir="tests/test_fixtures/configuration_for_testing_v2_v3_migration/data/",
    #     util_script="tests/test_utils.py",
    #     backend_dependencies=[BackendDependencies.POSTGRESQL],
    # ),
]

creating_custom_expectations = [
    IntegrationTestFixture(
        name="expect_multicolumn_values_to_be_multiples_of_three",
        user_flow_script="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_multicolumn_values_to_be_multiples_of_three.py",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

evaluation_parameters = [
    IntegrationTestFixture(
        name="dynamically_load_evaluation_parameters_from_a_database",
        user_flow_script="tests/integration/docusaurus/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.py",
        data_context_dir="tests/integration/fixtures/query_store/great_expectations",
        data_dir="tests/test_sets/taxi_yellow_tripdata_samples/first_3_files",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    )
]

fluent_datasources = [
    IntegrationTestFixture(
        name="how_to_connect_to_postgresql_data",
        user_flow_script="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py",
        data_context_dir="tests/integration/fixtures/no_datasources/great_expectations",
        backend_dependencies=[BackendDependencies.POSTGRESQL],
    ),
]

postgresql_integration_tests += connecting_to_your_data
postgresql_integration_tests += split_data
postgresql_integration_tests += sample_data
postgresql_integration_tests += migration_guide
postgresql_integration_tests += creating_custom_expectations
postgresql_integration_tests += evaluation_parameters
postgresql_integration_tests += fluent_datasources
