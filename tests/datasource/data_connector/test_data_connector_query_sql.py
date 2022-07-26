@pytest.fixture()
def create_db_and_instantiate_simple_sql_datasource():
    data_path: str = (
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata_2020.db"
    )

    datasource_config = {
        "name": "taxi_multi_batch_sql_datasource",
        "module_name": "great_expectations.datasource",
        "class_name": "SimpleSqlalchemyDatasource",
        "connection_string": "sqlite:///" + data_path,
        "tables": {
            "yellow_tripdata_sample_2020_01": {
                "partitioners": {
                    "whole_table": {},
                    "by_vendor_id": {
                        "splitter_method": "_split_on_divided_integer",
                        "splitter_kwargs": {"column_name": "vendor_id", "divisor": 1},
                    },
                },
            },
            "yellow_tripdata_sample_2020_02": {
                "partitioners": {
                    "whole_table": {},
                },
            },
        },
    }
    simple_sql_datasource: SimpleSqlalchemyDatasource = instantiate_class_from_config(
        config=datasource_config,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "datasource_name": "taxi_multi_batch_sql_datasource",
        },
        config_defaults={"module_name": "great_expectations.datasource"},
    )
    return simple_sql_datasource
