import great_expectations as gx


def setup():
    context = gx.get_context(mode="file")

    datasource_name = "my_datasource"
    my_connection_string = "${POSTGRESQL_CONNECTION_STRING}"

    data_source = context.data_sources.add_postgres(
        name=datasource_name, connection_string=my_connection_string
    )

    asset_name = "MY_TABLE_ASSET"
    database_table_name = "postgres_taxi_data"
    data_source.add_table_asset(table_name=database_table_name, name=asset_name)
