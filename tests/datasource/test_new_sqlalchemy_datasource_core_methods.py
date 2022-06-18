from great_expectations.datasource.new_sqlalchemy_datasource import (
    NewSqlAlchemyDatasource,
)


def test_NewSqlAlchemyDatasource_instantiation():
    my_datasource = NewSqlAlchemyDatasource(
        name="my_datasource",
        connection_string="sqlite:///tests/chinook.db",
    )

    print(my_datasource.list_tables())
    my_datasource.get_table("albums")
