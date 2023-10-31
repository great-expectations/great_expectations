import sqlite3
from sqlite3 import Error

import pytest

import great_expectations as gx


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def run_query(conn, query):
    try:
        c = conn.cursor()
        c.execute(query)
    except Error as e:
        print(e)


@pytest.fixture
def create_db():
    database = "pythonsqlite.db"
    my_table_name = "projects"
    column_name = "name"
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {my_table_name} (
                                    id integer PRIMARY KEY,
                                    {column_name} text NOT NULL
                                ); """

    sql_insert_data = f"""INSERT INTO {my_table_name} (id, {column_name}) VALUES (1, 'Cool App with SQLite & Python');"""

    # create a database connection
    conn = create_connection(database)

    # create table
    if conn is not None:
        # create projects table
        run_query(conn, sql_create_projects_table)

        # fill table
        run_query(conn, sql_insert_data)
    else:
        print("Error! cannot create the database connection.")


def me_both_lower():
    context = gx.get_context()
    database = "pythonsqlite.db"
    my_table_name = "projects"
    connection_string = "sqlite:///pythonsqlite.db"

    database = "pythonsqlite.db"
    my_table_name = "projects"
    column_name = "name"
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {my_table_name} (
                                    id integer PRIMARY KEY,
                                    {column_name} text NOT NULL
                                ); """

    sql_insert_data = f"""INSERT INTO {my_table_name} (id, {column_name}) VALUES (1, 'Cool App with SQLite & Python');"""

    # create a database connection
    conn = create_connection(database)

    # create table
    if conn is not None:
        # create projects table
        run_query(conn, sql_create_projects_table)

        # fill table
        run_query(conn, sql_insert_data)
    else:
        print("Error! cannot create the database connection.")

    datasource = context.sources.add_or_update_sqlite(
        name=my_table_name, connection_string=connection_string
    )

    datasource.add_table_asset(name=my_table_name, table_name=my_table_name)

    batch_request = datasource.get_asset(my_table_name).build_batch_request()

    expectation_suite_name = f"{my_table_name}_ge_suite"

    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )

    validator.expect_column_values_to_not_be_null(column=column_name)
    validator.expect_column_values_to_be_null(column=column_name)
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000000)
    validator.expect_column_distinct_values_to_be_in_set(
        column=column_name, value_set=["Cool App with SQLite & Python"]
    )
    print("Happy path complete!!!")

    expectations = {
        "expect_column_values_to_not_be_null": {"column": column_name},
        "expect_column_values_to_be_null": {"column": column_name},
        "expect_column_max_to_be_between": {
            "column": column_name,
            "min_value": 1,
            "max_value": 99,
        },
        "expect_table_row_count_to_be_between": {"min_value": 1, "max_value": 1000000},
        "expect_column_distinct_values_to_be_in_set": {
            "column": column_name,
            "value_set": ["Cool App with SQLite & Python"],
        },
    }
    for expectation, kwargs in expectations.items():
        getattr(validator, expectation)(**kwargs)
        print(f"Expectation type {expectation} succeeded with kwargs {kwargs}")
    # for expectation, kwargs in expectations.items():
    #    try:


#         exp = getattr(validator, expectation)(**kwargs)
#        print(f"Expectation type {expectation} succeeded with kwargs {kwargs}")
#    except Exception as e:
#        print(
#        f"Exception of type: {type(e)} and Message: {e}. Expectation type {expectation} failed with kwargs {kwargs}"
#        )


def test_me_only_table_name_upper():
    # TODO: this would need better messaging
    context = gx.get_context()
    database = "pythonsqlite.db"
    my_table_name = "projects"
    connection_string = "sqlite:///pythonsqlite.db"

    # https://github.com/knex/knex/issues/3434
    # case insensitive

    database = "pythonsqlite.db"
    my_table_name = "projects"
    column_name = "NAME"
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {my_table_name} (
                                    id integer PRIMARY KEY,
                                    {column_name} text NOT NULL
                                ); """

    sql_insert_data = f"""INSERT INTO {my_table_name} (id, {column_name}) VALUES (1, 'Cool App with SQLite & Python');"""

    # create a database connection
    conn = create_connection(database)

    # create table
    if conn is not None:
        # create projects table
        run_query(conn, sql_create_projects_table)

        # fill table
        run_query(conn, sql_insert_data)

        run_query(
            conn, "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;"
        )
    else:
        print("Error! cannot create the database connection.")

    datasource = context.sources.add_or_update_sqlite(
        name=my_table_name, connection_string=connection_string
    )

    datasource.add_table_asset(name=my_table_name, table_name=my_table_name)

    batch_request = datasource.get_asset(my_table_name).build_batch_request()

    expectation_suite_name = f"{my_table_name}_ge_suite"

    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    validator.expect_column_values_to_not_be_null(column="NAME")
    # validator.expect_column_values_to_be_null(column=column_name)
    # validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000000)
    # validator.expect_column_distinct_values_to_be_in_set(
    # column=column_name, value_set=["Cool App with SQLite & Python"]
    # )
    print("Happy path complete!!!")

    expectations = {
        "expect_column_values_to_not_be_null": {"column": column_name},
        "expect_column_values_to_be_null": {"column": column_name},
        "expect_column_max_to_be_between": {
            "column": column_name,
            "min_value": 1,
            "max_value": 99,
        },
        "expect_table_row_count_to_be_between": {"min_value": 1, "max_value": 1000000},
        "expect_column_distinct_values_to_be_in_set": {
            "column": column_name,
            "value_set": ["Cool App with SQLite & Python"],
        },
    }
    for expectation, kwargs in expectations.items():
        getattr(validator, expectation)(**kwargs)
        print(f"Expectation type {expectation} succeeded with kwargs {kwargs}")
    # for expectation, kwargs in expectations.items():
    #    try:


#         exp = getattr(validator, expectation)(**kwargs)
#        print(f"Expectation type {expectation} succeeded with kwargs {kwargs}")
#    except Exception as e:
#        print(
#        f"Exception of type: {type(e)} and Message: {e}. Expectation type {expectation} failed with kwargs {kwargs}"
#        )
