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


@pytest.mark.sqlite
def test_me_column_list():
    # TODO: this would need better messaging
    context = gx.get_context(cloud_mode=False)
    database = "pythonsqlite.db"
    my_table_name = "projects"
    connection_string = "sqlite:///pythonsqlite.db"

    # https://github.com/knex/knex/issues/3434
    # case insensitive

    database = "pythonsqlite.db"
    my_table_name = "all_lower"
    column_a_name = "COLUMN_A"
    column_b_name = "COLUMN_B"
    column_c_name = "COLUMN_C"
    sql_create_projects_table = f""" CREATE TABLE IF NOT EXISTS {my_table_name} (
                                    id integer PRIMARY KEY,
                                    {column_a_name} text NOT NULL,
                                    {column_b_name} text NOT NULL,
                                    {column_c_name} text NOT NULL
                                ); """
    sql_insert_data = f"""INSERT INTO {my_table_name} (id, {column_a_name}, {column_b_name}, {column_c_name}) VALUES (1, 'a', 'b', 'c');"""

    # create a database connection
    conn = create_connection(database)

    # create table
    if conn is not None:
        # create projects table
        run_query(conn, sql_create_projects_table)
        # fill table
        run_query(conn, sql_insert_data)
        run_query(
            conn,
            f"SELECT {column_a_name} FROM sqlite_schema WHERE type='multi_project' ORDER BY {column_a_name};",
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
    validator.expect_column_values_to_not_be_null(column=column_a_name)
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        column_A=column_a_name, column_B=column_b_name
    )
    validator.expect_multicolumn_sum_to_equal(
        column_list=[column_a_name, column_b_name], sum_total=4
    )
