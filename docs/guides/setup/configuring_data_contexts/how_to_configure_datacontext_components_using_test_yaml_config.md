---
title: How to configure DataContext components using test_yaml_config
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for Datasources and Stores. For many deployments of Great Expectations, these components (plus Expectations) are the only ones you'll need.

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)

</Prerequisites>

``test_yaml_config`` is primarily intended for use within a notebook, where you can iterate through an edit-run-check loop in seconds.

Steps
-----

1. **Instantiate a DataContext**

    Create a new Jupyter Notebook and instantiate a DataContext by running the following lines:

    ```python
    import great_expectations as ge
    context = ge.get_context()
    ```

2. **Create or copy a yaml config**

    You can create your own, or copy an example. For this example, we'll demonstrate using a Datasource that connects to PostgreSQL.

    ```python
    config = """
    class_name: SimpleSqlalchemyDatasource
    credentials:
        drivername: postgresql
        username: postgres
        password: ""
        host: localhost
        port: 5432
        database: test_ci

    introspection:
        whole_table: {}
    """
    ```

3. **Run context.test_yaml_config.**

    ```python
    context.test_yaml_config(
        name="my_postgresql_datasource",
        yaml_config=config
    )
    ```

    When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.
    
    In the case of a Datasource, this means

    1. confirming that the connection works,
    2. gathering a list of available DataAssets (e.g. tables in SQL; files or folders in a filesystem), and
    3. verifying that it can successfully fetch at least one Batch from the source.

    The output will look something like this:

    ```bash
    Attempting to instantiate class from config...
        Instantiating as a Datasource, since class_name is SimpleSqlalchemyDatasource
        Successfully instantiated SimpleSqlalchemyDatasource

    Execution engine: SqlAlchemyExecutionEngine
    Data connectors:
        whole_table : InferredAssetSqlDataConnector

        Available data_asset_names (3 of 14440):
            expect_table_row_count_to_equal_other_table_data_1 (1 of 1): [{}]
            expect_table_row_count_to_equal_other_table_data_2 (1 of 1): [{}]
            expect_table_row_count_to_equal_other_table_data_3 (1 of 1): [{}]

        Unmatched data_references (0 of 0): []

        Choosing an example data reference...
            Reference chosen: {}

            Fetching batch data...

            Showing 5 rows
            c1 c2    c3   c4
            0   4  a  None  4.0
            1   5  b  None  3.0
            2   6  c  None  3.5
            3   7  d  None  1.2

    <great_expectations.datasource.simple_sqlalchemy_datasource.SimpleSqlalchemyDatasource at 0x12c1e4d50>
    ```

    If something about your configuration wasn't set up correctly, ``test_yaml_config`` will raise an error.  Whenever possible, test_yaml_config provides helpful warnings and error messages. It can't solve every problem, but it can solve many.

    ```bash
    Attempting to instantiate class from config...
        Instantiating as a Datasource, since class_name is SimpleSqlalchemyDatasource
    ---------------------------------------------------------------------------
    OperationalError                          Traceback (most recent call last)
    ~/anaconda2/envs/py3/lib/python3.7/site-packages/sqlalchemy/engine/base.py in _wrap_pool_connect(self, fn, connection)
    2338         try:
    -> 2339             return fn()
    2340         except dialect.dbapi.Error as e:

    ...

    OperationalError: (psycopg2.OperationalError) could not connect to server: Connection refused
        Is the server running on host "localhost" (::1) and accepting
        TCP/IP connections on port 5433?
    could not connect to server: Connection refused
        Is the server running on host "localhost" (127.0.0.1) and accepting
        TCP/IP connections on port 5433?

    (Background on this error at: http://sqlalche.me/e/13/e3q8)
    ```

4. **Iterate as necessary.**

    From here, iterate by editing your config and re-running ``test_yaml_config``, adding config blocks for additional introspection, data assets, sampling, etc.

5. **(Optional:) Test additional methods.**

    Note that when ``test_yaml_config`` runs successfully, it adds the specified Datasource to your DataContext. This means that you can also test other methods, such as ``context.get_validator``, right within your notebook:

    ```python
    validator = context.get_validator(
        datasource_name="my_datasource",
        data_connector_name="whole_table",
        data_asset_name="my_table",
        create_expectation_suite_with_name="my_expectation_suite",
    )
    validator.expect_column_values_to_be_in_set("c1", [4,5,6])
    ```

6. **Save the config.**

    Once you are satisfied with your config, you can make it a permanent part of your Great Expectations setup by copying it into the appropriate section of your ``great_expectations/great_expectations.yml`` file.

    ```yaml
    datasources:
        my_datasource:
            class_name: SimpleSqlalchemyDatasource
            credentials:
                drivername: postgresql
                username: postgres
                password: ""
                host: localhost
                port: 5432
                database: test_ci

            introspection:
                whole_table: {}
    ```

7. **Check your modified config.**

    In a fresh notebook, test your edited config file by re-instantiating your DataContext:

    ```python
    context = ge.get_context()

    validator = context.get_validator(
        datasource_name="my_datasource",
        data_connector_name="whole_table",
        data_asset_name="my_table",
        create_expectation_suite_with_name="my_expectation_suite",
    )
    validator.expect_column_values_to_be_in_set("c1", [4,5,6])
    ```

