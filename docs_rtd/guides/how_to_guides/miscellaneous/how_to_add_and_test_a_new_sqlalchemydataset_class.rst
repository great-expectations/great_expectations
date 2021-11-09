.. _how_to_guides__miscellaneous__how_to_add_and_test_a_new_sqlalchemydataset_class:

How to add support for a new SQLAlchemy dialect
===============================================

This guide will help you extend the Great Expectations execution layer to work on a new SQL dialect.

.. warning::

   This guide is a working checklist. We improve it each time we add support for a new SQL dialect, but it still has rough edges. And adding support for a new database will probably never be a cookie-cutter operation.

   If you're interested in extending Great Expectations in this way, please reach out on `Slack <greatexpectations.io/slack>`__ in the ``#contributors-contributing`` channel, and we'll work with you to get there.

Steps
-----

1. Install and document dependencies
####################################

    * **Install the python dependencies for your dialect.**  While you're at it, please add a line(s) to ``requirements-dev-sqlalchemy.txt``. Examples:
    
        .. code-block:: bash
        
            pyhive[presto]>=0.6.2
            PyAthena[SQLAlchemy]>=1.1
            pybigquery>=0.4.15

    * **Install any necessary drivers**. sqlalchemy requires special drivers for some databases. If yours is one of them, please carefully document the steps .
    * **If possible, add docker containers for test resources.** (You can find examples of similar containers in `assets/docker/`.) Otherwise, please document the steps to set up a suitable test environment.

The core team will not be able to merge your contribution until they're able to replicate your setup.


2. Make changes to sqlalchemy_dataset.py
########################################

    * **At the top the file, add a try-except block**

        .. code-block:: python

            try:
                import pyhive.sqlalchemy_presto as presto
            except ImportError:
                presto = None


    * **In SqlAlchemyDataset.__init__, add an elif clause**

        .. code-block:: python

            elif self.engine.dialect.name.lower() == "presto":
                self.dialect = import_module("pyhive.sqlalchemy_presto")

    * **In SqlAlchemyDataset.create_temporary_table, add an elif clause**

        .. code-block:: python

            elif (self.engine.dialect.name.lower() == "awsathena"
              or self.engine.dialect.name.lower() == "presto"):
                stmt = "CREATE OR REPLACE TABLE {table_name} AS {custom_sql}".format(
                    table_name=table_name, custom_sql=custom_sql)

    * **Review SqlAlchemyDataset methods starting with _get_dialect_**

        Methods like ``_get_dialect_type_module`` and ``_get_dialect_regex_expression`` allow you to customize their behavior to accommodate the idiosyncrasies of specific databases. In some cases, the methods specify defaults that will work for most SQL engines. In other cases, you'll need to implement custom logic. In either case, you need to verify that they behave properly for your dialect.


3. Make changes to conftest.py
##############################

    * **In pytest_addoption, add an option**

        .. code-block:: python

            parser.addoption(
                "--no-presto",
                action="store_true",
                help="If set, suppress tests against presto",
            )

    * **In build_test_backends_list, add a variable and if clause**

        .. code-block:: python

            no_presto = metafunc.config.getoption("--no-presto")
            if not no_presto:
                presto_conn_str = "presto://presto@localhost/memory/test_ci"
                try:
                    engine = sa.create_engine(presto_conn_str)
                    conn = engine.connect()
                except (ImportError, sa.exc.SQLAlchemyError):
                    raise ImportError(
                        "presto tests are requested, but unable to connect to the presto database at "
                        f"'{presto_conn_str}'"
                    )
                test_backends += ["presto"]

    * **In the sa fixture method, add your test_backend to the list of backends**

        .. code-block:: python

            if "postgresql" not in test_backends and "sqlite" not in test_backends and "presto" not in test_backends:


    * **For each of the test datasets, add a schema entry for the dialect.**

        Examples:

            * In ``numeric_high_card_dataset`` : ``"presto": {"norm_0_1": "DOUBLE"},``
            * In ``datetime_dataset`` : ``"presto": {"datetime": "TIMESTAMP"},``
            * In ``dataset_sample_data`` : ``"presto": {"infinities": "DOUBLE", "nulls": "DOUBLE", "naturals": "DOUBLE"},``


4. Make changes to tests/test_utils.py
######################################

    * **Add a try-except clause to import dialect-specific types and map them to type names that will be used in test schema definitions.**

        .. code-block:: python

            try:
                import sqlalchemy.types as sqltypes
                from pyhive.sqlalchemy_presto import presto as prestotypes
                from pyhive.sqlalchemy_presto import PrestoDialect as prestodialect

                PRESTO_TYPES = {
                    "VARCHAR": sqltypes.VARCHAR,
                    "TEXT": sqltypes.VARCHAR,
                    "CHAR": sqltypes.CHAR,
                    "DOUBLE": prestotypes.DOUBLE,
                    "INTEGER": sqltypes.INTEGER,
                    "SMALLINT": sqltypes.SMALLINT,
                    "BIGINT": sqltypes.BIGINT,
                    "DATETIME": sqltypes.TIMESTAMP,
                    "TIMESTAMP": sqltypes.TIMESTAMP,
                    "DATE": sqltypes.DATE,
                    "FLOAT": prestotypes.DOUBLE,
                    "BOOLEAN": prestotypes.BOOLEAN,
                }
            except ImportError:
                PRESTO_TYPES = {}

    * **In get_dataset, add an elif clause to build temporary datasets for testing.** Note: some SQL backends require a schema. If so, this is also the right place to create a test schema, usually called ``test_ci``.

        .. code-block:: python

            elif dataset_type == "presto":
                from sqlalchemy import create_engine

                engine = create_engine("presto://presto@localhost/memory/test_ci", echo=False)
                conn = engine.connect()

                sql_dtypes = {}
                if (
                    schemas
                    and "presto" in schemas
                    and isinstance(engine.dialect, prestodialect)
                ):
                    schema = schemas["presto"]
                    sql_dtypes = {col: PRESTO_TYPES[dtype] for (col, dtype) in schema.items()}
                    for col in schema:
                        type_ = schema[col].lower()
                        if type_ in ["integer", "smallint", "bigint"]:
                            df[col] = pd.to_numeric(df[col])
                        elif type_ in ["float", "double"]:
                            df[col] = pd.to_numeric(df[col])
                        elif type_ in ["timestamp", "datetime"]:
                            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                        elif type_ in ["varchar"]:
                            df[col] = df[col].astype(str)

                tablename = generate_test_table_name(dataset_id)

                conn.execute("CREATE SCHEMA IF NOT EXISTS test_ci")
                df.to_sql(name=tablename, con=conn, index=False)

                # Build a SqlAlchemyDataset using that database
                return SqlAlchemyDataset(
                    tablename, engine=conn, profiler=profiler, caching=caching
                )


    * **Add your dialect to candidate_test_is_on_temporary_notimplemented_list**.

        .. code-block:: python

            def candidate_test_is_on_temporary_notimplemented_list(context, expectation_type):
                if context in ["sqlite", "postgresql", "mysql", "presto"]:

5. Use tests to verify consistency with other databases
########################################################

Since Great Expectations already has rich tests for Expectations, we recommend test-driven development when adding support for a new SQL dialect.

You can run the main dev loop with:

.. code-block:: bash

    pytest --no-postgresql --no-spark tests/test_definitions/test_expectations.py

You may need to add specific spot checks to text fixture JSON objects, such as: ``tests/test_definitions/column_map_expectations/expect_column_values_to_be_of_type.json``

In some rare cases, you may need to suppress certain tests for your SQL backend. In that case, you can use the ``only_for`` or ``suppress_test_for`` flags in the test configs. However, we try very hard to avoid such cases, since they weaken the "works the same on all execution engines" principle of Great Expectations.

Once Expectation tests pass, make sure all the remaining tests pass:

.. code-block:: bash

    pytest --no-postgresql --no-spark

4. Wrap up
##############################


.. warning::

   This guide covers steps to add support for a new SQL dialect to SqlAlchemyDataset, and make it testable. To fully enable this SQL dialect in the Great Expectations ecosystem, you may also want to:
   
   - develop a Datasource for this dialect
   - develop a CLI integration for this dialect
