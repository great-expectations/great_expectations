.. _how_to_guides__configuring_metadata_stores__how_to_configure_an_expectation_store_to_postgresql:

How to configure an Expectation store to postgresql
===================================================

By default, newly profiled Expectations are stored in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This guide will help you configure Great Expectations to store them in a PostgreSQL database table.

.. admonition:: Prerequisites: This how-to guide assumes that you have already:

    - Configured a :ref:`Data Context <tutorials__getting_started__initialize_a_data_context>`.
    - Configured an :ref:`Expectations Suite <tutorials__getting_started__create_your_first_expectations>`.
    - Configured :ref:`PostgreSQL <https://www.postgresql.org/>` database with credentials that can access the appropriate Tables.

1. **Configure your database credentials in the** `config_variables.yml` **file**

Database credentials are stored best stored in the  `config_variables.yml` file located at `great_expectations/uncommitted/config_variables.yml`, and is not part of source control.

    .. code-block:: yaml

        data_warehouse:
            drivername: postgres
            host: '<your_host_name>'
            port: '<your_port>'
            username: '<your_username>'
            password: '<your_password>'
            database: '<your_database_name>'


2. **Identify your Data Context Expectations Store**

    In your ``great_expectations.yml`` , look for the following lines.  The configuration tells Great Expectations to look for Expectations in a store called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.


    .. code-block:: yaml

        expectations_store_name: expectations_store

        stores:
            expectations_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: TupleFilesystemStoreBackend
                    base_directory: expectations/


3. **Update your configuration file to include a new store for Expectations on PostgreSQL**

    In our case, the name is set to ``expectations_postgres_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``DatabaseStoreBackend``.     Notice that credentials is set to `${data_warehouse}`, which references the corresponding key in the `config_variables.yml` file.

    .. code-block:: yaml

        expectations_store_name: expectations_postgres_store

        stores:
            expectations_postgres_store:
                class_name: ExpectationsStore
                store_backend:
                    class_name: DatabaseStoreBackend
                    credentials: ${data_warehouse}




4. **Confirm that the new Expectations store has been added by running** ``great_expectations store list``.

    Notice the output contains two Expectation stores: the original ``expectations_store`` on the local filesystem and the ``expectations_postgres_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in the S3 bucket as long as we set the ``expectations_store_name`` variable to ``expectations_postgres_store``.

    .. code-block:: bash

        great_expectations store list

        - name: expectations_store
        class_name: ExpectationsStore
        store_backend:
            class_name: TupleFilesystemStoreBackend
            base_directory: expectations/

        - name: expectations_postgres_store
        class_name: ExpectationsStore
        store_backend:
            class_name: DatabaseStoreBackend
            credentials:
                database: '<your_db_name>'
                drivername: postgresql
                host: '<your_host_name>'
                password: ******
                port: 5433
                username: '<your_username>'


6. **Confirm that Expectations can be accessed from Amazon S3 by running** ``great_expectations suite list``.

    If you followed Step 4, The output should include the 2 Expectations we copied to Amazon S3: ``exp1`` and ``exp2``.  If you did not copy Expectations to the new Store, you will see a message saying no expectations were found.

    .. code-block:: bash

        great_expectations suite list

        2 Expectation Suites found:
         - exp1

Additional Notes
----------------
    - GE will create a table called `ge_expectations_store` and create a table with teh fields `expectation_suite_name` and value being the JSON file that is equiavlent to what is stored.
    - GE will create a table called `ge_validations_store` and create a table with teh fields `expectation_suite_name`, and `run_name`, `run_time`, `batch_identifier` and `value`. These can be


    - For more information on how to configure a YAML file or environment variables, please take a look at https://docs.greatexpectations.io/en/latest/how_to_guides/configuring_data_contexts/how_to_use_a_yaml_file_or_environment_variables_to_populate_credentials.html


If it would be useful to you, please comment with a +1 and feel free to add any suggestions or questions below.




.. discourse::
    :topic_identifier: 183
