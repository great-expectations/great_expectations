---
title: How to configure an Expectation Store to use PostgreSQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder.  This guide will help you configure Great Expectations to store them in a PostgreSQL database.

<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- [Configured an Expectations Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- Configured a [PostgreSQL](https://www.postgresql.org/) database with appropriate credentials.

</Prerequisites>


## Steps

### 1. Configure the `config_variables.yml` file with your database credentials

We recommend that database credentials be stored in the `config_variables.yml` file, which is located in the `uncommitted/` folder by default, and is not part of source control. The following lines add database credentials under the key `db_creds`. Additional options for configuring the `config_variables.yml` file or additional environment variables can be found [here](../configuring_data_contexts/how_to_configure_credentials.md).

```yaml
db_creds:
  drivername: postgres
  host: '<your_host_name>'
  port: '<your_port>'
  username: '<your_username>'
  password: '<your_password>'
  database: '<your_database_name>'
```


### 2. Identify your Data Context Expectations Store

In your ``great_expectations.yml`` , look for the following lines.  The configuration tells Great Expectations to look for Expectations in a <TechnicalTag tag="store" text="Store" /> called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```


### 3. Update your configuration file to include a new Store for Expectations on PostgreSQL

In our case, the name is set to ``expectations_postgres_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``DatabaseStoreBackend``, and ``credentials`` will be set to ``${db_creds}``, which references the corresponding key in the ``config_variables.yml`` file.

```yaml
expectations_store_name: expectations_postgres_store

stores:
  expectations_postgres_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```


### 4. Confirm that the new Expectations Store has been added by running ``great_expectations store list``

Notice the output contains two <TechnicalTag tag="expectation_store" text="Expectation Stores" />: the original ``expectations_store`` on the local filesystem and the ``expectations_postgres_store`` we just configured.  This is ok, since Great Expectations will look for Expectations in PostgreSQL as long as we set the ``expectations_store_name`` variable to ``expectations_postgres_store``, which we did in the previous step.  The config for ``expectations_store`` can be removed if you would like.

```bash
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
      port: '<your_port>'
      username: '<your_username>'
```


### 5. Create a new Expectation Suite by running ``great_expectations suite new``

This command prompts you to create and name a new Expectation Suite and to select a sample batch of data for the Suite to describe. Behind the scenes, Great Expectations will create a new table in your database called ``ge_expectations_store``, and populate the fields ``expectation_suite_name`` and ``value`` with information from the newly created Expectation Suite.

If you follow the prompts and create an Expectation Suite called ``exp1``, you can expect to see output similar to the following :

```bash
great_expectations suite new

#  ...

Name the new Expectation Suite: exp1

Great Expectations will choose a couple of columns and generate expectations about them
to demonstrate some examples of assertions you can make about your data.

Great Expectations will store these expectations in a new Expectation Suite 'exp1' here:

postgresql://'<your_db_name>'/exp1

#  ...
```


### 6. Confirm that Expectations can be accessed from PostgreSQL by running ``great_expectations suite list``

The output should include the Expectation Suite we created in the previous step: ``exp1``.

```bash
great_expectations suite list

1 Expectation Suites found:
- exp1
```
