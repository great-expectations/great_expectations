---
title: How to configure an Expectation Store to use PostgreSQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


By default, new <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder.  Use the information provided here to configure Great Expectations to store Expectations in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A [PostgreSQL](https://www.postgresql.org/) database with appropriate credentials.

</Prerequisites>

## 1. Configure the `config_variables.yml` file with your database credentials

GX recommends storing database credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and not part of source control. 

To add database credentials, open ``config_variables.yml`` and add the following entry below the ``db_creds`` key: 

```yaml
    db_creds:
      drivername: postgresql
      host: '<your_host_name>'
      port: '<your_port>'
      username: '<your_username>'
      password: '<your_password>'
      database: '<your_database_name>'
```
To configure the ``config_variables.yml`` file, or additional environment variables, see [How to configure credentials](../configuring_data_contexts/how_to_configure_credentials.md).

## 2. Identify your Data Context Expectations Store

Open ``great_expectations.yml``and find the following entry:

```yaml
expectations_store_name: expectations_store

stores:
  expectations_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: expectations/
```

This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## 3. Update your configuration file to include a new Store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_postgres_store``, but it can be personalized. You also need to make some changes to the ``store_backend`` settings.  The ``class_name`` is ``DatabaseStoreBackend``, and ``credentials`` is ``${db_creds}`` to reference the corresponding key in the ``config_variables.yml`` file.

```yaml
expectations_store_name: expectations_postgres_store

stores:
  expectations_postgres_store:
      class_name: ExpectationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```
