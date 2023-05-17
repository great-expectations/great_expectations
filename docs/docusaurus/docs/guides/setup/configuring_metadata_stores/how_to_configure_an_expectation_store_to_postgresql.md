---
title: How to configure an Expectation Store to use PostgreSQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder.  This guide will help you configure Great Expectations to store them in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A [PostgreSQL](https://www.postgresql.org/) database with appropriate credentials.

</Prerequisites>


## Steps

### 1. Configure the `config_variables.yml` file with your database credentials

We recommend that database credentials be stored in the `config_variables.yml` file, which is located in the `uncommitted/` folder by default, and is not part of source control. The following lines add database credentials under the key `db_creds`. Additional options for configuring the `config_variables.yml` file or additional environment variables can be found [here](../configuring_data_contexts/how_to_configure_credentials.md).

```yaml
db_creds:
  drivername: postgresql
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
