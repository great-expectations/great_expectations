---
title: How to configure a Validation Result Store to PostgreSQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


By default, Validation Results are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder. <TechnicalTag tag="validation_result" text="Validation Results" /> can include examples of data (which could be sensitive or regulated) that should not be committed to a source control system.  Use the information provided here to configure Great Expectations to store Validation Results in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- [A PostgreSQL database](https://www.postgresql.org/) with appropriate credentials.

</Prerequisites>

## 1. Configure the ``config_variables.yml`` file with your database credentials

GX recommends storing database credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and not part of source control. 

1. Add the following entry to add database credentials under the key ``db_creds``: 

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

2. Optional. To use a specific schema as the backend, specify `schema` as an additional keyword argument. For example:

    ```yaml
    db_creds:
      drivername: postgresql
      host: '<your_host_name>'
      port: '<your_port>'
      username: '<your_username>'
      password: '<your_password>'
      database: '<your_database_name>'
      schema: '<your_schema_name>'
    ```

## 2. Identify your Data Context Validation Results Store

As with all <TechnicalTag tag="store" text="Stores" />, you can use your <TechnicalTag tag="data_context" text="Data Context" /> to find your <TechnicalTag tag="validation_result_store" text="Validation Results Store" />.  In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Validation Results in a Store named ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```

## 3. Update your configuration file to include a new Validation Results Store on PostgreSQL

In this example, the name is set to ``validations_postgres_store``, but it can be any name.  You also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``DatabaseStoreBackend``, and ``credentials`` will be set to ``${db_creds}``, which references the corresponding key in the ``config_variables.yml`` file. For example:

```yaml
validations_store_name: validations_postgres_store

stores:
  validations_postgres_store:
      class_name: ValidationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```


## 4. Confirm the addition of the new Validation Results Store

The output contains two Validation Result Stores: the original ``validations_store`` on the local filesystem and the ``validations_postgres_store``.  Great Expectations looks for Validation Results in PostgreSQL as long as you set the ``validations_store_name`` variable to ``validations_postgres_store``. The config for ``validations_store`` can be removed. For example:

```bash
great_expectations store list

- name: validations_store
class_name: ValidationsStore
store_backend:
  class_name: TupleFilesystemStoreBackend
  base_directory: uncommitted/validations/

- name: validations_postgres_store
class_name: ValidationsStore
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

## 5. Confirm the Validation Results Store is correctly configured

[Run a Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results store in PostgreSQL then visualize the results by [re-building Data Docs](../../../terms/data_docs.md).

Behind the scenes, Great Expectations creates a new table in your database named ``ge_validations_store``, and populates the fields with information from the Validation Results.
