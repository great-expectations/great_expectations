---
title: How to configure a Validation Result Store to PostgreSQL
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder. Validation Results can include sensitive or regulated data that should not be committed to a source control system.  Use the information provided here to configure Great Expectations to store Validation Results in a PostgreSQL database.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- [A PostgreSQL database](https://www.postgresql.org/) with appropriate credentials.

</Prerequisites>

## 1. Configure the ``config_variables.yml`` file with your database credentials

GX recommends storing database credentials in the ``config_variables.yml`` file, which is located in the ``uncommitted/`` folder by default, and not part of source control. 

1. To add database credentials, open ``config_variables.yml`` and add the following entry below the ``db_creds`` key: 

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

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />.  Open ``great_expectations.yml``and find the following entry:

```yaml
validations_store_name: validations_store

stores:
  validations_store:
      class_name: ValidationsStore
      store_backend:
          class_name: TupleFilesystemStoreBackend
          base_directory: uncommitted/validations/
```
This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

## 3. Update your configuration file to include a new Validation Results Store

Add the following entry to your ``great_expectations.yml``: 

```yaml
validations_store_name: validations_postgres_store

stores:
  validations_postgres_store:
      class_name: ValidationsStore
      store_backend:
          class_name: DatabaseStoreBackend
          credentials: ${db_creds}
```

In the previous example, `validations_store_name` is set to ``validations_postgres_store``, but it can be personalized.  Also, ``class_name`` is set to ``DatabaseStoreBackend``, and ``credentials`` is set to ``${db_creds}``, which references the corresponding key in the ``config_variables.yml`` file.  

## 4. Confirm the addition of the new Validation Results Store

In the previous example, a ``validations_store`` on the local filesystem and a ``validations_postgres_store`` are configured.  Great Expectations looks for Validation Results in PostgreSQL when the ``validations_store_name`` variable is set to ``validations_postgres_store``. Run the following command to remove ``validations_store`` and confirm the ``validations_postgres_store`` configuration:

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

## 5. Confirm the Validation Results Store is configured correctly

[Run a Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint#run-your-checkpoint-optional) to store results in the new Validation Results store in PostgreSQL, and then visualize the results by [re-building Data Docs](../../../terms/data_docs.md).

Great Expectations creates a new table in your database named ``ge_validations_store``, and populates the fields with information from the Validation Results.
