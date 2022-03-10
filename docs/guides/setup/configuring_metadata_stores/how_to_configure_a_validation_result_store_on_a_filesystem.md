---
title: How to configure a Validation Result store on a filesystem
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in the ``uncommitted/validations/`` directory.  Since Validation Results may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.  This guide will help you configure a new storage location for Validation Results on your filesystem.

This guide will explain how to use an <TechnicalTag tag="action" text="Action" /> to update <TechnicalTag tag="data_docs" text="Data Docs" /> sites with new Validation Results from <TechnicalTag tag="checkpoint" text="Checkpoint" /> runs.

<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- [Configured an Expectation Suite ](../../../tutorials/getting_started/create_your_first_expectations.md).
- [Configured a Checkpoint](../../../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).
- Determined a new storage location where you would like to store Validation Results. This can either be a local path, or a path to a secure network filesystem.

</Prerequisites>

## Steps

### 1. Configure a new folder on your filesystem where Validation Results will be stored

Create a new folder where you would like to store your Validation Results, and move your existing Validation Results over to the new location. In our case, the name of the Validation Result is ``npi_validations`` and the path to our new storage location is ``shared_validations/``.

```bash
# in the great_expectations/ folder
mkdir shared_validations
mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/
```

### 2. Identify your Data Context Validation Results Store

As with other <TechnicalTag tag="store" text="Stores" />, you can find your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> by using your <TechnicalTag tag="data_context" text="Data Context" />.  In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Validation Results in a Store called ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

```yaml
validations_store_name: validations_store

stores:
   validations_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/validations/
```

### 3. Update your configuration file to include a new store for Validation results on your filesystem

In the example below, Validation Results Store is being set to ``shared_validations_filesystem_store``, but it can be any name you like.  Also, the ``base_directory`` is being set to ``uncommitted/shared_validations/``, but it can be set to any path accessible by Great Expectations.

```yaml
validations_store_name: shared_validations_filesystem_store

stores:
   shared_validations_filesystem_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/shared_validations/
```

### 4. Confirm that the location has been updated by running ``great_expectations store list``

Notice the output contains two Validation stores: the original ``validations_store`` and the ``shared_validations_filesystem_store`` we just configured.  This is ok, since Great Expectations will look for Validation Results in the ``uncommitted/shared_validations/`` folder as long as we set the ``validations_store_name`` variable to ``shared_validations_filesystem_store``. The config for ``validations_store`` can be removed if you would like.

```bash
great_expectations store list

- name: validations_store
  class_name: ValidationsStore
  store_backend:
    class_name: TupleFilesystemStoreBackend
    base_directory: uncommitted/validations/

- name: shared_validations_filesystem_store
  class_name: ValidationsStore
  store_backend:
    class_name: TupleFilesystemStoreBackend
    base_directory: uncommitted/shared_validations/
```


### 5. Confirm that the Validation Results Store has been correctly configured

Run a [Checkpoint](../../../tutorials/getting_started/validate_your_data.md) to store results in the new Validation Results Store on in your new location then visualize the results by re-building [Data Docs](../../../tutorials/getting_started/check_out_data_docs.md).
