---
title: How to configure a Validation Result store on a filesystem
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in the ``uncommitted/validations/`` directory.  Validation Results can include sensitive or regulated data that should not be committed to a source control system. Use the information provided here to configure a new storage location for Validation Results in your filesystem. You'll learn how to use an <TechnicalTag tag="action" text="Action" /> to update <TechnicalTag tag="data_docs" text="Data Docs" /> sites with new Validation Results from <TechnicalTag tag="checkpoint" text="Checkpoint" /> runs.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectation Suite ](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](../../../guides/validation/checkpoints/how_to_create_a_new_checkpoint.md).
- A new storage location to store Validation Results. This can be a local path, or a path to a secure network filesystem.

</Prerequisites>

## 1. Create a new folder for Validation Results

Run the following command to create a new folder for your Validation Results and move your existing Validation Results to the new folder:

```bash
# in the great_expectations/ folder
mkdir shared_validations
mv uncommitted/validations/npi_validations/ uncommitted/shared_validations/
```
In this example, the name of the Validation Result is ``npi_validations`` and the path to the new storage location is ``shared_validations/``.

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

## 3. Update your configuration file to include a new Store for Validation results

In the following example, `validations_store_name` is set to ``shared_validations_filesystem_store``, but it can be personalized.  Also, ``base_directory`` is set to ``uncommitted/shared_validations/``, but you can set it to another path that is accessible by Great Expectations.

```yaml
validations_store_name: shared_validations_filesystem_store

stores:
   shared_validations_filesystem_store:
       class_name: ValidationsStore
       store_backend:
           class_name: TupleFilesystemStoreBackend
           base_directory: uncommitted/shared_validations/
```

## 4. Confirm that the Validation Results Store has been correctly configured

Run a [Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint#run-your-checkpoint-optional) to store results in the new Validation Results Store in your new location, and then visualize the results by re-building [Data Docs](../../../terms/data_docs.md).
