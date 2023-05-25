---
title: How to configure a Validation Result store in GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, <TechnicalTag tag="validation_result" text="Validation Results" /> are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder. Validation Results can include sensitive or regulated data that should not be committed to a source control system.  Use the information provided here to configure a new storage location for Validation Results in Google Cloud Storage (GCS).

To view all the code used in this topic, see [how_to_configure_a_validation_result_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py).

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Validation Results.

</Prerequisites>

## 1. Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validation Results will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

## 2. Identify your Data Context Validation Results Store

The configuration for your <TechnicalTag tag="validation_result_store" text="Validation Results Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml``and find the following entry: 

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py expected_existing_validations_store_yaml"
```
This configuration tells Great Expectations to look for Validation Results in the ``validations_store`` Store. The default ``base_directory`` for ``validations_store`` is ``uncommitted/validations/``.

## 3. Update your configuration file to include a new Store for Validation Results

In the following example, `validations_store_name` is set to ``validations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Validation Result files are stored.

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py configured_validations_store_yaml"
```

:::warning
If you are also storing [Expectations in GCS](../configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

## 4. Copy existing Validation Results to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Validation Results into GCS. For example, the following command copies the Validation results ``validation_1`` and ``validation_2``into a GCS bucket: 

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_command"
```
The following confirmation message is returned:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py copy_validation_output"
```
Additional methods for copying Validation Results into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

## 5. Reference the new configuration

To make Great Expectations look for Validation Results on the GCS store, set the ``validations_store_name`` variable to the name of your GCS Validations Store. In the previous example this was `validations_GCS_store`.

## 6. Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](/docs/guides/validation/how_to_validate_data_by_running_a_checkpoint) to store results in the new Validation Results Store on GCS, and then visualize the results by [re-building Data Docs](/docs/terms/data_docs).
