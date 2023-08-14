---
title: How to configure an Expectation Store to use GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, newl <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  Use the information provided here to configure a new storage location for Expectations in Google Cloud Storage (GCS).

To view all the code used in this topic, see [how_to_configure_an_expectation_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py).

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Expectations.

</Prerequisites>

## 1. Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Expectations will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

## 2. Identify your Data Context Expectations Store

The configuration for your Expectations <TechnicalTag tag="store" text="Store" /> is available in your <TechnicalTag tag="data_context" text="Data Context" />. Open ``great_expectations.yml`` and find the following entry: 

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml"
```

This configuration tells Great Expectations to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

## 3. Update your configuration file to include a new store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Expectations are stored.

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml"
```

:::warning
If you are also storing [Validations in GCS](./how_to_configure_a_validation_result_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/host_and_share_data_docs.md), make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

## 4. Copy existing Expectation JSON files to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Expectations into GCS. For example, the following command copies the Expectation ```my_expectation_suite`` from a local folder into a GCS bucket:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command"
```

The following confirmation message is returned:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output"
```

Additional methods for copying Expectations into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

## 5. Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to GCS, run the following Python command to confirm that Great Expectations can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied to GCS is returned. Expectation Suites that weren't copied to the new Store aren't listed.

## 6. Confirm that Expectations can be accessed from GCS

Run the following command to confirm your Expectations were copied to GCS:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command"
```

If your Expectations were not copied to Azure Blob Storage, a message indicating no Expectations were found is returned.



