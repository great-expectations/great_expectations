---
title: How to configure an Expectation Store to use GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import CLIRemoval from '/docs/components/warnings/_cli_removal.md'

<CLIRemoval />

By default, newly <TechnicalTag tag="profiling" text="Profiled" /> <TechnicalTag tag="expectation" text="Expectations" /> are stored as <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> in JSON format in the ``expectations/`` subdirectory of your ``great_expectations/`` folder.  This guide will help you configure Great Expectations to store them in a Google Cloud Storage (GCS) bucket.

## Prerequisites

<Prerequisites>

- [Configured a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).
- [Configured an Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- Configured a Google Cloud Platform (GCP) [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that can access the appropriate GCP resources, which include Storage Objects.
- Identified the GCP project, GCS bucket, and prefix where Expectations will be stored.

</Prerequisites>

## Steps

### 1. Configure your GCP credentials

Check that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Expectations will be stored.

The Google Cloud Platform documentation describes how to verify your [authentication for the Google Cloud API](https://cloud.google.com/docs/authentication/getting-started), which includes:

1. Creating a Google Cloud Platform (GCP) service account,
2. Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
3. Verifying authentication by running a simple [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

### 2. Identify your Data Context Expectations Store

In your ``great_expectations.yml``, look for the following lines.  The configuration tells Great Expectations to look for Expectations in a <TechnicalTag tag="store" text="Store" /> called ``expectations_store``. The ``base_directory`` for ``expectations_store`` is set to ``expectations/`` by default.

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml"
```


### 3. Update your configuration file to include a new store for Expectations on GCS

In our case, the name is set to ``expectations_GCS_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to your GCP project, ``bucket`` will be set to the address of your GCS bucket, and ``prefix`` will be set to the folder on GCS where Expectation files will be located.

:::warning

If you are also storing [Validations in GCS](./how_to_configure_a_validation_result_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.

:::

```yaml name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml"
```


### 4. Copy existing Expectation JSON files to the GCS bucket (This step is optional)

One way to copy Expectations into GCS is by using the ``gsutil cp`` command, which is part of the Google Cloud SDK. The following example will copy one Expectation, ``my_expectation_suite`` from a local folder to the GCS bucket. Information on other ways to copy Expectation JSON files, like the Cloud Storage browser in the Google Cloud Console, can be found in the [Documentation for Google Cloud](https://cloud.google.com/storage/docs/uploading-objects).

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command"
```

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output"
```


### 5. Confirm that the new Expectations store has been added

Run the following:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_command"
```

Only the active Stores will be listed. Great Expectations will look for Expectations in GCS as long as we set the ``expectations_store_name`` variable to ``expectations_GCS_store``, and the config for ``expectations_store`` can be removed if you would like.

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_stores_output"
```


### 6. Confirm that Expectations can be accessed from GCS

To do this, run the following:

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command"
```

If you followed Step 4, the output should include the Expectation we copied to GCS: ``my_expectation_suite``. If you did not copy Expectations to the new Store, you will see a message saying no Expectations were found.

```bash name="tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_output"
```

## Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_configure_an_expectation_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py)
