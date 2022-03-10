---
title: How to configure a Validation Result store in GCS
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

By default, Validation Results are stored in JSON format in the ``uncommitted/validations/`` subdirectory of your ``great_expectations/`` folder.  Since Validation Results may include examples of data (which could be sensitive or regulated) they should not be committed to a source control system.  This guide will help you configure a new storage location for Validation Results in a Google Cloud Storage (GCS) bucket.

<Prerequisites>

- [Configured a Data Context](../../../tutorials/getting_started/initialize_a_data_context.md).
- [Configured an Expectations Suite](../../../tutorials/getting_started/create_your_first_expectations.md).
- [Configured a Checkpoint](../../../tutorials/getting_started/validate_your_data.md).
- Configured a Google Cloud Platform (GCP) [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that can access the appropriate GCP resources, which include Storage Objects.
- Identified the GCP project, GCS bucket, and prefix where Validation Results will be stored.

</Prerequisites>

## Steps

### 1. Configure your GCP credentials

Check that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Validation Results will be stored.

The Google Cloud Platform documentation describes how to verify your [authentication for the Google Cloud API](https://cloud.google.com/docs/authentication/getting-started), which includes:

1. Creating a Google Cloud Platform (GCP) service account,
2. Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable,
3. Verifying authentication by running a simple [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

### 2. Identify your Data Context Validation Results Store

As with other Stores, you can find your Validation Results Store through your <TechnicalTag tag="data_context" text="Data Context" />. In your ``great_expectations.yml``, look for the following lines. The configuration tells Great Expectations to look for Validation Results in a Store called ``validations_store``. The ``base_directory`` for ``validations_store`` is set to ``uncommitted/validations/`` by default.

```yaml file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L79-L86
```


### 3. Update your configuration file to include a new Store for Validation Results on GCS

In our case, the name is set to ``validations_GCS_store``, but it can be any name you like.  We also need to make some changes to the ``store_backend`` settings.  The ``class_name`` will be set to ``TupleGCSStoreBackend``, ``project`` will be set to your GCP project, ``bucket`` will be set to the address of your GCS bucket, and ``prefix`` will be set to the folder on GCS where Validation files will be located.

```yaml file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L94-L103
```

:::warning
If you are also storing [Expectations in GCS](../configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md) or [DataDocs in GCS](../configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), please ensure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::


### 4. Copy existing Validation results to the GCS bucket (This step is optional)

One way to copy Validation Results into GCS is by using the ``gsutil cp`` command, which is part of the Google Cloud SDK. In the example below, two Validation results, ``validation_1`` and ``validation_2`` are copied to the GCS bucket. Information on other ways to copy Validation results, like the Cloud Storage browser in the Google Cloud Console, can be found in the [Documentation for Google Cloud](https://cloud.google.com/storage/docs/uploading-objects).

```bash file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L148-L149
```

```bash file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L204
```



### 5. Confirm that the new Validation Results Store has been added by running

```bash file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L209
```

Only the active Stores will be listed. Great Expectations will look for Validation Results in GCS as long as we set the ``validations_store_name`` variable to ``validations_GCS_store``, and the config for ``validations_store`` can be removed if you would like.

```bash file=../../../../tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py#L220-L226
```

### 6. Confirm that the Validation Results Store has been correctly configured

[Run a Checkpoint](../../../tutorials/getting_started/validate_your_data.md) to store results in the new Validation Results Store on GCS then visualize the results by [re-building Data Docs](../../../tutorials/getting_started/validate_your_data.md).


## Additional Notes
To view the full script used in this page, see it on GitHub:
- [how_to_configure_a_validation_result_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/docusaurus/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.py)
