Use the information provided here to configure a new storage location for Expectations in GCS.

To view all the code used in this topic, see [how_to_configure_an_expectation_store_in_gcs.py](https://github.com/great-expectations/great_expectations/tree/develop/docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py).

### Prerequisites

- [A Data Context](/core/installation_and_setup/manage_data_contexts.md).
- [An Expectations Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md).
- A GCP [service account](https://cloud.google.com/iam/docs/service-accounts) with credentials that allow access to GCP resources such as Storage Objects.
- A GCP project, GCS bucket, and prefix to store Expectations.

### Configure your GCP credentials

Confirm that your environment is configured with the appropriate authentication credentials needed to connect to the GCS bucket where Expectations will be stored. This includes the following:

- A GCP service account.
- Setting the ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.
- Verifying authentication by running a [Google Cloud Storage client](https://cloud.google.com/storage/docs/reference/libraries) library script.

For more information about validating your GCP authentication credentials, see [Authenticate to Cloud services using client libraries](https://cloud.google.com/docs/authentication/getting-started).

### Identify your Data Context Expectations Store

The configuration for your Expectations Store is available in your Data Context. Open ``great_expectations.yml`` and find the following entry: 

```yaml title="great_expectations.yml" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py expected_existing_expectations_store_yaml"
```

This configuration tells GX Core to look for Expectations in the ``expectations_store`` Store. The default ``base_directory`` for ``expectations_store`` is ``expectations/``.

### Update your configuration file to include a new store for Expectations

In the following example, `expectations_store_name` is set to ``expectations_GCS_store``, but it can be personalized.  You also need to change the ``store_backend`` settings. The ``class_name`` is ``TupleGCSStoreBackend``, ``project`` is your GCP project, ``bucket`` is the address of your GCS bucket, and ``prefix`` is the folder on GCS where Expectations are stored.

```yaml title="YAML" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py configured_expectations_store_yaml"
```

:::warning
If you are storing Validations in GCS make sure that the ``prefix`` values are disjoint and one is not a substring of the other.
:::

### Copy existing Expectation JSON files to the GCS bucket (Optional)

Use the ``gsutil cp`` command to copy Expectations into GCS. For example, the following command copies the Expectation ```my_expectation_suite`` from a local folder into a GCS bucket:

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_command"
```

The following confirmation message is returned:

```bash title="Terminal output" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py copy_expectation_output"
```

Additional methods for copying Expectations into GCS are available. See [Upload objects from a filesystem](https://cloud.google.com/storage/docs/uploading-objects).

### Confirm that the new Expectation Suites have been added

If you copied your existing Expectation Suites to GCS, run the following Python command to confirm that GX Core can find them:

<!--A snippet is required for this code block.-->

```python
import great_expectations as gx

context = gx.get_context()
context.list_expectation_suite_names()
```

A list of Expectation Suites you copied to GCS is returned. Expectation Suites that weren't copied to the new Store aren't listed.

### Confirm that Expectations can be accessed from GCS

Run the following command to confirm your Expectations were copied to GCS:

```bash title="Terminal input" name="docs/docusaurus/docs/oss/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.py list_expectation_suites_command"
```

If your Expectations were not copied to Azure Blob Storage, a message indicating no Expectations were found is returned.