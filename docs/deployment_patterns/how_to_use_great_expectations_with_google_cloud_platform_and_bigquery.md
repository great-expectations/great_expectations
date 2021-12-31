---
title: How to Use Great Expectations with Google Cloud Platform and BigQuery
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'

Great Expectations works well with many types of [Google Cloud Platform](https://cloud.google.com/gcp) (GCP) workflows. This guide will help you integrate Great Expectations with GCP using our recommended workflow.

<Prerequisites>

- Have a working local installation of Great Expectations that is at least version 0.13.49.
- Have read through the documentation and are familiar with the Google Cloud Platform features that are used in this guide.
- Have completed the set-up of a GCP project with a running Google Cloud Storage container that is accessible from your region, and read/write access to a BigQuery database if this is where you are loading your data.
- Access to a GCP [Service Account](https://cloud.google.com/iam/docs/service-accounts) with permission to access and read objects in Google Cloud Storage, and read/write access to a BigQuery database if this is where you are loading your data.

</Prerequisites>


We recommend that you use Great Expectations in GCP by using the following services:
  - [Google Cloud Composer](https://cloud.google.com/composer) (GCC) for managing workflow orchestration including validating your data. GCC is built on [Apache Airflow](https://airflow.apache.org/).
  - [BigQuery](https://cloud.google.com/bigquery) or files in [Google Cloud Storage](https://cloud.google.com/storage) (GCS) as your [Datasource](../reference/datasources.md)
  - [GCS](https://cloud.google.com/storage) for storing metadata ([Expectation Suites](../reference/expectations/expectations.md), [Validation Results](../reference/validation.md), [Data Docs](../reference/data_docs.md))
  - [Google App Engine](https://cloud.google.com/appengine) (GAE) for hosting and controlling access to [Data Docs](../reference/data_docs.md).

We also recommend that you deploy Great Expectations to GCP in two steps:
1. Developing a local configuration for GE that uses GCP services to connect to your data, store Great Expectations metadata, and run a Checkpoint.
2. Migrating the local configuration to Google Cloud Composer so that the workflow can be orchestrated automatically on GCP.

In line with our recommendation this document is presented in 2 parts:

In [Part 1](#part-1-local-configuration-of-great-expectations-that-connects-to-google-cloud-platform) we will be building a local configuration of Great Expectations to connect to the correct Metadata Stores on GCP (for [Expectation Suites](../reference/expectations/expectations.md), [Validation Results](../reference/validation.md), and [Data Docs](../reference/data_docs.md)), as well as data in BigQuery or Google Cloud Storage.
We will be setting up and testing our configuration by checking that we can set up a run a Checkpoint locally. Then in [Part 2](#part-2-migrating-our-local-configuration-to-google-cloud-composer), we will describe how the local Great Expectations configuration can be migrated to Google Cloud Composer and the same checkpoint be run as part of our automated workflow.

The following diagram shows the recommended components for a Great Expectations deployment in Google Cloud Platform:  

![Screenshot of Data Docs](../deployment_patterns/images/ge_and_gcp_diagram.png)

Relevant documentation for the components can be found here:

- [How to configure an Expectation store to use GCS](https://docs.greatexpectations.io/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs)
- [How to configure a Validation Result store in GCS](https://docs.greatexpectations.io/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs)
- [How to host and share Data Docs on GCS](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs)
- Optionally, you can also use a [Secret Manager for GCP Credentials](https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials)

## Part 1: Local Configuration of Great Expectations that connects to Google Cloud Platform

### 1. If necessary, upgrade your Great Expectations version

The current guide was developed and tested using Great Expectations 0.13.49. Please ensure that your current version is equal or newer than this.

A local installation of Great Expectations can be upgraded using a simple `pip install` command with the `--upgrade` flag.

```bash
pip install great-expectations --upgrade
```

### 2. Connect to Metadata Stores on GCP

The following sections will describe how you can take a basic local configuration of Great Expectations and connect it to Metadata stores on GCP.

The full configuration used in this guide can be found in the [`great-expectations` repository](https://github.com/great-expectations/great_expectations/tests/integration/fixtures/gcp_deployment/) and is also linked at the bottom of this document

#### Add Expectations Store
By default, newly profiled Expectations are stored in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder. A new Expectations Store can be configured by adding the following lines into your `great_expectations.yml` file, replacing the `project`, `bucket` and `prefix` with your information. 

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L38-L44
```

Great Expectations can use this new Expectations Store, `expectations_GCS_store`, by setting the `expectations_store_name` value in the `great_expectations.yml` file.

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L72
```

For additional details and example configurations, please refer to [How to configure an Expectation store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md).

#### Add Validations Store
By default, Validations are stored in JSON format in the `uncommitted/validations/` subdirectory of your `great_expectations/` folder. A new Validations Store can be configured by adding the following lines into your `great_expectations.yml` file, replacing the `project`, `bucket` and `prefix` with your information. 

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L52-L58
```

Great Expectations can use this new Validations Store, `validations_GCS_store`, by setting the `validations_store_name` value in the `great_expectations.yml` file.

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L73
```

For additional details and example configurations, please refer to  [How to configure an Validation Result store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.md).

#### Add Data Docs Store
To host and share Datadocs on GCS, we recommend following the guide on how to [How to host and share Datadocs on GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), which will explain how to host and share Data Docs on Google Cloud Storage using IP-based access.

Afterwards, your `great-expectations.yml` will contain the following configuration under `data_docs_sites`.

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L91-L99
```


And you should be able to view the deployed DataDocs site by running the following CLI command:

```bash
gcloud app browse
```

If successful, the `gcloud` CLI will provide the URL to your app and launch it in a new browser window, and you should be able to view the index page of your Data Docs site.

### 3. Connect to your Data

The remaining sections contains a simplified description of how to connect to your data in GCS or BigQuery and eventually build a [Checkpoint](../reference/checkpoints_and_actions.md) that will be migrated to Google Cloud Composer. The code can be run in a Jupyter Notebook that is in your `great_expectations/` folder.
More details can be found in the corresponding How to Guides, which have been linked.

<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

To connect to your data in GCS, first instantiate your project's DataContext by importing the necessary packages and modules 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L3-L6
```

and loading your DataContext into memory using the `get_context()` method.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L8
```

Next load the following Datasource configuration that will connect to data in GCS,

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L205-L223
```

and save the configuration into your DataContext by using the `add_datasource()` function.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L235
```

For more details on how to configure the Datasource, and additional information on authentication, please refer to [How to connect to data on GCS using Pandas
](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/cloud/gcs/pandas)

</TabItem>
<TabItem value="bigquery">

To connect to your data in BigQuery, first instantiate your project's DataContext by importing the necessary packages and modules 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L1-L4
```

and loading your DataContext into memory using the `get_context()` method.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L6
```

Next load the following Datasource configuration that will connect to data in GCS,

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L233-L247
```

and save the configuration into your DataContext by using the `add_datasource()` function.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L257
```

For more details on how to configure the BigQuery Datasource, please refer to [How to connect to data on GCS using Pandas
](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/bigquery)

</TabItem>
</Tabs>

### 4. Get Batch and Create ExpectationSuite

<Tabs
  groupId="get-batch-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

For our example, we will be creating our ExpectationSuite with [instant feedback from a sample Batch of data](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data), which we will describe in our `BatchRequest`. For additional examples on how to create ExpectationSuites, either through [domain knowledge](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly) or using the [User Configurable Profiler](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_a_profiler), please refer to the documentation under `How to Guides` -> `Creating and editing Expectations for your data` -> `Core skills`. 

First load a batch of data by specifying a `data_asset_name` in a `BatchRequest`.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L238-L242
```

Next create an ExpectationSuite (`yellow_tripdata_gcs_suite` in our example), and use it to get a `Validator`. 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L244-L250
```

Next use the `Validator` to run expectations on the batch and automatically add them to your ExpectationSuite. For our example, we will add `expect_column_values_to_not_be_null` and `expect_column_values_to_be_between`. 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L252-L258
```

Lastly, save the ExpectationSuite, which now contains our two Expectations.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L258
```

For more details on how to configure the RuntimeBatchRequest, as well as an example of how you can load data by specifying a GCS path to a single CSV, please refer to [How to connect to data on GCS using Pandas
](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/bigquery)

</TabItem>
<TabItem value="bigquery">

For our example, we will be creating our ExpectationSuite with [instant feedback from a sample Batch of data](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data), which we will describe in our `BatchRequest`. For additional examples on how to create ExpectationSuites, either through [domain knowledge](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly) or using the [User Configurable Profiler](https://docs.greatexpectations.io/docs/guides/expectations/how_to_create_and_edit_expectations_with_a_profiler), please refer to the documentation under `How to Guides` -> `Creating and editing Expectations for your data` -> `Core skills`. 

First load a batch of data by specifying a SQL query in a `RuntimeBatchRequest`.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L260-L269
```

Next create an ExpectationSuite (`yellow_tripdata_bigquery_suite` in our example), and use it to get a `Validator`. 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L271-L277
```

Next use the `Validator` to run expectations on the batch and automatically add them to your ExpectationSuite. For our example, we will add `expect_column_values_to_not_be_null` and `expect_column_values_to_be_between`. 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L279-L283
```

Lastly, save the ExpectationSuite, which now contains our two Expectations.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L285
```

For more details on how to configure the BatchRequest, as well as an example of how you can load data by specifying a table name, please refer to [How to connect to data on GCS using Pandas
](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/database/bigquery)

</TabItem>
</Tabs>

### 5. Build and Run Checkpoint 

For our example, we will create a basic Checkpoint configuration using the `SimpleCheckpoint` class. For [additional examples](https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint), information on [how to add validations, data, or suites to existing checkpoints](https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint), and [more complex configurations](https://docs.greatexpectations.io/docs/guides/validation/checkpoints/how_to_configure_a_new_checkpoint_using_test_yaml_config) please refer to the documentation under `How to Guides` -> `Validating your data` -> `Checkpoints`.

<Tabs
  groupId="checkpoint-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

Add the following checkpoint `gcs_taxi_check` to the DataContext.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L260-L274
```

Run the checkpoint directly in-code 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L275-L277
```

or through the CLI 

```bash
great_expectations --v3-api checkpoint run gcs_taxi_check
```

If you have successfully configured the local prototype, you will have the following:

1. You should have an ExpectationSuite in the GCS bucket configured in `expectations_GCS_store` (ExpectationSuite is named `yellow_tripdata_suite` in our example).
2. You should have a new Validation Result in the GCS bucket configured in `validation_GCS_store`.
3. You should have Data Docs in the GCS bucket configured in `gs_site` and accessible by running `gcloud app browse`.

Now you are ready to migrate the local configuration to Google Cloud Composer.

</TabItem>
<TabItem value="bigquery">

Add the following checkpoint `bigquery_taxi_check` to the DataContext 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L287-L307
```

Run the checkpoint directly in-code 

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery_yaml_configs.py#L308-L310
```

or through the CLI 

```bash
great_expectations --v3-api checkpoint run gcs_taxi_check
```

If you have successfully configured the local prototype, you will have the following:

1. You should have an ExpectationSuite in the GCS bucket configured in `expectations_GCS_store` (ExpectationSuite is named `yellow_tripdata_suite` in our example).
2. You should have a new Validation Result in the GCS bucket configured in `validation_GCS_store`.
3. You should have Data Docs in the GCS bucket configured in `gs_site` and accessible by running `gcloud app browse`.

Now you are ready to migrate the local configuration to Google Cloud Composer.


</TabItem>
</Tabs>


## Part 2: Migrating our Local Configuration to Google Cloud Composer

In this section we are going to take the local GE configuration from Part 1 and migrate it to a Google Cloud Composer environment so that we can automate the workflow.

There are a number of ways that Great Expectations can be run in Airflow.

1. [Running a Checkpoint in Airflow using a `bash operator`](how_to_run_a_checkpoint_in_airflow.md#option-1-running-a-checkpoint-with-a-bashoperator)
2. [Running a Checkpoint in Airflow using a `python operator`](how_to_run_a_checkpoint_in_airflow.md#option-2-running-the-checkpoint-script-output-with-a-pythonoperator)
3. [Running a Checkpoint in Airflow using a `Airflow operator`](https://legacy.docs.greatexpectations.io/en/latest/guides/workflows_patterns/deployment_astronomer.html)

For our Deployment Guide we are going to use the `bash operator` to run the Checkpoint.

Also, this portion of the Guide is also found in the following Loom video

### 1. Create and Configure a Service Account

Create and configure a Service Account on GCS with the appropriate privileges needed to run Cloud Composer.

In order to run Great Expectations in a Google Cloud Composer environment you will need the following privileges:

- `Composer Administrator`
- `Composer Worker`
- `Logging Admin`
- `Storage Object Creator`
- `Storage Object Viewer`
- `Storage Object Admin`


If you are accessing data in BigQuery, please ensure your Service account also has privileges for

- `BigQuery Data Viewer`
- `BigQuery Job User`
- `BigQuery Read Session User`

Please follow the steps described in the [official documentation](https://cloud.google.com/iam/docs/service-accounts) to create a service account.

### 2. Create Google Cloud Composer environment

Create a Cloud Composer environment in the project you will be running Great Expectations. Please follow the steps described in the [official documentation](https://cloud.google.com/composer/docs/composer-2/create-environments) to create an environment that is suited for your needs.

:::info Note on Versions.
The current Deployment Guide was developed and tested in Great Expectations 0.13.49, Composer 1.17.7 and Airflow 2.0.2. Please ensure your Environment is equal or greater.
:::

### 3. Install Great Expectations in Google Cloud Composer

Installing Python dependencies can be done through the web Console, `gcloud` or through a REST query.  Please follow the steps describe in [Installing Python dependencies](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies#console) to install `great-expectations` in Google Cloud Composer.

:::info Troubleshooting Installation
If you run into trouble while installing Great Expectations in Cloud Composer, the [official documentation offers the following guide on troubleshooting PyPI package installation](https://cloud.google.com/composer/docs/troubleshooting-package-installation)
:::

### 4. Move local configuration to Google Cloud Composer

Next step is to move the local configuration in `great_expectations/` to Google Cloud Composer so that it can be orchestrated. It is easiest to move the entire to move the entire `great_expectations/` folder to the GCS, where Composer can access the configuration.

Cloud Composer uses Cloud Storage to store Apache Airflow DAGs, also known as workflows. Each environment has an associated Cloud Storage bucket. Cloud Composer schedules only the DAGs in the Cloud Storage bucket. The shared location is `gcs_fuse`.

Upload the `great_expectations/` folder to this location, and it will be accessible using `gcsfuse/` folder from the worker nodes.

### 5. Write DAG and Add to Google Cloud Composer

The [Official Documentation for Google Cloud Composer](https://cloud.google.com/composer/docs/how-to/using/writing-dags) provides input on how to write a DAG.

In our case we are creating a DAG with one node `t1` that runs our `BashOperator`. The operation that we run is to change to the `great_expectations/` directory that we uploaded in the previous step, and running the checkpoint using the
same CLI command we used to run the checkpoint locally using the `great_expectations --v3-api checkpoint run new_taxi_check`.

```python file=../../tests/integration/fixtures/gcp_deployment/ge_checkpoint.py
```

### 6. Run DAG / Checkpoint

Now that the DAG has been uploaded, we can [trigger the DAG](https://cloud.google.com/composer/docs/triggering-dags) using the following methods:

1. Trigger the DAG on a schedule
2. Trigger the DAG manually
3. Trigger the DAG in response to events.

For more details please look at the official documentation for [Cloud Composer](https://cloud.google.com/composer/docs/triggering-dags).

For our purposes, we will set up the DAG to run 1 time a day, and trigger it manually to see that it works.

We can go to the `ENVIRONMENT CONFIGURATION` for our `ENVIRONMENT` and go to the `Airflow web UI`, there we should see our DAG, and be able to trigger the DAG manually.

### 7. Check that DAG / Checkpoint has run successfully

If the DAG was able to be run successfully, we should first see a `SUCCESS` status appear on the DAGs page of the Airflow Web UI. We can also check so check that DATADOCS have been generated by going to our app folder.

: Success! We have successfully migrated our local configuration to Google Cloud Composer.

### 8. Iterate and Improve.

In this deployment guide we have specifically elected to deploy the simplest version of migrating the deployment to Composer. There are

** please don't hesitate to reach out to us if you run into problems. We are very active on Great Expectation Slack.***
