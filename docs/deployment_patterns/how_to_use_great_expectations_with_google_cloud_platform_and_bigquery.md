---
title: How to Use Great Expectations with Google Cloud Platform and BigQuery
---
import Prerequisites from './components/deployment_pattern_prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Congratulations from '../guides/connecting_to_your_data/components/congratulations.md'

Great Expectations works well with many types of [Google Cloud Platform](https://cloud.google.com/gcp) (GCP) workflows. This guide with help you integrate Great Expectations with GCP using our recommended workflow.

We recommend that you use Great Expectations in GCP by using the following services:
  - [Google Cloud Composer](https://cloud.google.com/composer) (GCC) for managing workflow orchestration including validating your data. GCC is built on [Apache Airflow](https://airflow.apache.org/).
  - [BigQuery](https://cloud.google.com/bigquery) or files in [Google Cloud Storage](https://cloud.google.com/storage) (GCS) as your [Datasource](../reference/datasources.md)
  - [GCS](https://cloud.google.com/storage) for storing metadata ([Expectation Suites](../reference/expectations/expectations.md), [Validation Results](../reference/validation.md), [Data Docs](../reference/data_docs.md))
  - [Google App Engine](https://cloud.google.com/appengine) (GAE) for hosting and controlling access to [Data Docs](../reference/data_docs.md).

We also recommend that you deploy Great Expectations to GCP in two steps:
1. Developing a local configuration for GE that uses GCP services to connect to you data and store Great Expectations metadata.
2. Migrating the local configuration to Google Cloud Composer so that the workflow can be orchestrated automatically on GCP.

In line with our recommendation this document is presented in 2 parts:

In part 1(link!!) we will be building a local configuration of Great Expectations to connect to the correct Metadata Stores on GCP (for [Expectation Suites](../reference/expectations/expectations.md), [Validation Results](../reference/validation.md), and [Data Docs](../reference/data_docs.md)), as well as data in BigQuery or Google Cloud Storage.
We will be setting up and testing our configuration by checking that we can set up a run a Checkpoint locally. Then in Part 2(link!!), we will describe how the local Great Expectations configuration can be migrated to Google Cloud Composer and the same checkpoint be run as part of our automated workflow.

The following diagram shows the recommended components for a Great Expectations deployment in Google Cloud Platform.

![Screenshot of Data Docs](../deployment_patterns/images/ge_and_gcp_diagram_draft.png)

If you use a different configuration and have ideas about how this guide can be improved, please connect with us on [Slack](https://greatexpectationstalk.slack.com/ssb/redirect#/shared-invite/email) or submit a [PR to our Github repository](https://github.com/great-expectations/great_expectations) - we love to make things better!

Before you begin please, ensure you have fulfilled the following Prerequisites:

<Prerequisites>

- Have a working local installation of Great Expectations that is at least version 0.13.49.
- Have read through the documentation and are familiar with the Google Cloud Platform features that are used in this guide.
- Have completed the set-up of a GCP project with a running Google Cloud Storage container that is accessible from your region, and read/write access to a BigQuery database if that is where you are loading your data.
- Access to a GCP [Service Account](https://cloud.google.com/iam/docs/service-accounts) with permission to access and read objects in Google Cloud Storage, and read/write access to a BigQuery database if this is where you are loading your data.

</Prerequisites>

## Part 1: Local Configuration of Great Expectations that connects to Google Cloud Platform

### 1. If necessary, upgrade your Great Expectations version

The current guide was developed and tested using Great Expectations 0.13.49, so please ensure that your current version is equal or newer than this.

A local installation of Great Expectations can be upgraded using a simple `pip install` command with the `--upgrade` flag.

```bash
pip install great-expectations --upgrade
```

### 2. Connect to Metadata Stores on GCP

The following sections will describe how you can take a basic local configuration of Great Expectations and connect it to Metadata stores on GCP.
Most of the detail around the configuration, along with full working-scripts that can be used to build the configuration can be found in the linked documents that describe `Setting Up Great Expectations.`

#### Add Expectation Store
By default, newly profiled Expectations are stored in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder. Please follow the guide
[How to configure an Expectation store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_in_gcs.md) to configure and test your Expectation Store, and return to this Deployment Guide.

At this point, you will have a local configuration containing the following Expectation Store,
```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L38-L44
```

And have configured Great Expectations to use it to store Expectations.
```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L72
```

#### Add Validation Store
By default, Validations are stored in JSON format in the `uncommitted/validations/` subdirectory of your `great_expectations/` folder.
Please follow the guide  [How to configure an Validation Result store to use GCS](../guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_in_gcs.md) to configure and test your Validation Result store, and return to this Deployment Guide.

At this point, you will have a local configuration containing the following Validation Result store,

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L52-L58
```
And have configured Great Expectations to use it to store Validation Results.

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L73
```

#### Add DataDocs Store
To host and share Datadocs on GCS, we recommend following the guide on how to [How to host and share Datadocs on GCS](../guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_gcs.md), which will explain how to host and share Data Docs on Google Cloud Storage using IP-based access. Afterwards, please return to this Deployment Guide.

At this point, you will have a local configuration containing the following DataDocs store:

```YAML file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L91-L99
```

By running the following CLI command, please test to see that you have successfully deployed DataDocs.
```bash
gcloud app browse
```

### 3. Connect to your Data

<Tabs
  groupId="gcs-or-bigquery-yaml-python"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS-yaml conf', value:'gcs-yaml'},
  {label: 'Data in GCS-python conf', value:'gcs-python'},
  {label: 'Data in BigQuery-yaml conf', value:'bigquery-yaml'},
  {label: 'Data in BigQuery-python conf', value:'bigquery-python'},
  ]}>
<TabItem value="gcs-yaml">

Using these example configurations, add in your GCS bucket and path to a directory that contains some of your data:

The below configuration is representative of the default setup you'll see when preparing your own environment.

```python file=../../tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs_yaml_configs.py#L219-L237
```

For more information on the different ways that Datasource can be configured, please look at the following documentation:

</TabItem>
<TabItem value="bigquery-yaml">

First, install the necessary dependencies for Great Expectations to connect to your BigQuery database by running the following in your terminal:

The below configuration is representative of the default setup you'll see when preparing your own environment.

```python file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L38-L44
```

For more information on the different ways that Datasource can be configured, please look at the following.

```python file=../../tests/integration/fixtures/gcp_deployment/great_expectations/great_expectations.yml#L38-L44
```

</TabItem>
</Tabs>

### 3. Build BatchRequest




### 4. Get Validator and Build ExpectationSuite


### 5. Run Checkpoint

Success. you can see whether your validation results have appeared in the correct DataDocs place. You can go and see you should be abel to see the
* In the correct*
* Validation Results
* Expectation Suites
* DataDocs.

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
