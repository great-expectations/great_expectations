---
title: Integrating DataHub With Great Expectations
authors:
    name: AUTHOR NAME
    url: https://datahubproject.io/
---

:::info
* Maintained By: DataHub
* Status: Beta
* Support/Contact: https://slack.datahubproject.io/
:::

### Introduction
With this integration, Great Expectations's validation results are integrated with DataHub as Data Quality metadata (Assertion) associated with Datasets. DataHub pulls metadata aspects such as Schema, Ownership, Lineage of Dataset from a variety of data platforms. With access to Validations metadata from Great Expectations, users can view Data Quality checks in DataHub UI along with other metadata from source data platforms.


### Technical background
We have implemented [Custom Action](https://docs.greatexpectations.io/docs/terms/action#how-to-create) named `DataHubValidationAction`

:::note Prerequisites
 - Created a [Great Expectations Checkpoint](https://docs.greatexpectations.io/docs/terms/checkpoint)
 - Have Access to a [DataHub Instance](https://datahubproject.io/docs/quickstart)
:::

`DataHubValidationAction` pushes assertions metadata to DataHub. This includes

- **Assertion Details**: Details of assertions (i.e. expectation) set on a Dataset (Table). Expectation set on a dataset in GE aligns with `AssertionInfo` aspect in DataHub. AssertionInfo captures the dataset and dataset fields on which assertion is applied, along with its scope, type and parameters. 
- **Assertion Results**: Evaluation results for an assertion tracked over time. 
Validation Result for an expectation in GE align with `AssertionRunEvent` aspect in DataHub. AssertionRunEvent captures the time at which validation was run, batch(subset) of dataset on which it was run, the success status along with other result fields.


### Dev loops unlocked by integration
* View dataset and column level expectations set on a dataset
* View time-series history of expectation's outcome (pass/fail)
* View current health status of dataset

### Setup

1. Install the required dependency in your Great Expectations environment.  
    ```shell
    pip install 'acryl-datahub[great-expectations]'
    ```


2. To add `DataHubValidationAction` in Great Expectations Checkpoint, add following configuration in action_list for your Great Expectations `Checkpoint`. For more details on setting action_list, see [Checkpoints and Actions](https://docs.greatexpectations.io/docs/reference/checkpoints_and_actions/) 
    ```yml
    action_list:
      - name: datahub_action
        action:
          module_name: datahub.integrations.great_expectations.action
          class_name: DataHubValidationAction
          server_url: http://localhost:8080 #DataHub server url
    ```

## Usage

:::tip
Stand up and take a breath
:::

####  1. Ingest the metadata from source data platform into DataHub
For example, if you have GE checkpoint that runs expectations on a BigQuery dataset,
ingest the respective bigquery project into DataHub using [BigQuery](https://datahubproject.io/docs/generated/ingestion/sources/bigquery#module-bigquery) metadata ingestion source. You should be able to see BigQuery dataset in DataHub UI.

#### 2. Update GE checkpoint Configurations
Follow [Setup](#setup) guide to add `DataHubValidationAction` that will push validation results to DataHub. 

#### 3. Run the GE checkpoint

#### 4. Hurray!
The Validation Results would show up in Validation tab on Dataset page in DataHub UI. 


## Further discussion

### Things to consider
Currently this integration only supports v3 api datasources using SqlAlchemyExecutionEngine.

This integration does not support

- v2 Datasources such as SqlAlchemyDataset
- v3 Datasources using execution engine other than SqlAlchemyExecutionEngine (Spark, Pandas)
- Cross-dataset expectations (those involving > 1 table)

### When things don't work

- Follow [Debugging](https://datahubproject.io/docs/metadata-ingestion/integration_docs/great-expectations/#debugging) section to see what went wrong!
- Feel free to ping us on [DataHub Slack](https://slack.datahubproject.io/)!


### Other resources

 - [Demo](https://www.loom.com/share/d781c9f0b270477fb5d6b0c26ef7f22d) of Great Expectations Datahub Integration in action 
 - Configuration Options for [DataHubValidationAction](https://datahubproject.io/docs/metadata-ingestion/integration_docs/great-expectations#setting-up)
 - DataHub [Metadata Ingestion Sources](https://datahubproject.io/docs/metadata-ingestion)
