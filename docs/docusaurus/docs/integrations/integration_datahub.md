---
title: Integrating DataHub With Great Expectations
authors:
    name: Mayuri Nehate, John Joyce, Maggie Hays 
    url: https://datahubproject.io
description: "DataHub"
sidebar_custom_props: { icon: 'img/integrations/datahub_icon.png' }
---

:::info
* Maintained By: DataHub
* Status: Beta
* Support/Contact: https://slack.datahubproject.io/
:::

This integration allows you to push the results of running Expectations into DataHub (https://datahubproject.io/). DataHub is a metadata platform which enables search & discovery, federated governance, and data observability for the Modern Data Stack.


### Technical background
There is a custom Action named `DataHubValidationAction` which allows you to view Expectation Results inside of DataHub.

### Prerequisites

 - A [Great Expectations Checkpoint](https://docs.greatexpectations.io/docs/terms/checkpoint)
 - [A DataHub instance](https://datahubproject.io/docs/quickstart)

`DataHubValidationAction` pushes Expectations metadata to DataHub. This includes

- **Expectation Details**: Details of assertions (i.e. Expectation) set on a Dataset (Table). Expectation set on a dataset in GX aligns with `AssertionInfo` aspect in DataHub. `AssertionInfo` captures the dataset and dataset fields on which assertion is applied, along with its scope, type and parameters. 
- **Expectation Results**: Evaluation results for an assertion tracked over time. 
Validation Result for an Expectation in GX align with `AssertionRunEvent` aspect in DataHub. `AssertionRunEvent` captures the time at which Validation was run, Batch(subset) of dataset on which it was run, the success status along with other result fields.


### Dev loops unlocked by integration
* View dataset and column level Expectations set on a dataset
* View time-series history of Expectation's outcome (pass/fail)
* View current health status of dataset

### Setup

Install the required dependency in your Great Expectations environment.  
```shell
pip install 'acryl-datahub[great-expectations]'
```

## Usage

:::tip
Stand up and take a breath
:::

####  1. Ingest the metadata from source data platform into DataHub
For example, if you have GX Checkpoint that runs Expectations on a BigQuery dataset, then first
ingest the respective dataset into DataHub using [BigQuery](https://datahubproject.io/docs/generated/ingestion/sources/bigquery#module-bigquery) metadata ingestion source recipe. 

```bash
datahub ingest -c recipe.yaml
```
You should be able to see the dataset in DataHub UI.

#### 2. Update GX Checkpoint Configurations
Add `DataHubValidationAction` in `action_list` of your Great Expectations Checkpoint. For more details on setting action_list, see [the configuration section of the GX Actions reference entry](https://docs.greatexpectations.io/docs/terms/action#configuration) 
```yml
action_list:
  - name: datahub_action
    action:
      module_name: datahub.integrations.great_expectations.action
      class_name: DataHubValidationAction
      server_url: http://localhost:8080 #DataHub server url
```

**Configuration options:**
- `server_url` (required): URL of DataHub GMS endpoint
- `env` (optional, defaults to "PROD"): Environment to use in namespace when constructing dataset URNs.
- `platform_instance_map` (optional): Platform instance mapping to use when constructing dataset URNs. Maps the GX 'data source' name to a platform instance on DataHub. e.g. `platform_instance_map: { "datasource_name": "warehouse" }`
- `graceful_exceptions` (defaults to true): If set to true, most runtime errors in the lineage backend will be suppressed and will not cause the overall Checkpoint to fail. Note that configuration issues will still throw exceptions.
- `token` (optional): Bearer token used for authentication.
- `timeout_sec` (optional): Per-HTTP request timeout.
- `retry_status_codes` (optional): Retry HTTP request also on these status codes.
- `retry_max_times` (optional): Maximum times to retry if HTTP request fails. The delay between retries is increased exponentially.
- `extra_headers` (optional): Extra headers which will be added to the datahub request.
- `parse_table_names_from_sql` (defaults to false): The integration can use an SQL parser to try to parse the datasets being asserted. This parsing is disabled by default, but can be enabled by setting `parse_table_names_from_sql: True`.  The parser is based on the [`sqllineage`](https://pypi.org/project/sqllineage/) package.

#### 3. Run the GX checkpoint

Run the following command to retrieve and run a Checkpoint:

```python name="tests/integration/docusaurus/reference/glossary/checkpoints.py retrieve_and_run"
```

#### 4. Hurray!
The Validation Results would show up in Validation tab on Dataset page in DataHub UI. 


## Further discussion

### Things to consider
This integration does not support

- Datasources using an Execution Engine other than `SqlAlchemyExecutionEngine` (Spark, Pandas)
- Cross-dataset Expectations (those involving > 1 table)

### When things don't work

- Follow [Debugging](https://datahubproject.io/docs/metadata-ingestion/integration_docs/great-expectations/#debugging) section to see what went wrong!
- Feel free to ping us on [DataHub Slack](https://slack.datahubproject.io/)!


### Other resources

 - [Demo](https://www.loom.com/share/d781c9f0b270477fb5d6b0c26ef7f22d) of Great Expectations Datahub Integration in action
 - DataHub [Metadata Ingestion Sources](https://datahubproject.io/docs/metadata-ingestion)