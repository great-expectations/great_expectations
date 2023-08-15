---
sidebar_label: 'Identify failed table rows in an Expectation'
title: 'Identify failed rows in an Expectation'
id: identify_failed_rows_expectations
description: Use 'unexpected_index_column_names' in the 'result_format' parameter to identify failed table rows in an Expectation.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Quickly identifying problematic rows can reduce troubleshooting effort when an Expectation fails. After identifying the failed row, you can modify or remove the value in the table and run the Expectation again.

In the following examples, you'll use sample data, a sample Checkpoint, and a sample Batch Request. All the example code is located in the [primary_keys_in_validation_results GitHub repository](https://github.com/great-expectations/great_expectations/tree/develop/examples/demos/primary_keys_in_validation_results).

## Prerequisites

- [A working installation of Great Expectations](/docs/guides/setup/setup_overview).

## Import the Great Expectations module and instantiate a Data Context

Use the `get_context()` method to create a new Data Context:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_context"
```

## Import the Checkpoint result and Batch Request

Run the following code to import the example Checkpoint result and Batch Request:

```python
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import BatchRequest
```

## Add the `unexpected_index_column_names` parameter

Open the Checkpoint `result_format` and add the `unexpected_index_column_names` parameter to identify the column names you want to examine. For example:

```python
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
}
```
In this example, the list of column names is a list of strings. You can add additional column names to meet your requirements.

Optional. To suppress the query, add the `return_unexpected_index_query` parameter and set it to `False`. For example:

```python
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
    "return_unexpected_index_query" : False
}
```

## Apply the updated `result_format` to the Checkpoint

To apply the updated `result_format` to every Expectation in a Suite, you define it in your Checkpoint configuration below the `runtime_configuration` key, or you can use the `result_format` parameter. 

The following is an example of the `result_format` parameter:

```python
results = context.run_checkpoint(
    checkpoint_name="my_checkpoint", result_format=result_format
)
```

The following is an example of the Checkpoint configuration:

```python
name: my_checkpoint
config_version: 1.0
template_name:
module_name: great_expectations.checkpoint
class_name: Checkpoint
run_name_template: '%Y-%M-foo-bar-template'
expectation_suite_name: visitors_exp
batch_request: {}
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
evaluation_parameters: {}
runtime_configuration: {}
validations:
  - batch_request:
      datasource_name: my_datasource
      data_connector_name: my_spark_data_connector
      data_asset_name: visits.csv
profilers: []
ge_cloud_id:
expectation_suite_ge_cloud_id:
```

## Add the udated Checkpoint configuration

Run the following code to add the updated Checkpoint configuration:

```python
context.add_checkpoint(**checkpoint_config)
```

## Retrieve and run the Checkpoint

Run the following code to retrieve and run your Checkpoint:

```python
results: CheckpointResult = context.run_checkpoint(
    checkpoint_name="my_checkpoint", result_format=result_format
)
```

## Open the Data Docs

Run the following code to open the Data Docs:

```python
context.open_data_docs()
```

## Review Validation Results

After you run the Checkpoint, a dictionary for each failed row is returned. Each dictionary contains the row’s identifier and the failed value. 

<Tabs
  groupId="identify_failed_rows_expectations"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark Azure Blob Storage', value:'spark'},
  {label: 'SQLAlchemy', value:'sqlalchemy'},
  ]}>
<TabItem value="pandas">

The following is an example of the information returned after running the Checkpoint. For pandas, the indices are included.

```python
{'element_count': 6,
 'unexpected_count': 3,
 'unexpected_percent': 50.0,
 'partial_unexpected_list': ['user_signup', 'purchase', 'download'],
 'unexpected_index_column_names': ['event_id'],
 'missing_count': 0,
 'missing_percent': 0.0,
 'unexpected_percent_total': 50.0,
 'unexpected_percent_nonmissing': 50.0,
 'partial_unexpected_index_list': [{'event_type': 'user_signup',
   'event_id': 3},
  {'event_type': 'purchase', 'event_id': 4},
  {'event_type': 'download', 'event_id': 5}],
 'partial_unexpected_counts': [{'value': 'download', 'count': 1},
  {'value': 'purchase', 'count': 1},
  {'value': 'user_signup', 'count': 1}],
 'unexpected_list': ['user_signup', 'purchase', 'download'],
 'unexpected_index_list': [{'event_type': 'user_signup', 'event_id': 3},
  {'event_type': 'purchase', 'event_id': 4},
  {'event_type': 'download', 'event_id': 5}],
 'unexpected_index_query': [3, 4, 5]}
```

</TabItem>
<TabItem value="spark">

The following is an example of the information returned after running the Checkpoint. For Spark, a filter condition on the DataFrame is included.

</TabItem>
<TabItem value="sqlalchemy">

The following is an example of the information returned after running the Checkpoint. For SQLAlchemy, the utput includes a query that can be used directly against the database backend.

</TabItem>
</Tabs>

