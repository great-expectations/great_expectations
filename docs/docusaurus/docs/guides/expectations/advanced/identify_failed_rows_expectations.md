---
sidebar_label: 'Identify failed table rows in an Expectation'
title: 'Identify failed rows in an Expectation'
id: identify_failed_rows_expectations
description: Use 'unexpected_index_column_names' in the 'result_format' parameter to identify failed table rows in an Expectation.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Quickly identifying problematic rows can reduce troubleshooting effort when an Expectation fails. After identifying the failed row, you can modify or remove the value in the table and run the Expectation again.

We will use a small example table that stores visitor events for a [webpage.](https://github.com/great-expectations/great_expectations/tree/develop/tests/test_sets/visits)

| **event_id** | **visit_id** | **date** | **event_type** |   |
|--------------|--------------|----------|----------------|---|
| 0            | 1470408760   | 20220104 | page_load      |   |
| 1            | 1470429409   | 20220104 | page_load      |   |
| 2            | 1470441005   | 20220104 | page_view      |   |
| 3            | 1470387700   | 20220104 | user_signup    |   |
| 4            | 1470438716   | 20220104 | purchase       |   |
| 5            | 1470420524   | 20220104 | download       |   |


In our example we will use a a Checkpoint (`my_checkpoint`) that runs an ExpectationSuite (`visitors_exp`) with 1 Expectation type : `ExpectColumnValuesToBeInSet` ont he `event_type` column. The `set`
will be `["page_load", "page_view]`, which means the rows with `user_signup`, `purchase`, and `download` will fail the Expectation.

All the example code is located in the [primary_keys_in_validation_results GitHub repository](https://github.com/great-expectations/great_expectations/tree/develop/examples/demos/primary_keys_in_validation_results).

## Prerequisites

- [A working installation of Great Expectations](/docs/guides/setup/setup_overview).

## Import the Great Expectations module and instantiate a Data Context

Use the `get_context()` method to create a new Data Context:

```python name="tests/integration/docusaurus/expectations/advanced/failed_rows_pandas.py get context"
```

## Import the Checkpoint 

Run the following code to import the example Checkpoint:

```python name="tests/integration/docusaurus/expectations/advanced/failed_rows_pandas.py get context"
```

## Set the `unexpected_index_column_names` parameter

The failed rows will be represented using the values defined in the `unexpected_index_column_names` parameter. 

In our example, we are setting it to `event_id`, which means we would like to see the `event_ids` of the rows that fail the Expectation, but `unexpected_index_column_names` can also be a list of columns that you would like the failed rows to be respresented by. The Checkpoint will also return the `unexpected_index_query`, which can be used to retrieve the full set of failed results from the data. 

In our example, we are also setting  the `result_format` to `COMPLETE`, which means we are getting the full set of results. More information about `result_format` can be found [here](https://docs.greatexpectations.io/docs/reference/expectations/result_format/#configure-result-format).

```python name="tests/integration/docusaurus/expectations/advanced/failed_rows_pandas.py set unexpected_index_column_names"
```

Optional. To suppress the query, add the `return_unexpected_index_query` parameter and set it to `False`. For example:

```python
result_format: dict = {
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["event_id"],
    "return_unexpected_index_query" : False
}
```

## Run Checkpoint using `result_format`

To apply the updated `result_format` to every Expectation in a Suite, you can pass it directly to the `run()` method. 

```python name="tests/integration/docusaurus/expectations/advanced/failed_rows_pandas.py set unexpected_index_column_names"
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

The following is an example of the information returned after running the Checkpoint. For Pandas, a filter condition on the DataFrame is included.

```python
{
    "element_count": 6,
    "unexpected_count": 3,
    "unexpected_percent": 50.0,
    "partial_unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_column_names": ["event_id"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 50.0,
    "unexpected_percent_nonmissing": 50.0,
    "partial_unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3},
        {"event_type": "purchase", "event_id": 4},
        {"event_type": "download", "event_id": 5},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_type": "user_signup", "event_id": 3},
        {"event_type": "purchase", "event_id": 4},
        {"event_type": "download", "event_id": 5},
    ],
    "unexpected_index_query": "df.filter(items=[3, 4, 5], axis=0)",
}
```

</TabItem>
<TabItem value="spark">

The following is an example of the information returned after running the Checkpoint. For Spark, a filter condition on the DataFrame is included.


```python 
{
    "element_count": 6,
    "unexpected_count": 3,
    "unexpected_percent": 50.0,
    "partial_unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_column_names": ["event_id"],
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 50.0,
    "unexpected_percent_nonmissing": 50.0,
    "partial_unexpected_index_list": [
        {"event_id": 3, "event_type": "user_signup"},
        {"event_id": 4, "event_type": "purchase"},
        {"event_id": 5, "event_type": "download"},
    ],
    "partial_unexpected_counts": [
        {"value": "download", "count": 1},
        {"value": "purchase", "count": 1},
        {"value": "user_signup", "count": 1},
    ],
    "unexpected_list": ["user_signup", "purchase", "download"],
    "unexpected_index_list": [
        {"event_id": 3, "event_type": "user_signup"},
        {"event_id": 4, "event_type": "purchase"},
        {"event_id": 5, "event_type": "download"},
    ],
    "unexpected_index_query": "df.filter(F.expr((event_type IS NOT NULL) AND (NOT (event_type IN (page_load, page_view)))))",
}

```

</TabItem>
<TabItem value="sqlalchemy">

The following is an example of the information returned after running the Checkpoint. For SQLAlchemy, the output includes a query that can be used directly against the database backend.

</TabItem>
</Tabs>

