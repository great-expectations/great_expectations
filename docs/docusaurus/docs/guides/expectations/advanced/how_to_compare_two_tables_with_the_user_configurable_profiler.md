---
title: How to compare two tables with the UserConfigurableProfiler
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

import InProgress from '/docs/components/warnings/_in_progress.md'

<InProgress />

In this guide, you will utilize a <TechnicalTag tag="profiler" text="UserConfigurableProfiler" /> to create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> that can be used to gauge whether two tables are identical. This workflow can be used, for example, to validate migrated data.

<Prerequisites>

- Have a basic understanding of [Expectation Configurations in Great Expectations](https://docs.greatexpectations.io/docs/reference/expectations/expectations).
- Have read the overview of <TechnicalTag tag="profiler" text="Profilers" /> and the section on [UserConfigurableProfilers](../../../terms/profiler.md#userconfigurableprofiler) in particular.

</Prerequisites>


## Steps

### 1. Decide your use-case

This workflow can be applied to batches created from full tables, or to batches created from queries against tables. These two approaches will have slightly different workflows detailed below.

<Tabs
  groupId="tables"
  defaultValue='full-table'
  values={[
  {label: 'Full Table', value:'full-table'},
  {label: 'Query', value:'query'},
  ]}>

<TabItem value="full-table">

### 2. Set-Up
<br/>

In this workflow, we will be making use of the `UserConfigurableProfiler` to profile against a <TechnicalTag tag="batch_request" text="BatchRequest" /> representing our source data, and validate the resulting suite against a `BatchRequest` representing our second set of data.

To begin, we'll need to set up our imports and instantiate our <TechnicalTag tag="data_context" text="Data Context" />:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py imports"
```

:::note
Depending on your use-case, workflow, and directory structures, you may need to update you context root directory as follows:
```python
context = gx.get_context(
    context_root_dir='/my/context/root/directory/great_expectations'
)
```
:::

### 3. Create Batch Requests
<br/>

In order to profile our first table and validate our second table, we need to set up our Batch Requests pointing to each set of data.

In this guide, we will use a MySQL <TechnicalTag tag="datasource" text= "Datasource" /> as our source data -- the data we trust to be correct.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py mysql_batch_request"
```

From this data, we will create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and use that suite to validate our second table, pulled from a PostgreSQL Datasource.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py pg_batch_request"
```

### 4. Profile Source Data
<br/>

We can now use the `mysql_batch_request` defined above to build a <TechnicalTag tag="validator" text="Validator" />:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py get_validator"
```

Instantiate our `UserConfigurableProfiler`:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py profiler"
```

And use that profiler to build and save an Expectation Suite:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py build_suite"
```

<details>
<summary><code>excluded_expectations</code>?</summary>
Above, we excluded <code>expect_column_quantile_values_to_be_between</code>, as it isn't fully supported by some SQL dialects.

This is one example of the ways in which we can customize the Suite built by our Profiler.

For more on these configurations, see our [guide on the optional parameters available with the `UserConfigurableProfiler`](../../../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md#optional-parameters).
</details>

### 5. Checkpoint Set-Up
<br/>

Before we can validate our second table, we need to define a <TechnicalTag tag="checkpoint" text="Checkpoint" />.

We will pass both the `pg_batch_request` and Expectation Suite defined above to this checkpoint.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py checkpoint_config"
```

### 6. Validation
<br/>

Finally, we can use our Checkpoint to validate that our two tables are identical:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison.py run_checkpoint"
```

If we now inspect the results of this Checkpoint (`results["success"]`), we can see that our Validation was successful!

By default, the Checkpoint above also updates your Data Docs, allowing you to further inspect the results of this workflow.

</TabItem>

<TabItem value="query">

### 2. Set-Up
<br/>

In this workflow, we will be making use of the `UserConfigurableProfiler` to profile against a <TechnicalTag tag="batch_request" text="RuntimeBatchRequest" /> representing a query against our source data, and validate the resulting suite against a `RuntimeBatchRequest` representing a query against our second set of data.

To begin, we'll need to set up our imports and instantiate our <TechnicalTag tag="data_context" text="Data Context" />:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py imports"
```

:::note
Depending on your use-case, workflow, and directory structures, you may need to update you context root directory as follows:
```python
context = gx.get_context(
    context_root_dir='/my/context/root/directory/great_expectations'
)
```
:::

### 3. Create Batch Requests
<br/>

In order to profile our first table and validate our second table, we need to set up our Batch Requests pointing to each set of data. These will be `RuntimeBatchRequests`, specifying a query against our data to be executed at runtime.

In this guide, we will use a MySQL <TechnicalTag tag="datasource" text= "Datasource" /> as our source data -- the data we trust to be correct.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py mysql_runtime_batch_request"
```

From this data, we will create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and use that suite to validate our second table, pulled from a PostgreSQL Datasource.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py pg_runtime_batch_request"
```

### 4. Profile Source Data
<br/>

We can now use the `mysql_runtime_batch_request` defined above to build a <TechnicalTag tag="validator" text="Validator" />:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py get_validator"
```

Instantiate our `UserConfigurableProfiler`:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py profiler"
```

And use that profiler to build and save an Expectation Suite:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py build_suite"
```

<details>
<summary><code>excluded_expectations</code>?</summary>
Above, we excluded <code>expect_column_quantile_values_to_be_between</code>, as it isn't fully supported by some SQL dialects.

This is one example of the ways in which we can customize the Suite built by our Profiler.

For more on these configurations, see our [guide on the optional parameters available with the `UserConfigurableProfiler`](../../../guides/expectations/how_to_create_and_edit_expectations_with_a_profiler.md#optional-parameters).
</details>

### 5. Checkpoint Set-Up
<br/>

Before we can validate our second table, we need to define a <TechnicalTag tag="checkpoint" text="Checkpoint" />.

We will pass both the `pg_runtime_batch_request` and Expectation Suite defined above to this checkpoint.

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py checkpoint_config"
```

### 6. Validation
<br/>

Finally, we can use our Checkpoint to validate that our two batches of data - queried from two different tables - are identical:

```python name="tests/integration/docusaurus/expectations/advanced/user_configurable_profiler_cross_table_comparison_from_query.py run_checkpoint"
```

If we now inspect the results of this Checkpoint (`results["success"]`), we can see that our Validation was successful!

By default, the Checkpoint above also updates your Data Docs, allowing you to further inspect the results of this workflow.

</TabItem>

</Tabs>

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just compared two tables across Datasources! &#127881;
</b></p>
</div>
