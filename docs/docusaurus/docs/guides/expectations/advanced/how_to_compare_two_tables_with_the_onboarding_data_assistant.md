---
title: Compare two tables with the Onboarding Data Assistant
---
import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

In this guide, you will utilize a <TechnicalTag tag="data_assistant" text="Data Assistant" /> to create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> that can be used to gauge whether two tables are identical. This workflow can be used, for example, to validate migrated data.

## Prerequisites

<Prerequisites>

- A minimum of two configured [Datasources](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/connect_to_data_overview) and [Assets](https://docs.greatexpectations.io/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_a_sql_table)
- A basic understanding of how to [configure Expectation in Great Expectations](https://docs.greatexpectations.io/docs/reference/expectations/expectations)
- Completion of the <TechnicalTag tag="data_assistant" text="Data Assistants" /> overview

</Prerequisites>


## Set-Up

In this workflow, we will be making use of the `OnboardingDataAssistant` to profile against a <TechnicalTag tag="batch_request" text="BatchRequest" /> representing our source data, and validate the resulting suite against a `BatchRequest` representing our second set of data.

To begin, we'll need to import Great Expectations and instantiate our <TechnicalTag tag="data_context" text="Data Context" />:

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py imports"
```

:::note
Depending on your use-case, workflow, and directory structures, you may need to update you context root directory as follows:
```python
context = gx.get_context(
    context_root_dir='/my/context/root/directory/great_expectations'
)
```
:::

## Create Batch Requests

In order to profile our first table and validate our second table, we need to set up our Batch Requests pointing to each set of data.

In this guide, we will use a MySQL <TechnicalTag tag="datasource" text= "Datasource" /> as our source data -- the data we trust to be correct.

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py mysql_batch_request"
```

From this data, we will create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> and use that suite to validate our second table, pulled from a PostgreSQL Datasource.

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py pg_batch_request"
```

## Profile Source Data

We can now use the `OnboardingDataAssistant` to profile our MySQL data defined in the `mysql_batch_request` above.

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_assistant"
```

And use the results from the Data Assistant to build and save an Expectation Suite:

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py build_suite"
```

<details>
<summary><code>exclude_column_names</code>?</summary>
In the previous example, specific columns were excluded to prevent Expectations from being set against them.

Some dialects of SQL handle data types in different ways, which can lead to (among other things) mismatches in precision on some numbers.

In our hypothetical use case these inconsistencies are tolerated, and therefore Expectations are not set against the columns likely to generate the errors.

This is an example of how an Expectation Suite created by the Data Assistant can be customized.
For more on these configurations, see our [guide on the `OnboardingDataAssistant](../../../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md).
</details>

## Checkpoint Set-Up

Before we can validate our second table, we need to define a <TechnicalTag tag="checkpoint" text="Checkpoint" />.

We will pass both the `pg_batch_request` and the Expectation Suite defined above to this checkpoint.

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py checkpoint_config"
```

## Validation

Finally, we can use our Checkpoint to validate that our two tables are identical:

```python name="tests/integration/docusaurus/expectations/advanced/data_assistant_cross_table_comparison.py run_checkpoint"
```

If we now inspect the results of this Checkpoint (`results["success"]`), we can see that our Validation was successful!

By default, the Checkpoint above also updates your Data Docs, allowing you to further inspect the results of this workflow.

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just compared two tables across Datasources! &#127881;
</b></p>
</div>
