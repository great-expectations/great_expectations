---
title: How to add Statement Renderers for Custom Expectations
---

import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you implement Statement Renderers for your Custom Expectations.

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>

We will add Statement Renderers to the Custom Expectations built in the guides for [creating Custom Column Aggregate Expectations](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md) 
and [creating Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).

Great Expectations supports a number of Renderers, allowing you to control how your Custom Expectations are displayed in your [Data Docs](../../../reference/data_docs.md).

Implementing renderers as part of your Custom Expectations is not strictly required - if not provided, Great Expectations will render your Custom Expectation using a basic default renderer:

![Expectation rendered using default renderer](../../../images/expectation_fallback.png)

If you decide to contribute your Expectation, its entry in the [Expectations Gallery](https://greatexpectations.io/expectations/) will reflect the Renderers that it supports.

## Steps

### 1. Decide which renderer type to implement

There are two basic types of Statement Renderers:

- `renderer.prescriptive` renders a human-readable form of your Custom Expectation
- `renderer.diagnostic` renders diagnostic information about the results of your Custom Expectation

Prescriptive Renderers help provide clarity and structure in your Data Docs.

Diagnostic Renderers allow you to serve 
summary statistics, unexpected value samples, and observed values from your Custom Expectation, 
delivering further insights about your data.

<details>
<summary>But what do they look like?</summary>
There are several ways to implement Prescriptive and Diagnostic Renderers. The image below gives some examples of what 
these Renderers look like in action!
<br/><br/>

![Annotated Validation Result Example](../../../images/validation_result_example.png)
</details>

<Tabs
  groupId="renderer-types"
  defaultValue='prescriptive'
  values={[
  {label: 'Prescriptive', value:'prescriptive'},
  {label: 'Diagnostic', value:'diagnostic'},
  ]}>

<TabItem value="prescriptive">

### 2. Declare the method for your Prescriptive Renderer
<br/>

In general, Prescriptive Renderers receive an ExpectationConfiguration as input and return a list of rendered elements.

There are several ways to implement a Prescriptive Renderer. We're going to implement a String Template Renderer and a Table Renderer. 
Both of these implementations will share the `@renderer(renderer_type="renderer.prescriptive)` decorator, the `@render_evaluation_parameter_string` decorator, and the same initial method declaration. In our case, this looks like the following:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L233-L266
```

:::note
While not strictly necessary for all Custom Expectations, 
adding the ``@render_evaluation_parameter_string`` decorator allows [Expectations that use Evaluation Parameters](../../../guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.md) 
to render the values of the Evaluation Parameters along with the rest of the output.
:::

<Tabs
  groupId="prescriptive-types"
  defaultValue="string-template"
  values={[
  {label: 'String Template', value: 'string-template'},
  {label: 'Table', value: 'table'}
]}>

<TabItem value="string-template">

### 3. Implement the logic for your String Template Renderer
<br/>

The String Template Prescriptive Renderer will render a semantic declaration of your Custom Expectation populated with the parameters your Custom Expectation requires.

:::note
The following example is being implemented in the Custom Expectation built in our [guide on how to create Custom Column Aggregate Expectations](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md).
:::

To make this happen, we're going to build a template string based off of which parameters have been provided:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L268-L292
```

Then we return our string template, including the parameters that will populate and render it:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L294-L305
```

### 4. Verifying your implementation
<br/>

If the core logic for your Custom Expectation is already complete, you can now utilize your Custom Expectation in an [Expectation Suite](../../../tutorials/getting_started/tutorial_create_expectations.md) 
and validate against your data with a [Checkpoint](../../../tutorials/getting_started/tutorial_validate_data.md), 
and see something similar to the following in your Data Docs:

![String Template Example](../../../images/string_template.png)

</TabItem>

<TabItem value="table">

### 3. Implement the logic for your Table Renderer
<br/>

The Table Renderer will render a semantic declaration of your Custom Expectation, including a table of values populated with the parameters your Custom Expectation requires.

:::note
The following example can be found in [`ExpectColumnQuantileValuesToBeBetween`](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py).
:::

To make this happen, we first build our template string from our parameters:

```python file=../../../../great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py#L357-L378
```

Then we build the table from our parameters:

```python file=../../../../great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py#L380-L416
```

Finally, we return both our template string and our table:

```python file=../../../../great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py#L418
```

### 4. Verifying your implementation
<br/>

If the core logic for your Custom Expectation is already complete, you can now utilize your Custom Expectation in an [Expectation Suite](../../../tutorials/getting_started/tutorial_create_expectations.md) 
and validate against your data with a [Checkpoint](../../../tutorials/getting_started/tutorial_validate_data.md), 
and see something similar to the following in your Data Docs:

![Table Example](../../../images/table.png)

</TabItem>
</Tabs>

If you have already implemented one of the Diagnostic Renderers covered elsewhere in this guide, you should now see the following when you call the `print_diagnostic_checklist()` method on your Custom Expectation:

```console
✔ Has both Statement Renderers: prescriptive and diagnostic
```

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've successfully implemented a Prescriptive Renderer for a Custom Expectation! &#127881;
</b></p>
</div>

</TabItem>

<TabItem value="diagnostic">

### 2. Declare the method for your Diagnostic Renderer
<br/>

In general, Diagnostic Renderers receive an ExpectationValidationResult as input and return a list of rendered elements.

There are several ways to implement a Diagnostic Renderer. We're going to implement an Unexpected Statement Renderer, an Unexpected Table Renderer, and an Observed Value Renderer. 
All three of these implementations will utilize the `@render_evaluation_parameter_string` decorator, and the same initial method declaration. 

Each will have a slightly different `@renderer` decorators and method names reflecting the type of Diagnostic Renderer being implemented, but will have similar initial declarations.

Here is an example for the Observed Value Renderer:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L178-L188
```

:::note
While not strictly necessary for all Custom Expectations, 
adding the ``@render_evaluation_parameter_string`` decorator allows [Expectations that use Evaluation Parameters](../../../guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters.md) 
to render the values of the Evaluation Parameters along with the rest of the output.
:::

<Tabs
  groupId="diagnostic-types"
  defaultValue="observed-value"
  values={[
  {label: 'Observed Value', value: 'observed-value'},
  {label: 'Unexpected Statement', value: 'unexpected-statement'},
  {label: 'Unexpected Table', value: 'unexpected-table'}
]}>

<TabItem value="observed-value">

### 3. Implement the logic for your Observed Value Renderer
<br/>

The Observed Value Diagnostic Renderer will render the `observed_value` or `unexpected_percent` returned by your Custom Expectation, if one is returned.

:::note
The following example is being implemented in the Custom Expectation built in our [guide on how to create Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).
:::

To make this happen, we're going to access the results of our Custom Expectation:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L190-L192
```

And return the observed value or unexpected percent in our results, transforming those values to strings if necessary:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L194-L207
```

### 4. Verifying your implementation
<br/>

If the core logic for your Custom Expectation is already complete, you can now utilize your Custom Expectation in an [Expectation Suite](../../../tutorials/getting_started/tutorial_create_expectations.md) 
and validate against your data with a [Checkpoint](../../../tutorials/getting_started/tutorial_validate_data.md), when you build your Data Docs
you should see an observed value or unexpected percent populating the `Observed Value` column of your validation results.

</TabItem>

<TabItem value="unexpected-statement">

### 3. Implement the logic for your Unexpected Statement Renderer
<br/>

The Unexpected Statement Diagnostic Renderer will render summary statistics returned by your Custom Expectation, if `unexpected_count` and `element_count` are returned.

:::note
The following example is being implemented in the Custom Expectation built in our [guide on how to create Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).
:::

To make this happen, we're going to access the results of our Custom Expectation:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L221-L222
```

If our Custom Expectation raised an exception, we're going to build an exception template string:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L224-L227
```

And return the rendered exception and traceback:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L229-L273
```

If our Custom Expectation succeeds, or encounters no unexpected values, we return an empty list:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L275-L276
```

Otherwise, we build a template string summarizing our unexpected results, and return the rendered string:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L277-L309
```

### 4. Verifying your implementation
<br/>

If the core logic for your Custom Expectation is already complete, you can now utilize your Custom Expectation in an [Expectation Suite](../../../tutorials/getting_started/tutorial_create_expectations.md) 
and validate against your data with a [Checkpoint](../../../tutorials/getting_started/tutorial_validate_data.md). When you build your Data Docs, 
if your Custom Expectation encountered unexpected values, you should see a statement in the `Expectation` column of your validation results detailing 
summary statistics about those unexpected values.

</TabItem>

<TabItem value="unexpected-table">

### 3. Implement the logic for your Unexpected Table Renderer
<br/>

The Unexpected Table Diagnostic Renderer will render a sample of unexpected values or value counts returned by your Custom Expectation, if `partial_unexpected_list` or `partial_unexpected_counts` are returned.

:::note
The following example is being implemented in the Custom Expectation built in our [guide on how to create Custom Column Map Expectations](../creating_custom_expectations/how_to_create_custom_column_map_expectations.md).
:::

To make this happen, we're going to access the results of our Custom Expectation, and return `None` if we don't have the required data:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L321-L332
```

Then, we're going to start building our table:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L334
```

If we have `partial_unexpected_counts`, we're going to total the counts for each value, and build rows of `value` and `count`. 
If we don't have all of the unexpected values available, we reduce our table from `value` and `count` down to just the `Sampled Unexpected Values`:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L336-L355
```

If we don't have `partial_unexpected_counts`, we build our table of `Sampled Unexpected Values`:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L357-L369
```

And return the rendered table:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py#L371-L382
```

### 4. Verifying your implementation
<br/>

If the core logic for your Custom Expectation is already complete, you can now utilize your Custom Expectation in an [Expectation Suite](../../../tutorials/getting_started/tutorial_create_expectations.md) 
and validate against your data with a [Checkpoint](../../../tutorials/getting_started/tutorial_validate_data.md). When you build your Data Docs, 
if your Custom Expectation encountered unexpected values, you should see a table in the `Expectation` column of your validation results with a sampling of those values.

</TabItem>
</Tabs>

If you have already implemented one of the Prescriptive Renderers covered elsewhere in this guide, you should now see the following when you call the `print_diagnostic_checklist()` method on your Custom Expectation:

```console
✔ Has both Statement Renderers: prescriptive and diagnostic
```

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've successfully implemented a Diagnostic Renderer for a Custom Expectation! &#127881;
</b></p>
</div>

</TabItem>
</Tabs>

### 5. Contribution (Optional)

Renderers are not a requirement for for [contribution](../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md) back to Great Expectations at an Experimental level.

Implementing at least one Prescriptive and Diagnostic Renderer from this guide is required for contribution back to Great Expectations at a Beta or Production level.

If you believe your Custom Expectation is ready for contribution, please submit a [Pull Request](https://github.com/great-expectations/great_expectations/pulls), and we will work with you to ensure your Custom Expectation meets these standards.

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full scripts used in this page, see them on GitHub:
- [expect_column_max_to_be_between_custom.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py)
- [expect_column_values_to_equal_three.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_equal_three.py)
- [expect_column_quantile_values_to_be_between.py](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/core/expect_column_quantile_values_to_be_between.py)
:::