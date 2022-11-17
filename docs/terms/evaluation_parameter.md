---
id: evaluation_parameter
title: Evaluation Parameter
hoverText: A dynamic value used during Validation of an Expectation which is populated by evaluating simple expressions or by referencing previously generated metrics.
---

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

An Evaluation Parameter is a dynamic value used during <TechnicalTag relative="../" tag="validation" text="Validation" /> of an <TechnicalTag relative="../" tag="expectation" text="Expectation" /> which is populated by evaluating simple expressions or by referencing previously generated <TechnicalTag relative="../" tag="metric" text="Metrics" />.

### Features and promises

You can use Evaluation Parameters to configure Expectations to use dynamic values, such as a value from a previous step in a pipeline or a date relative to today.  Evaluation Parameters can be simple expressions such as math expressions or the current date, or reference Metrics generated from a previous Validation run.  During interactive development, you can even provide a temporary value that should be used during the initial evaluation of the Expectation.

### Relationship to other objects

Evaluation Parameters are used in Expectations when Validating data.  <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> use <TechnicalTag relative="../" tag="action" text="Actions" /> to store Evaluation Parameters in the <TechnicalTag relative="../" tag="evaluation_parameter_store" text="Evaluation Parameter Store" />.

## Use cases

<CreateHeader/>

When creating Expectations based on introspection of Data, it can be useful to reference the results of a previous Expectation Suite's Validation.  To do this, you would use an `URN` directing to an Evaluation Parameter store.  An example of this might look something like the following:

```python title="Python code"
eval_param_urn = 'urn:great_expectations:validations:my_expectation_suite_1:expect_table_row_count_to_be_between.result.observed_value'
downstream_batch.expect_table_row_count_to_equal(
    value={
        '$PARAMETER': eval_param_urn, # this is the actual parameter we're going to use in the validation
    }
)
```

The core of this is a `$PARAMETER : URN` pair. When Great Expectations encounters a `$PARAMETER` flag during validation, it will replace the `URN` with a value retrieved from an Evaluation Parameter Store or Metrics Store.

If you do not have a previous Expectation Suite's Validation Results to reference, however, you can instead provide Evaluation Parameters with a temporary initial value. For example, the interactive method of creating Expectations is based on Validating Expectations against a previous run of the same Expectation Suite.  Since a previous run has not been performed when Expectations are being created, Evaluation Parameters cannot reference a past Validation and will require a temporary value instead.  This will allow you to test Expectations that are meant to rely on values from previous Validation runs before you have actually used them to Validate data.  

Say you are creating additional expectations for the data that you used in the [Getting Started Tutorial](../tutorials/getting_started/tutorial_overview.md). (You have completed the Getting Started Tutorial, right?)  You want to create an expression that asserts that the row count for each Validation remains the same as the previous `upstream_row_count`, but since there is no previous `upstream_row_count` you need to provide a value that matches what the Expectation you are creating will find.

To do so, you would first edit your existing (or create a new) Expectation Suite using the CLI.  This will open a Jupyter Notebook.  After running the first cell, you will have access to a Validator object named `validator` that you can use to add new Expectations to the Expectation Suite.

The Expectation you will want to add to solve the above problem is the `expect_table_row_count_to_equal` Expectation, and this Expectation uses an evaluation parameter: `upstream_row_count`.  Therefore, when using the validator to add the `expect_table_row_count_to_equal` Expectation you will have to define the parameter in question (`upstream_row_count`) by assigning it to the `$PARAMETER` value in a dictionary.  Then, you would provide the temporary value for that parameter by setting it as the value of the `$PARAMETER.<parameter_in_question>` key in the same dictionary.  Or, in this case, the `$PARAMETER.upstream_row_count`.

For an example of this, see below:

```python title="Python code"
validator.expect_table_row_count_to_equal(
    value={"$PARAMETER": "upstream_row_count", "$PARAMETER.upstream_row_count": 10000},
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```

This will return `{'success': True}`.

An alternative method of defining the temporary value for an Evaluation Parameter is the `set_evaluation_parameter()` method, as shown below:

```python title="Python code"
validator.set_evaluation_parameter("upstream_row_count", 10000)

validator.expect_table_row_count_to_equal(
    value={"$PARAMETER": "upstream_row_count"},
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```

This will also return `{'success': True}`.

Additionally, if the Evaluation Parameter's value is set in this way, you do not need to set it again (or define it alongside the use of the `$PARAMETER` key) for future Expectations that you create with this Validator.

It is also possible for advanced users to create Expectations using Evaluation Parameters by turning off interactive evaluation and adding the Expectation configuration directly to the Expectation Suite.  For more information on this, see our guide on [how to create and edit Expectations based on domain knowledge without inspecting data directly](../guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly.md).

More typically, when validating Expectations, you will provide Evaluation Parameters that are only available at runtime.


<ValidateHeader/>

Evaluation Parameters that are configured as part of a Checkpoint's Expectations will be used without further interaction from you.  Additionally, Evaluation Parameters will be stored by having the `StoreEvaluationParametersAction` subclass of the `ValidationAction` class defined in a Checkpoint configuration's `action_list`.

However, if you wish to provide specific values for Evaluation Parameters when running a Checkpoint (for instance, when you are testing a newly configured Checkpoint) you can do so by either defining the value of the Evaluation Parameter as an environment variable, or by passing the Evaluation Parameter value in as a dictionary assigned to the named parameter `evaluation_parameters` in the Data Context's `run_checkpoint()` method.

For example, say you have a Checkpoint named `my_checkpoint` that is configured to use the Evaluation Parameter `upstream_row_count`.  To associate this Evaluation Parameter with an environment variable, you would edit the Checkpoint's configuration like this:

```yaml title="YAML configuration"
name: my_checkpoint
...
evaluation_parameters:
    upstream_row_count: $MY_ENV_VAR
```

If you would rather pass the value of the Environment Variable `upstream_row_count` in as a dictionary when the Checkpoint is run, you can do so like this:

```python title="Python code"
import great_expectations as ge

test_row_count = 10000

context = ge.get_context()
context.run_checkpoint(`my_checkpoint`, evaluation_parameters={"upstream_row_count":test_row_count})
```


## Features

### Dynamic values

Evaluation Parameters are defined by expressions that are evaluated at run time and replaced with the corresponding values.  These expressions can include such things as:
- Values from previous Validation runs, such as the number of rows in a previous Validation.
- Values modified by basic arithmatic, such as a percentage of rows in a previous Validation.
- Temporal values, such as "now" or "timedelta."
- Complex values, such as lists.

:::note
Although complex values like lists can be used as the value of an Evaluation Parameter, you cannot currently combine complex values with arithmetic expressions.
:::

## API basics

### How to create

An Evaluation Parameter is defined when an Expectation is created.  The Evaluation Parameter at that point will be a reference, either indicating a Metric from the results of a previous Validation, or an expression which will be evaluated prior to a Validation being run on the Expectation Suite.

The Evaluation Parameter references take the form of a dictionary with the `$PARAMETER` key.  The value for this key will be directions to the desired Metric or the Evaluation Parameter's expression.  In either case, it will be evaluated at run time and replaced with the value described by the reference dictionary's value.  If the reference is pointing to a previous Validation's Metrics, it will be in the form of a `$PARAMETER`: `URN` pair, rather than a `$PARAMETER`: `expression` pair.

To store Evaluation Parameters, define a `StoreEvaluationParametersAction` subclass of the `ValidationAction` class in a Checkpoint configuration's `action_list`, and run that Checkpoint.

It is also possible to [dynamically load Evaluation Parameters from a database](../guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.md).

### Evaluation Parameter expressions

Evaluation Parameters can include basic arithmetic and temporal expressions.  For example, we might want to specify that a new table's row count should be between 90 - 110 % of an upstream table's row count (or a count from a previous run). Evaluation parameters support basic arithmetic expressions to accomplish that goal:

```python title="Python code"
validator.set_evaluation_parameter("upstream_row_count", 10000)

validator.expect_table_row_count_to_be_between(
    min_value={"$PARAMETER": "trunc(upstream_row_count * 0.9)"},
    max_value={"$PARAMETER": "trunc(upstream_row_count * 1.1)"}, 
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```
This will return `{'success': True}`.

We can also use the temporal expressions "now" and "timedelta". This example states that we expect values for the "load_date" column to be within the last week.

```python title="Python code"
validator.expect_column_values_to_be_greater_than(
    column="load_date",
    min_value={"$PARAMETER": "now() - timedelta(weeks=1)"}
)
```

Evaluation Parameters are not limited to simple values, for example you could include a list as a parameter value.  Going back to our taxi data, let's say that we know there are only two types of accepted payment: Cash or Credit Card, which are represented by a 1 or a 2 in the `payment_type` column.  We could verify that these are the only values present by using a list, as shown below:

```python title="Python code"
validator.set_evaluation_parameter("runtime_values", [1,2])

validator.expect_column_values_to_be_in_set(
    "payment_type", 
    value_set={"$PARAMETER": "runtime_values"}
)
```

This Expectation will fail (the NYC taxi data allows for four types of payments), and now we are aware that what we thought we knew about the `payment_type` column wasn't accurate, and that now we need to research what those other two payment types are!

:::note
- You cannot currently combine complex values with arithmetic expressions.
:::

