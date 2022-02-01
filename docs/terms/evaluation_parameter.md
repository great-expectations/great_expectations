---
id: evaluation_parameter
title: Evaluation Parameter
hoverText: A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.
---

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

An Evaluation Parameter is a connector to store and retrieve information about parameters used during <TechnicalTag relative="../" tag="validation" text="Validation" /> of an <TechnicalTag relative="../" tag="expectation" text="Expectation" /> which reference simple expressions or previously generated metrics.

### Features and promises

You can use Evaluation Parameters to configure Expectations to use dynamic values, such as a value from a previous step in a pipeline or a date relative to today.  Evaluation Parameters can be simple expressions such as math expressions or the current date, or reference Metrics generated from a previous Validation run.  During interactive development, you can even provide a temporary value that should be used during the initial evaluation of the Expectation.

### Relationship to other objects

Evaluation Parameters are used in Expectations when Validating data.  <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> use <TechnicalTag relative="../" tag="validation_action" text="Action" /> to store Evaluation Parameters in the <TechnicalTag relative="../" tag="evaluation_parameter_store" text="Evaluation Parameter Store" />.

## Use cases

<CreateHeader/>

When creating expectations through the interactive method, Evaluation Parameters can be provided a temporary initial value.  This will allow you to test Expectations that are meant to rely on values from previous Validation runs before you have actually used them to Validate data.  Say you have created a Pandas dataframe called `my_df` that references a CSV that contains ten rows of data.  (To do so in the interactive process for creating Expectations, you would use the `great_expectations.read_csv()` method).  You want to create an expression that asserts that the row count for each Validation remains the same as the previous `upstream_row_count`, but since there is no previous `upstream_row_count` you need to provide a value that matches what the Expectation you are creating will find.

To do so, you would define the parameter in question (`upstream_row_count`) by assigning it to the `$PARAMETER` value in a dictionary.  Then, you would provide the temporary value for that parameter by setting it as the value of the `$PARAMETER.<parameter_in_question>` key in the same dictionary.  Or, in this case, the `$PARAMETER.upstream_row_count`.

For an example of this, see below:

```python title="Python code"
my_df.expect_table_row_count_to_equal(
    value={"$PARAMETER": "upstream_row_count", "$PARAMETER.upstream_row_count": 10},
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```

This will return `{'success': True}`.

An alternative method of defining the temporary value for an Evaluation Parameter is the `set_evaluation_parameter()` method, as shown below:

```python title="Python code"
my_df.set_evaluation_parameter("upstream_row_count", 10)
```

If the Evaluation Parameter's value is set in this way, you do not need to set it again (or define it alongside the use of the `$PARAMETER` key) for future Expectations.

More typically, when validating Expectations, you can provide Evaluation Parameters that are only available at runtime.

```python title="Python code"
my_df.validate(
    expectation_suite=my_dag_step_config, 
    evaluation_parameters={"upstream_row_count": upstream_row_count}
)
```

<ValidateHeader/>

Evaluation Parameters that are configured as part of a Checkpoint's Expectations will be used without further interaction from you.  Additionally, Evaluation Parameters will be stored by having the `StoreEvaluationParametersAction` subclass of the `ValidationAction` class defined in a Checkpoint configuration's `action_list`.



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

An Evaluation Parameter is "created" when a parameter is referenced in the results of another Expectation Suite's Validation.  To store them, define a `StoreEvaluationParametersAction` subclass of the `ValidationAction` class in a Checkpoint configuration's `action_list`, and run that Checkpoint.

Evaluation Parameters are *referenced* by making a dictionary with the `$PARAMETER` key.  The value for this key will be the Evaluation Parameter's expression, which will be evaluated at run time and replaced with the value described by said expression.

It is also possible to [dynamically load Evaluation Parameters from a database](../guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database.md).

### Evaluation Parameter expressions

Evaluation Parameters can include basic arithmetic and temporal expressions.  For example, we might want to specify that a new table's row count should be between 90 - 110 % of an upstream table's row count (or a count from a previous run). Evaluation parameters support basic arithmetic expressions to accomplish that goal:

```python title="Python code"
my_df.expect_table_row_count_to_be_between(
    min_value={"$PARAMETER": "trunc(upstream_row_count * 0.9)"},
    max_value={"$PARAMETER": "trunc(upstream_row_count * 1.1)"}, 
    result_format={'result_format': 'BOOLEAN_ONLY'}
)
```
This will return `{'success': True}`.

We can also use the temporal expressions "now" and "timedelta". This example states that we expect values for the "load_date" column to be within the last week.

```python title="Python code"
my_df.expect_column_values_to_be_greater_than(
    column="load_date",
    min_value={"$PARAMETER": "now() - timedelta(weeks=1)"}
)
```

Evaluation Parameters are not limited to simple values, for example you could include a list as a parameter value:

```python title="Python code"
my_df.expect_column_values_to_be_in_set(
    "my_column", 
    value_set={"$PARAMETER": "runtime_values"}
)
my_df.validate(
    evaluation_parameters={"runtime_values": [1, 2, 3]}
)
```

:::note
- You cannot currently combine complex values with arithmetic expressions.
:::

