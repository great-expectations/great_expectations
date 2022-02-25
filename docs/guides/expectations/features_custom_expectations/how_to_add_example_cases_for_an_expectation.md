---
title: How to create example cases for a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'

This guide will help you add example cases to document and test the behavior of your [Expectation](../../../reference/expectations/expectations.md). 

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>


Example cases in Great Expectations serve a dual purpose:
* First, they help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate.
* Second, they provide test cases that the Great Expectations testing framework can execute automatically.

If you decide to contribute your Expectation, its entry in the [Expectations Gallery](https://greatexpectations.io/expectations/) will render these examples.

We will explain the structure of these tests using the Custom Expectation implemented in our guide on [how to create Custom Column Aggregate Expectations](../creating_custom_expectations/how_to_create_custom_column_aggregate_expectations.md).

## Steps

### 1. Decide which tests you want to implement

Expectations can have a robust variety of possible applications. We want to create tests that demonstrate (and verify) the capabilities and limitations of our Custom Expectation.

<details>
  <summary>What kind of tests can I create?</summary>
These tests can include examples intended to pass, fail, or error out, and expected results can be as open-ended as <inlineCode>{`{"success": False}`}</inlineCode>, or as granular as:
<code>{`{
  "success": True,
  "expectation_config": {
      "expectation_type": "expect_column_value_z_scores_to_be_less_than",
      "kwargs": {
          "column": "a",
          "mostly": 0.9,
          "threshold": 4,
          "double_sided": True,
      },
      "meta": {},
  },
  "result": {
      "element_count": 6,
      "unexpected_count": 0,
      "unexpected_percent": 0.0,
      "partial_unexpected_list": [],
      "missing_count": 0,
      "missing_percent": 0.0,
      "unexpected_percent_total": 0.0,
      "unexpected_percent_nonmissing": 0.0,
  },
  "exception_info": {
      "raised_exception": False,
      "exception_traceback": None,
      "exception_message": None,
  }
}`}
</code>
</details>

At a minimum, we want to create tests that show what our Custom Expectation will and will *not* do. 
These basic positive and negative example cases are the minimum amount of test coverage required for a Custom Expectation to be accepted into the Great Expectations codebase at an [Experimental level](../../../contributing/contributing_maturity.md#contributing-expectations).
To begin with, let's implement those two basic tests: one positive example case, and one negative example case. 

### 2. Defining our data

Search for `examples = []` in the template file you are modifying for your new Custom Expectation. 
We're going to populate `examples` with a list of example cases.

<details>
  <summary>What is an example case?</summary>
Each example is a dictionary with two keys:
<ul>
<li> <code>data</code>: defines the input data of the example as a table/dataframe. </li>
<li> <code>tests</code>: a list of test cases that use the data defined above as input to validate against. </li>
    <ul>
        <li> <code>title</code>: a descriptive name for the test case. Make sure to have no spaces. </li>
        <li> <code>include_in_gallery</code>: set it to True if you want this test case to be visible in the gallery as an example (true for most test cases). </li>
        <li> <code>in</code>: contains exactly the parameters that you want to pass in to the Expectation. <inlineCode>{`"in": {"column": "x", "min_value": 4}`}</inlineCode> would be equivalent to <code>expect_column_max_to_be_between_custom(column="x", min_value=4)</code> </li>
        <li> <code>out</code>: indicates the results the test requires from the <code>ValidationResult</code> needed to pass. </li>
        <li> <code>exact_match_out</code>: if you set <code>exact_match_out=False</code>, then you don’t need to include all the elements of the result object - only the ones that are important to test, such as <inlineCode>{`{"success": True}`}</inlineCode>. </li>
    </ul>
</ul>
</details>

In our example, `data` will have two columns, "x" and "y", each with five rows. 
If you define multiple columns, make sure that they have the same number of rows. 
When possible, include test data and tests that includes null values (`None` in the Python test definition).

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L88
```

When you define data in your examples, we will mostly guess the type of the columns. 
Sometimes you need to specify the precise type of the columns for each backend. Then you use the `schemas` attribute (on the same level as `data` and `tests` in the dictionary):

```console
"schemas": {
  "spark": {
    "x": "IntegerType",
  },
  "sqlite": {
    "x": "INTEGER",
  },
```

:::info
While Pandas is fairly flexible in typing, Spark and many SQL dialects are much more strict. 

You may find you wish to use data that is incompatible with a given backend, or write different individual tests for different backends. 
To do this, you can use the `only_for` attribute, which accepts `pandas`, `spark`, `sqlite` or a SQL dialect:

```console
"only_for": "spark"
```

Passing this attribute on the same level as `data`, `tests`, and `schemas` 
will tell Great Expectations to only instantiate the data specified in that example for the given backend, ensuring you don't encounter any backend-related errors relating to data before your Custom Expectation can even be tested:


Passing this attribute within a test (at the same level as `title`, `in`, `out`, etc.) will execute that individual test only for that specified backend.
:::

### 3. Defining our tests

In our example, `tests` will be a list containing dictionaries defining each test. 

You will need to:
1. Title your tests (`title`)
2. Define the input for your tests (`in`)
3. Decide how precisely you want to test the output of your tests (`exact_match_out`)
4. Define the expected output for your tests (`out`)

If you are interested in contributing your Custom Expectation back to Great Expectations, you will also need to decide if you want these tests publically displayed to demonstrate the functionality of your Custom Expectation (`include_in_gallery`).

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py#L86-L130
```

:::note
You may have noticed that specifying `test_backends` isn't required for successfully testing your Custom Expectation.

If not specified, Great Expectations will attempt to determine the implemented backends automatically, but wll only run SQLAlchemy tests against sqlite.
:::

<details>
  <summary>Can I test for errors?</summary>
Yes! If you would like to define an example case illustrating when your Custom Expectation should throw an error, 
you can pass an empty <code>out</code> key, and include an <code>error</code> key defining a <code>traceback_substring</code>. 
<br/><br/>
For example:
<br/><br/>
<code>{`"out": {},
"error": {
    "traceback_substring" : "TypeError: Column values, min_value, and max_value must either be None or of the same type."
}`}
</code>
</details>

### 4. Verifying our tests

If you now run your file, `print_diagnostic_checklist()` will attempt to execute these example cases.

If the tests are correctly defined, and the rest of the logic in your Custom Expectation is already complete,
you will see the following in your Diagnostic Checklist:

```console
✔ Has at least one positive and negative example case, and all test cases pass
```

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've successfully created example cases & tests for a Custom Expectation! &#127881;
</b></p>
</div>

### 5. Contribution (Optional)

This guide will leave you with test coverage sufficient for [contribution](../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md) back to Great Expectations at an Experimental level.  

If you're interested in having your contribution accepted at a Beta level, these tests will need to pass for all supported backends (Pandas, Spark, & SQLAlchemy).

For full acceptance into the Great Expectations codebase at a Production level, we require a more robust test suite. 
If you believe your Custom Expectation is otherwise ready for contribution at a Production level, please submit a [Pull Request](https://github.com/great-expectations/great_expectations/pulls), and we will work with you to ensure adequate testing.

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full script used in this page, see it on GitHub:
- [expect_column_max_to_be_between_custom.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_max_to_be_between_custom.py)
:::