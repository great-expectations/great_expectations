---
title: How to create example cases for a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'

This guide will help you add example cases to document and test the behavior of your Expectation. 

<Prerequisites>

 - Created a [Custom Expectation](../creating_custom_expectations/overview.md)

</Prerequisites>


Example cases in Great Expectations serve a dual purpose:
* They help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate;
* And provide test cases that the Great Expectations testing framework can execute automatically.

If you decide to contribute your Expectation, its entry in the [Expectations Gallery](https://greatexpectations.io/expectations/) will render these examples.

### Steps


* help the users of the Expectation understand its logic by providing examples of input data that the Expectation will evaluate as valid and as invalid. 
* provide test cases that the Great Expectations testing framework can execute automatically


We will explain the structure of these tests using the example provided in one of the templates that implements `expect_column_values_to_equal_three`:

To illustrate, search for `examples = []` in the template file you are modifying for your new custom Expectation.

````python
examples = [{
    "data": {
        "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
    },
    "tests": [
        {
            "title": "positive_test_with_mostly",
            "include_in_gallery": True,
            "exact_match_out": False,
            "in": {"column": "mostly_threes", "mostly": 0.6},
            "out": {
                "success": True,
                "unexpected_index_list": [6, 7],
                "unexpected_list": [2, -1],
            },
        }
    ],
}]
````

The value of `examples` is a list of examples.

Each example is a dictionary with two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table has one column named “mostly_threes” with 10 rows. If you define multiple columns, make sure that they have the same number of rows. If possible, include test data and tests that includes null values (None in the Python test definition).

* `tests`: a list of test cases that use the data defined above as input to validate
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* 'include_in_gallery': set it to True if you want this test case to be visible in the gallery as an example (true for most test cases).
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"column": "mostly_threes", "mostly": 0.6}` in the example above is equivalent to `expect_column_values_to_equal_three(column="mostly_threes, mostly=0.6)`
	* `out` is based on the Validation Result returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the result object - only the ones that are important to test.

Uncomment that code snippet and replace with your examples.

Run `run_diagnostics` again. The newly added examples will appear in the output. They are not executed as tests yet, because most of the code in the Expectation is still commented out.

:::note

When you define data in your examples, we will mostly guess the type of the columns. Sometimes you need to specify the precise type of the columns for each backend. Then you use schema attribute (on the same level as data and tests in the dictionary):

````console
"schemas": {
  "spark": {
    "mostly_threes": "IntegerType",
  },
  "sqlite": {
    "mostly_threes": "INTEGER",
  },
````
:::
