---
title: How to create a Custom Set-Based Column Map Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

**`SetBasedColumnMapExpectations`** are a sub-type of <TechnicalTag tag="expectation" text="ColumnMapExpectaion"/>. They are evaluated for a single column and ask whether each row in that column belongs to the specified set.

Based on the result, they then calculate the percentage of rows that gave a positive answer. If that percentage meets a specified threshold (100% by default), the Expectation considers that data valid.  This threshold is configured via the `mostly` parameter, which can be passed as input to your Custom `SetBasedColumnMapExpectation` as a `float` between 0 and 1.

This guide will walk you through the process of creating a Custom `SetBasedColumnMapExpectation`.

<Prerequisites>

- Read the [overview for creating Custom Expectations](./overview.md).

</Prerequisites>

## Steps

### 1. Choose a name for your Expectation

First, decide on a name for your own Expectation. By convention, all `ColumnMapExpectations`, including `SetBasedColumnMapExpectations`, start with `expect_column_values_`. You can see other naming conventions in the [Expectations section](/docs/contributing/style_guides/code_style#expectations)  of the code Style Guide.

Your Expectation will have two versions of the same name: a `CamelCaseName` and a `snake_case_name`. For example, this tutorial will use:

- `ExpectColumnValuesToBeInSolfegeScaleSet`
- `expect_column_values_to_be_in_solfege_scale_set`

### 2. Copy and rename the template file

By convention, each Expectation is kept in its own python file, named with the snake_case version of the Expectation's name.

You can find the template file for a custom [`SetBasedColumnMapExpectation` here](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/set_based_column_map_expectation_template.py). Download the file, place it in the appropriate directory, and rename it to the appropriate name.

```bash
cp set_based_column_map_expectation_template.py /SOME_DIRECTORY/expect_column_values_to_be_in_solfege_scale_set.py
```

<details>
  <summary>Where should I put my Expectation file?</summary>
  <div>
    <p>
        During development, you don't actually need to put the file anywhere in particular. It's self-contained, and can be executed anywhere as long as <code>great_expectations</code> is installed.
    </p>
    <p>
        But to use your new Expectation alongside the other components of Great Expectations, you'll need to make sure the file is in the right place. The right place depends on what you intend to use it for.
    </p>
    <p>
        <ul>
            <li>If you're building a Custom Expectation for personal use, you'll need to put it in the <code>great_expectations/plugins/expectations</code> folder of your Great Expectations deployment, and import your Custom Expectation from that directory whenever it will be used. When you instantiate the corresponding <code>DataContext</code>, it will automatically make all plugins in the directory available for use.</li>
            <li>If you're building a Custom Expectation to contribute to the open source project, you'll need to put it in the repo for the Great Expectations library itself. Most likely, this will be within a package within <code>contrib/</code>: <code>great_expectations/contrib/SOME_PACKAGE/SOME_PACKAGE/expectations/</code>. To use these Expectations, you'll need to install the package.</li>
        </ul>
    </p>
	<p>
		See our <a href="how_to_use_custom_expectations"> guide on how to use a Custom Expectation</a> for more!
	</p>
  </div>
</details>

### 3. Generate a diagnostic checklist for your Expectation

Once you've copied and renamed the template file, you can execute it as follows.

```bash
python expect_column_values_to_be_in_solfege_scale_set.py
```

The template file is set up so that this will run the Expectation's `print_diagnostic_checklist()` method. This will run a diagnostic script on your new Expectation, and return a checklist of steps to get it to full production readiness.

```
Completeness checklist for ExpectColumnValuesToBeInSomeSet:
  ✔ Has a valid library_metadata object
    Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
    Has basic input validation and type checking
    Has both Statement Renderers: prescriptive and diagnostic
    Has core logic that passes tests for all applicable Execution Engines
    Passes all linting checks
    Has a robust suite of tests, as determined by a code owner
    Has passed a manual review by a code owner for code standards and style guides
```

When in doubt, the next step to implement is the first one that doesn't have a ✔ next to it. This guide covers the first four steps on the checklist.

### 4. Change the Expectation class name and add a docstring

Let's start by updating your Expectation's name and docstring.

Replace the Expectation class name
```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L14
```

with your real Expectation class name, in upper camel case:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L7
```

You can also go ahead and write a new one-line docstring, replacing
```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L15
```

with something like:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L8
```

You'll also need to change the class name at the bottom of the file, by replacing this line:

```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L44
```

with this one:
```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L131
```

Later, you can go back and write a more thorough docstring.

At this point you can re-run your diagnostic checklist. You should see something like this:
```
$ python expect_column_values_to_be_in_solfege_scale_set.py

Completeness checklist for ExpectColumnValuesToBeInSolfegeScaleSet:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
...
```

Congratulations! You're one step closer to implementing a Custom Expectation.

### 5. Add example cases

Next, we're going to search for `examples = []` in your file, and replace it with at least two test examples. These examples serve a dual purpose:

1. They provide test fixtures that Great Expectations can execute automatically via pytest.

2. They help users understand the logic of your Expectation by providing tidy examples of paired input and output. If you contribute your Expectation to open source, these examples will appear in the Gallery.

Your examples will look something like this:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L36-L116
```

Here's a quick overview of how to create test cases to populate `examples`. The overall structure is a list of dictionaries. Each dictionary has two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table has columns named `lowercase_solfege_scale`, `uppercase_solfege_scale`, and `mixed`. All of these columns have 8 rows. (Note: if you define multiple columns, make sure that they have the same number of rows.)
* `tests`: a list of test cases to validate against the data frame defined in the corresponding `data`.
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* `include_in_gallery`: This must be set to `True` if you want this test case to be visible in the Gallery as an example.
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"column": "mixed", "mostly": .1}` in the example above is equivalent to `expect_column_values_to_be_in_solfege_scale_set(column="mixed", mostly=0.1)`
	* `out` is based on the Validation Result returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the Validation Result object - only the ones that are important to test.


If you run your Expectation file again, you won't see any new checkmarks, as the logic for your Custom Expectation hasn't been implemented yet. 
However, you should see that the tests you've written are now being caught and reported in your checklist:

```
$ python expect_column_values_to_be_in_solfege_scale_set.py

Completeness checklist for ExpectColumnValuesToBeInSolfegeScaleSet:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
...
	Has core logic that passes tests for all applicable Execution Engines and SQL dialects
		  Only 0 / 2 tests for pandas are passing
		  Only 0 / 2 tests for spark are passing
		  Only 0 / 2 tests for sqlite are passing
		  Failing: basic_positive_test, basic_negative_test
...
```

:::note
For more information on tests and example cases, <br/>
see our guide on [how to create example cases for a Custom Expectation](../features_custom_expectations/how_to_add_example_cases_for_an_expectation.md).
:::

### 6. Define your set and connect it to your Expectation

This is the stage where you implement the actual business logic for your Expectation.   

In the case of your Custom `SetBasedColumnMapExpectation`, Great Expectations will handle the actual validation of your data against your set. 

To do this, we replace these:

```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L18-L20
```

with something like this:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L10-L33
```

For more detail when rendering your Custom Expectation, you can optionally specify the semantic name of the set you're validating. 

For example:

```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L21
```

becomes:

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L34
```

Great Expectations will use these values to tell your Custom Expectation to apply your specified set as a <TechnicalTag tag="metric" text="Metric"/> to be utilized in validating your data.

This is all that you need to define for now. The `SetBasedColumnMapExpectation` class has built-in logic to handle all the machinery of data validation, including standard parameters like `mostly`, generation of Validation Results, etc.

<details>
  <summary>Other parameters</summary>
  <div>
    <p>
        <b>Expectation Success Keys</b> - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success.
    </p>
    <p>
        <b>Expectation Default Kwarg Values</b> (Optional) - Default values for success keys and the defined domain, among other values.
    </p>
  </div>
</details>

Running your diagnostic checklist at this point should return something like this:
```
$ python expect_column_values_to_be_in_solfege_scale_set.py

Completeness checklist for ExpectColumnValuesToBeInSolfegeScaleSet:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case, and all test cases pass
  ✔ Has core logic and passes tests on at least one Execution Engine
...
```

<div style={{"text-align":"center"}}>  
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>  
Congratulations!<br/>&#127881; You've just built your first Custom Set-Based Column Map Expectation! &#127881;  
</b></p>  
</div>

:::note
If you've already built a [Custom Expectation](./overview.md) of a different type,
you may notice that we didn't explicitly implement a `_validate` method or Metric class here. While we have to explicitly create these for other types of Custom Expectations,
the `SetBasedColumnMapExpectation` class handles Metric creation and result validation implicitly; no extra work needed!
:::

### 7. Contribution (Optional)

This guide will leave you with a Custom Expectation sufficient for [contribution](../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md) back to Great Expectations at an Experimental level.

If you plan to contribute your Expectation to the public open source project, you should update the `library_metadata` object before submitting your [Pull Request](https://github.com/great-expectations/great_expectations/pulls). For example:

```python file=../../../../examples/expectations/set_based_column_map_expectation_template.py#L34-L39
```

would become

```python file=../../../../tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py#L123-L126
```

This is particularly important because ***we*** want to make sure that ***you*** get credit for all your hard work!

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full script used in this page, see it on GitHub:
- [expect_column_values_to_be_in_solfege_scale_set.py](https://github.com/great-expectations/great_expectations/blob/hackathon-docs/tests/integration/docusaurus/expectations/creating_custom_expectations/expect_column_values_to_be_in_solfege_scale_set.py)
:::
