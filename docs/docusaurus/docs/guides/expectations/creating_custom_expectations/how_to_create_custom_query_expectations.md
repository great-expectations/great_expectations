---
title: Create a Custom Query Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

**`QueryExpectations`** are a type of <TechnicalTag tag="expectation" text="Expectation"/>, enabled for SQL and Spark, that enable a higher-complexity type of workflow
when compared with core Expectation classes such as **`ColumnAggregate`**, **`ColumnMap`**, and **`Table`**.

`QueryExpectations` allow you to set Expectations against the results of your own custom queries, and make intermediate queries to your database.
While this approach can result in extra roundtrips to your database, it can also unlock advanced functionality for your <TechnicalTag tag="custom_expectation" text="Custom Expectations"/>.

They are evaluated for the results of a query, and answer a semantic question about your data returned by that query. For example, `expect_queried_table_row_count_to_equal` answers how many rows are returned from your table by your query.

This guide will walk you through the process of creating your own custom `QueryExpectation`.

## Prerequisites

<Prerequisites>

- Completion of the [overview for creating Custom Expectations](./overview.md).

</Prerequisites>

## Steps

### 1. Choose a name for your Expectation

First, decide on a name for your own Expectation. By convention, `QueryExpectations` always start with `expect_queried_`.

All `QueryExpectations` support the parameterization of your <TechnicalTag tag="batch" text="Active Batch"/>;
some `QueryExpectations` also support the parameterization of a `Column`. This tutorial will detail both approaches.

<Tabs
  groupId="expectation_type"
  defaultValue='query.table'
  values={[
  {label: 'Batch Parameterization', value:'query.table'},
  {label: 'Batch & Column Parameterization', value:'query.column'},
  ]}>

<TabItem value="query.table">

Your Expectation will have two versions of the same name: a `CamelCaseName` and a `snake_case_name`. For example, this tutorial will use:

- `ExpectQueriedTableRowCountToBe`
- `expect_queried_table_row_count_to_be`

:::info
For more on Expectation naming conventions, see the [Expectations section](../../../contributing/style_guides/code_style.md#expectations) of the Code Style Guide.
:::

### 2. Copy and rename the template file

By convention, each Expectation is kept in its own python file, named with the snake_case version of the Expectation's name.

You can find the template file for a custom [`QueryExpectation` here](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/query_expectation_template.py).
Download the file, place it in the appropriate directory, and rename it to the appropriate name.

```bash
cp query_expectation_template.py /SOME_DIRECTORY/expect_queried_table_row_count_to_equal.py
```

<details>
  <summary>Where should I put my Expectation file?</summary>
  <div>
  <br/>
    <p>
        Within a development environment, you don't need to put the file in a specific folder. The Expectation itself should be self-contained, and can be executed anywhere as long as <code>great_expectations</code> is installed, which is sufficient for development and testing. However, to use your new Expectation alongside the other components of Great Expectations (as one would for production purposes), you'll need to make sure the file is in the right place. Where the right place is will depend on what you intend to use it for.
    </p>
    <p>
        <ul>
            <li>If you're building a <TechnicalTag tag="custom_expectation" text="Custom Expectation" /> for personal use, you'll need to put it in the <code>great_expectations/plugins/expectations</code> folder of your Great Expectations deployment, and import your Custom Expectation from that directory whenever it will be used. When you instantiate the corresponding <code>DataContext</code>, it will automatically make all <TechnicalTag tag="plugin" text="Plugins" /> in the directory available for use.</li>
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
python expect_queried_table_row_count_to_be.py
```

The template file is set up so that this will run the Expectation's `print_diagnostic_checklist()` method.
This will run a diagnostic script on your new Expectation, and return a checklist of steps to get it to full production readiness.

```
Completeness checklist for ExpectQueryToMatchSomeCriteria:
  ✔ Has a valid library_metadata object
    Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
    Has basic input validation and type checking
    Has both Statement Renderers: prescriptive and diagnostic
    Has core logic that passes tests for all applicable Execution Engines and SQL dialects
    Has a robust suite of tests, as determined by a code owner
    Has passed a manual review by a code owner for code standards and style guides
```

When in doubt, the next step to implement is the first one that doesn't have a ✔ next to it.
This guide will walk you through the first five steps, the minimum for a functioning Custom Expectation and all that is required for [contribution back to open source](../../../contributing/contributing_maturity.md#contributing-expectations) at an Experimental level.

### 4. Change the Expectation class name and add a docstring

Now we're going to begin laying the groundwork for the functionality of your Custom Expectation.

Let's start by updating your Expectation's name and docstring.

Replace the Expectation class name
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py ExpectQueryToMatchSomeCriteria class_def"
```

with your real Expectation class name, in upper camel case:
```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py ExpectQueriedTableRowCountToBe class_def"
```

You can also go ahead and write a new one-line docstring, replacing
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py docstring"
```

with something like:
```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py docstring"
```

You'll also need to change the class name at the bottom of the file, by replacing this line:
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py print_diagnostic_checklist"
```

with this one:
```python name="expect_queried_table_row_count_to_be.py print_diagnostic_checklist"
```

Later, you can go back and write a more thorough docstring. See [Expectation Docstring Formatting](https://github.com/great-expectations/great_expectations/blob/develop/docs/expectation_gallery/3-expectation-docstring-formatting.md).

At this point you can re-run your diagnostic checklist. You should see something like this:
```
$ python expect_queried_table_row_count_to_be.py

Completeness checklist for ExpectQueriedTableRowCountToBe:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
...
```

Congratulations! You're one step closer to implementing a Custom Query Expectation.

<details>
<summary>What about my Metric?</summary>
If you've built a Custom Expectation before, you may have noticed that the template doesn't contain a <TechnicalTag tag="metric" text="Metric"/> class.

While you are still able to create a Custom Metric for your Custom Expectation if needed, the nature of `QueryExpectations`
allows us to provide a small number of generic `query.*` Metrics capable of supporting many use-cases.
</details>

### 5. Add example cases

Next, we're going to search for `examples = []` in your file, and replace it with at least two test examples. These examples serve a dual purpose:

1. They provide test fixtures that Great Expectations can execute automatically via `pytest`.
2. They help users understand the logic of your Expectation by providing tidy examples of paired input and output. If you contribute your Expectation to open source, these examples will appear in the Gallery.

Your examples will look something like this:

```python name="expect_queried_table_row_count_to_be.py examples"
```

Here's a quick overview of how to create test cases to populate `examples`. The overall structure is a list of dictionaries. Each dictionary has two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table `test` has one column named `col1` and a second column named `col2`. Both columns have 5 rows. (Note: if you define multiple columns, make sure that they have the same number of rows.)
* `tests`: a list of test cases to <TechnicalTag tag="validation" text="Validate" /> against the data frame defined in the corresponding `data`.
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* `include_in_gallery`: This must be set to `True` if you want this test case to be visible in the Gallery as an example.
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"value": 5}` in the example above is equivalent to `expect_queried_table_row_count_to_be(value=5)`
	* `out` is based on the <TechnicalTag tag="validation_result" text="Validation Result" /> returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the Validation Result object - only the ones that are important to test.

<details>
<summary><code>only_for</code>?</summary>
<code>only_for</code> is an optional key you can pass to offer more granular control over which backends and SQL dialects your tests are run against.
</details>

If you run your Expectation file again, you won't see any new checkmarks, as the logic for your Custom Expectation hasn't been implemented yet.
However, you should see that the tests you've written are now being caught and reported in your checklist:

```
$ python expect_queried_table_row_count_to_be.py

Completeness checklist for ExpectQueriedTableRowCountToBe:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
...
	Has core logic that passes tests for all applicable Execution Engines and SQL dialects
		  Only 0 / 2 tests for sqlite are passing
		    Failing: basic_positive_test, basic_negative_test
...
```

:::note
For more information on tests and example cases, <br/>
see our guide on [how to create example cases for a Custom Expectation](../features_custom_expectations/how_to_add_example_cases_for_an_expectation.md).
:::

### 6. Implement a Query & Connect a Metric to your Expectation

The query is the core of a `QueryExpectation`; this query is what defines the scope of your expectations for your data.

To implement your query, replace the `query` attribute of your Custom Expectation.

This:

```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py sql_query"
```

Becomes something like this:

```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py query"
```

:::warning
As noted above, `QueryExpectations` support parameterization of your <TechnicalTag tag="batch" text="Active Batch"/>.

We *strongly* recommend making use of that parameterization as above, by querying against `{active_batch}`.
Not doing so could result in your Custom Expectation unintentionally being run against the wrong data!
:::

Metrics for `QueryExpectations` are a thin wrapper, allowing you to execute that parameterized SQL query with Great Expectations. The results of that query are then validated to judge whether your data meets ***your*** expectations.

Great Expectations provides a small number of simple, ready-to-use `query.*` Metrics that can plug into your Custom Expectation, or serve as a basis for your own custom Metrics.


:::note
Query Metric functions have the `@metric_value` decorator, with the appropriate `engine`.

The `@metric_value` decorator allows us to explicitly structure queries and directly access our compute domain.
While this can result in extra roundtrips to your database in some situations, it allows for advanced functionality and customization of your Custom Expectations.

See an example of a `query.table` metric [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/metrics/query_metrics/query_table.py).
:::

To connect this Metric to our Custom Expectation, we'll need to include the `metric_name` for this Metric in our `metric_dependencies`.

This tuple:

```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py metric_dependencies"
```

Becomes:

```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py metric_dependencies"
```

<details>
  <summary>Other parameters</summary>
  <div>
    <p>
        <b>Expectation Success Keys</b> - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success. <code>QueryExpectations</code> must include <code>"query"</code> in <code>success_keys</code>.
    </p>
    <p>
        <b>Expectation Default Kwarg Values</b> (Optional) - Default values for success keys and the defined domain, among other values.
    </p>
    <p>
        <b>Metric Value Keys</b> (Optional) - Contains any additional arguments passed as parameters to compute the Metric.
    </p>
  </div>
</details>

### 7. Validate

In this step, we simply need to validate that the results of our Metrics meet our Expectation.

The validate method is implemented as `_validate(...)`:

```python name="expect_queried_table_row_count_to_be.py _validate function signature"
```

This method takes a dictionary named `metrics`, which contains all Metrics requested by your Metric dependencies,
and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not.

To do so, we'll be accessing our success keys, as well as the result of our previously-calculated Metrics.
For example, here is the definition of a `_validate(...)` method to validate the results of our `query.table` Metric against our success keys:

```python name="expect_queried_table_row_count_to_be.py _validate function"
```

Running your diagnostic checklist at this point should return something like this:
```
$ python expect_queried_table_row_count_to_be.py

Completeness checklist for ExpectQueriedTableRowCountToBe:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case, and all test cases pass
  ✔ Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
...
```

### 8. Linting

Finally, we need to lint our now-functioning Custom Expectation. Our CI system will test your code using `black`, and `ruff`.

If you've [set up your dev environment](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md), these libraries will already be available to you, and can be invoked from your command line to automatically lint your code:

```console
black <PATH/TO/YOUR/EXPECTATION.py>
ruff <PATH/TO/YOUR/EXPECTATION.py> --fix
```

:::info
If desired, you can automate this to happen at commit time. See our [guidance on linting](../../../contributing/style_guides/code_style.md#linting) for more on this process.
:::

Once this is done, running your diagnostic checklist should now reflect your Custom Expectation as meeting our linting requirements:

```
$ python expect_queried_table_row_count_to_be.py

Completeness checklist for ExpectQueriedTableRowCountToBe:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case, and all test cases pass
  ✔ Has core logic and passes tests on at least one Execution Engine
  ✔ Passes all linting checks
...
```

</TabItem>

<TabItem value="query.column">

Your Expectation will have two versions of the same name: a `CamelCaseName` and a `snake_case_name`. For example, this tutorial will use:

- `ExpectQueriedColumnValueFrequencyToMeetThreshold`
- `expect_queried_column_value_frequency_to_meet_threshold`

:::info
For more on Expectation naming conventions, see the [Expectations section](../../../contributing/style_guides/code_style.md#expectations) of the Code Style Guide.
:::

### 2. Copy and rename the template file

By convention, each Expectation is kept in its own python file, named with the snake_case version of the Expectation's name.

You can find the template file for a custom [`QueryExpectation` here](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/query_expectation_template.py).
Download the file, place it in the appropriate directory, and rename it to the appropriate name.

```bash
cp query_expectation_template.py /SOME_DIRECTORY/expect_queried_column_value_frequency_to_meet_threshold.py
```

<details>
  <summary>Where should I put my Expectation file?</summary>
  <div>
  <br/>
    <p>
        Within a development environment, you don't need to put the file in a specific folder. The Expectation itself should be self-contained, and can be executed anywhere as long as <code>great_expectations</code> is installed, which is sufficient for development and testing. However, to use your new Expectation alongside the other components of Great Expectations (as one would for production purposes), you'll need to make sure the file is in the right place. Where the right place is will depend on what you intend to use it for.
    </p>
    <p>
        <ul>
            <li>If you're building a <TechnicalTag tag="custom_expectation" text="Custom Expectation" /> for personal use, you'll need to put it in the <code>great_expectations/plugins/expectations</code> folder of your Great Expectations deployment, and import your Custom Expectation from that directory whenever it will be used. When you instantiate the corresponding <code>DataContext</code>, it will automatically make all <TechnicalTag tag="plugin" text="Plugins" /> in the directory available for use.</li>
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
python expect_queried_column_value_frequency_to_meet_threshold.py
```

The template file is set up so that this will run the Expectation's `print_diagnostic_checklist()` method.
This will run a diagnostic script on your new Expectation, and return a checklist of steps to get it to full production readiness.

```
Completeness checklist for ExpectQueriedColumnValueFrequencyToMeetThreshold:
  ✔ Has a valid library_metadata object
    Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
    Has basic input validation and type checking
    Has both Statement Renderers: prescriptive and diagnostic
    Has core logic that passes tests for all applicable Execution Engines and SQL dialects
    Has a robust suite of tests, as determined by a code owner
    Has passed a manual review by a code owner for code standards and style guides
```

When in doubt, the next step to implement is the first one that doesn't have a ✔ next to it.
This guide will walk you through the first five steps, the minimum for a functioning Custom Expectation and all that is required for [contribution back to open source](../../../contributing/contributing_maturity.md#contributing-expectations) at an Experimental level.

### 4. Change the Expectation class name and add a docstring

Now we're going to begin laying the groundwork for the functionality of your Custom Expectation.

Let's start by updating your Expectation's name and docstring.

Replace the Expectation class name
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py ExpectQueryToMatchSomeCriteria class_def"
```

with your real Expectation class name, in upper camel case:
```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py ExpectQueriedColumnValueFrequencyToMeetThreshold class_def"
```

You can also go ahead and write a new one-line docstring, replacing
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py docstring"
```

with something like:
```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py docstring"
```

You'll also need to change the class name at the bottom of the file, by replacing this line:
```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py print_diagnostic_checklist"
```

with this one:
```python name="expect_queried_column_value_frequency_to_meet_threshold.py print_diagnostic_checklist()"
```

Later, you can go back and write a more thorough docstring.

At this point you can re-run your diagnostic checklist. You should see something like this:
```
$ python expect_queried_column_value_frequency_to_meet_threshold.py

Completeness checklist for ExpectQueriedColumnValueFrequencyToMeetThreshold:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
    Has at least one positive and negative example case, and all test cases pass
    Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
...
```

Congratulations! You're one step closer to implementing a Custom Query Expectation.

<details>
<summary>What about my Metric?</summary>
If you've built a Custom Expectation before, you may have noticed that the template doesn't contain a <TechnicalTag tag="metric" text="Metric"/> class.

While you are still able to create a Custom Metric for your Custom Expectation if needed, the nature of `QueryExpectations`
allows us to provide a small number of generic `query.*` Metrics capable of supporting many use-cases.
</details>

### 5. Add example cases

Next, we're going to search for `examples = []` in your file, and replace it with at least two test examples. These examples serve a dual purpose:

1. They provide test fixtures that Great Expectations can execute automatically via `pytest`.
2. They help users understand the logic of your Expectation by providing tidy examples of paired input and output. If you contribute your Expectation to open source, these examples will appear in the Gallery.

Your examples will look something like this:

```python name="expect_queried_column_value_frequency_to_meet_threshold.py examples"
```

Here's a quick overview of how to create test cases to populate `examples`. The overall structure is a list of dictionaries. Each dictionary has two keys:

* `data`: defines the input data of the example as a table/data frame. In this example the table `test` has one column named `col1` and a second column named `col2`. Both columns have 5 rows. (Note: if you define multiple columns, make sure that they have the same number of rows.)
* `tests`: a list of test cases to <TechnicalTag tag="validation" text="Validate" /> against the data frame defined in the corresponding `data`.
	* `title` should be a descriptive name for the test case. Make sure to have no spaces.
	* `include_in_gallery`: This must be set to `True` if you want this test case to be visible in the Gallery as an example.
	* `in` contains exactly the parameters that you want to pass in to the Expectation. `"in": {"column": "col2", "value": "a", "threshold": 0.6,}` in the example above is equivalent to `expect_queried_column_value_frequency_to_meet_threshold(column="col2", value="a", threshold=0.6)`
	* `out` is based on the <TechnicalTag tag="validation_result" text="Validation Result" /> returned when executing the Expectation.
	* `exact_match_out`: if you set `exact_match_out=False`, then you don’t need to include all the elements of the Validation Result object - only the ones that are important to test.

<details>
<summary><code>only_for</code>?</summary>
<code>only_for</code> is an optional key you can pass to offer more granular control over which backends and SQL dialects your tests are run against.
</details>

If you run your Expectation file again, you won't see any new checkmarks, as the logic for your Custom Expectation hasn't been implemented yet.
However, you should see that the tests you've written are now being caught and reported in your checklist:

```
$ python expect_queried_column_value_frequency_to_meet_threshold.py

Completeness checklist for ExpectQueriedColumnValueFrequencyToMeetThreshold:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
...
	Has core logic that passes tests for all applicable Execution Engines and SQL dialects
		  Only 0 / 2 tests for sqlite are passing
		    Failing: basic_positive_test, basic_negative_test
...
```

:::note
For more information on tests and example cases, <br/>
see our guide on [how to create example cases for a Custom Expectation](../features_custom_expectations/how_to_add_example_cases_for_an_expectation.md).
:::

### 6. Implement a Query & Connect a Metric to your Expectation

The query is the core of a `QueryExpectation`; this query is what defines the scope of your expectations for your data.

To implement your query, replace the `query` attribute of your Custom Expectation.

This:

```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py sql_query"
```

Becomes something like this:

```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py query"
```

:::warning
As noted above, `QueryExpectations` support parameterization of your <TechnicalTag tag="batch" text="Active Batch"/>, and can support parameterization of a column name.

While parameterizing a column name with `{col}` is optional and supports flexibility in your Custom Expectations,
we *strongly* recommend making use of batch parameterization, by querying against `{active_batch}`.
Not doing so could result in your Custom Expectation unintentionally being run against the wrong data!
:::

Metrics for `QueryExpectations` are a thin wrapper, allowing you to execute that parameterized SQL query with Great Expectations. The results of that query are then validated to judge whether your data meets ***your*** expectations.

Great Expectations provides a small number of simple, ready-to-use `query.*` Metrics that can plug into your Custom Expectation, or serve as a basis for your own custom Metrics.


:::note
Query Metric functions have the `@metric_value` decorator, with the appropriate `engine`.

The `@metric_value` decorator allows us to explicitly structure queries and directly access our compute domain.
While this can result in extra roundtrips to your database in some situations, it allows for advanced functionality and customization of your Custom Expectations.

See an example of a `query.column` metric [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/expectations/metrics/query_metrics/query_column.py).
:::

To connect this Metric to our Custom Expectation, we'll need to include the `metric_name` for this Metric in our `metric_dependencies`.

In this case, we'll be using the `query.column` Metric, allowing us to parameterize both our <TechnicalTag tag="batch" text="Active Batch"/> and a column name.

This tuple:

```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py metric_dependencies"
```

Becomes:

```python name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_column_value_frequency_to_meet_threshold.py metric_dependencies"
```

<details>
  <summary>Other parameters</summary>
  <div>
    <p>
        <b>Expectation Success Keys</b> - A tuple consisting of values that must / could be provided by the user and defines how the Expectation evaluates success. <code>QueryExpectations</code> must include <code>"query"</code> in <code>success_keys</code>.
    </p>
    <p>
        <b>Expectation Default Kwarg Values</b> (Optional) - Default values for success keys and the defined domain, among other values.
    </p>
    <p>
        <b>Metric Value Keys</b> (Optional) - Contains any additional arguments passed as parameters to compute the Metric.
    </p>
  </div>
</details>

### 7. Validate

In this step, we simply need to validate that the results of our Metrics meet our Expectation.

The validate method is implemented as `_validate(...)`:

```python name="expect_queried_column_value_frequency_to_meet_threshold.py _validate function signature"
```

This method takes a dictionary named `metrics`, which contains all Metrics requested by your Metric dependencies,
and performs a simple validation against your success keys (i.e. important thresholds) in order to return a dictionary indicating whether the Expectation has evaluated successfully or not.

To do so, we'll be accessing our success keys, as well as the result of our previously-calculated Metrics.
For example, here is the definition of a `_validate(...)` method to validate the results of our `query.column` Metric against our success keys:

```python name="expect_queried_column_value_frequency_to_meet_threshold.py _validate function"
```

Running your diagnostic checklist at this point should return something like this:
```
$ python expect_queried_column_value_frequency_to_meet_threshold.py

Completeness checklist for ExpectQueriedColumnValueFrequencyToMeetThreshold:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case, and all test cases pass
  ✔ Has core logic and passes tests on at least one Execution Engine
    Passes all linting checks
...
```

### 8. Linting

Finally, we need to lint our now-functioning Custom Expectation. Our CI system will test your code using `black`, and `ruff`.

If you've [set up your dev environment](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) as recommended in the Prerequisites, these libraries will already be available to you, and can be invoked from your command line to automatically lint your code:

```console
black <PATH/TO/YOUR/EXPECTATION.py>
ruff <PATH/TO/YOUR/EXPECTATION.py> --fix
```

:::info
If desired, you can automate this to happen at commit time. See our [guidance on linting](../../../contributing/style_guides/code_style.md#linting) for more on this process.
:::

Once this is done, running your diagnostic checklist should now reflect your Custom Expectation as meeting our linting requirements:

```
$ python expect_queried_column_value_frequency_to_meet_threshold.py

Completeness checklist for ExpectQueriedColumnValueFrequencyToMeetThreshold:
  ✔ Has a valid library_metadata object
  ✔ Has a docstring, including a one-line short description
  ✔ Has at least one positive and negative example case, and all test cases pass
  ✔ Has core logic and passes tests on at least one Execution Engine
  ✔ Passes all linting checks
...
```

</TabItem>

</Tabs>

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just built your first Custom QueryExpectation! &#127881;
</b></p>
</div>

### 9. Contribution (Optional)

This guide will leave you with a Custom Expectation sufficient for [contribution](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md) to Great Expectations at an Experimental level.

If you plan to contribute your Expectation to the public open source project, you should update the `library_metadata` object before submitting your [Pull Request](https://github.com/great-expectations/great_expectations/pulls). For example:

```python name="tests/integration/docusaurus/expectations/examples/query_expectation_template.py library_metadata"
```

would become

```python name="expect_queried_column_value_frequency_to_meet_threshold.py library_metadata"
```

This is particularly important because ***we*** want to make sure that ***you*** get credit for all your hard work!

:::note
For more information on our code standards and contribution, see our guide on [Levels of Maturity](../../../contributing/contributing_maturity.md#contributing-expectations) for Expectations.

To view the full scripts used in this page, see them on GitHub:
- [expect_queried_table_row_count_to_be.py](https://github.com/great-expectations/great_expectations/blob/develop/contrib/experimental/great_expectations_experimental/expectations/expect_queried_table_row_count_to_be.py)
- [expect_queried_column_value_frequency_to_meet_threshold](https://github.com/great-expectations/great_expectations/blob/develop/contrib/experimental/great_expectations_experimental/expectations/expect_queried_column_value_frequency_to_meet_threshold.py)
:::
