---
title: What is an Expectation?
---


An Expectation is a statement describing a verifiable property of data. Like assertions in traditional Python unit
tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit
tests, Great Expectations applies Expectations to data instead of code.

Great Expectations' built-in library includes more than 50 common Expectations, such as:

* `expect_column_values_to_not_be_null`
* `expect_column_values_to_match_regex`
* `expect_column_values_to_be_unique`
* `expect_column_values_to_match_strftime_format`
* `expect_table_row_count_to_be_between`
* `expect_column_median_to_be_between`

For a full list of available Expectations, please check out the [Expectation Gallery](https://greatexpectations.io/expectations).

You can also extend Great Expectations by [creating your own Custom Expectations](../../guides/expectations/creating_custom_expectations/overview.md).

Expectations *enhance communication* about your data and *improve quality* for data applications. Using Expectations
helps reduce trips to domain experts and avoids leaving insights about data on the "cutting room floor."

## Expectation Concepts: Domain and Success Keys

A **domain** makes it possible to address a specific set of data, such as a *table*, *query result*, *column* in a table
or dataframe, or even a metric computed on a previous batch of data.

A domain is defined by a set of key-value pairs. The **domain keys** are the keys that uniquely define the domain for an
Expectation. They vary depending on the Expectatation; for example, many Expectations apply to data in a single `column`
, but others apply to data from multiple columns or to properties that do not apply to a column at all.

An Expectation also defines **success keys** that determine the values of its metrics and when the Expectation will
succeed.

For example, the `expect_column_values_to_be_in_set` Expectation relies on the `batch_id`, `table`, `column`,
and `row_condition` **domain keys** to determine what data are described by a particular configuration, and
the `value_set` and `mostly` **success keys** to evaluate whether the Expectation is actually met for that data.

**Note**: The *batch_id* and *table* domain keys are often omitted when running a validation, because the Expectation is
being applied to a single batch and table. However, they must be provided in cases where they could be ambiguous.

**Metrics** use a similar concept: they also use the same kind of **domain keys** as Expectations, but instead of
success keys, we call the keys that determine a Metric's value its **value keys**.

## Expectation Suites

Expectation Suites combine multiple Expectations into an overall description of a dataset. For example, a team can group
all the Expectations about its `rating` table in the movie ratings database from our previous example into an
Expectation Suite and call it `movieratings.ratings`. Note these names are completely flexible and the only constraint
on the name of a suite is that it must be unique to a given project.

Each Expectation Suite is saved as a JSON file in the `great_expectations/expectations` subdirectory of the Data
Context. Users check these files into the version control each time they are updated, same way they treat their source
files. This discipline allows data quality to be an integral part of versioned pipeline releases.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through an iterative loop of Review and Edit
as the team's understanding of the data described by the suite evolves.

## Methods for creating and editing Expectations

Generating Expectations is one of the most important parts of using Great Expectations effectively, and there are a
variety of methods for generating and encoding Expectations. When Expectations are encoded in the Great Expectations
format, they become shareable and persistent sources of truth about how data was expected to behave-and how it actually
did.

There are several paths to generating Expectations:

1. Automated inspection of datasets. Currently, the profiler mechanism in Great Expectations produces Expectation Suites
   that can be used for validation. In some cases, the goal is [profiling](../profilers.md) your data, and in other cases automated
   inspection can produce Expectations that will be used in validating future batches of data.
1. Expertise. Rich experience from subject-matter experts, Analysts, and data owners is often a critical source of
   Expectations. Interviewing experts and encoding their tacit knowledge of common distributions, values, or failure
   conditions can be can excellent way to generate Expectations.
1. Exploratory Analysis. Using Great Expectations in an exploratory analysis workflow (e.g. within Jupyter Notebooks)
   is an important way to develop experience with both raw and derived datasets and generate useful and testable
   Expectations about characteristics that may be important for the data's eventual purpose, whether reporting or
   feeding another downstream model or data system.

## Custom Expectations

Expectations are especially useful when they capture critical aspects of data understanding that analysts and
practitioners know based on its *semantic* meaning. It's common to want to extend Great Expectations with application-or
domain-specific Expectations. For example:

```bash
expect_column_text_to_be_in_english
expect_column_value_to_be_valid_icd_code
```

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Building Custom Expectations is easy and allows your custom logic to become part of the validation, documentation, and
even profiling workflows that make Great Expectations stand out. See the guide on [creating Custom Expectations](../../guides/expectations/creating_custom_expectations/overview.md)
for more information on building Expectations and updating Data Context configurations to automatically load batches of
data with custom Data Assets.
