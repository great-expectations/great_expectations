---
title: Expectation
id: expectation
hoverText: A verifiable assertion about data.
---
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import TechnicalTag from '../term_tags/_tag.mdx';
import SetupHeader from '/docs/images/universal_map/_um_setup_header.mdx'
import ConnectHeader from '/docs/images/universal_map/_um_connect_header.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';
import ValidateHeader from '/docs/images/universal_map/_um_validate_header.mdx';


<UniversalMap setup='inactive' connect='inactive' create='active' validate='active'/> 

## Overview

### Definition

An Expectation is a verifiable assertion about data. 

### Features and promises

Great Expectations is a framework for defining Expectations and running them against your data. Like assertions in traditional Python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code. For example, you could define an Expectation that a column contains no null values, and Great Expectations would run that Expectation against your data, and report if a null value was found.

Expectations *enhance communication* about your data and *improve quality* for data applications. They help you take the implicit assumptions about your data and make them explicit. Using Expectations helps reduce trips to domain experts and avoids leaving insights about data on the "cutting room floor."

### Relationship to other objects

Expectations are grouped into <TechnicalTag relative="../" tag="expectation_suite" text="Expectation Suites" />, which are in turn stored and retrieved through an <TechnicalTag relative="../" tag="expectation_store" text="Expectation Store" />.  <TechnicalTag relative="../" tag="profiler" text="Profilers" /> will analyze data in order to generate Expectations, and <TechnicalTag relative="../" tag="checkpoint" text="Checkpoints" /> rely on Expectation Suites (and the Expectations contained therein) to <TechnicalTag relative="../" tag="validation" text="Validate" /> data.

## Use cases

<CreateHeader/>

Expectations are obviously a fundamental component of the Create Expectations step in working with Great Expectations.  There are two points at which you will have direct interaction with them.  The first is when you are creating new Expectations.  The second is when you are editing them.  Expectations are not meant to be static: the recommended best practice is an iterative process where your Expectations are edited as your data, and your understanding of that data, change.  For further information on this process, please see [our overview on the Create Expectations process](../guides/expectations/create_expectations_overview.md), and [our related how-to guides](../guides/expectations/index.md).


<ValidateHeader/>

When you create your Checkpoints, you will be able to configure them to use specific Expectation Suites.  Other than setting up this configuration (or arranging to pass Expectation Suites at runtime) you will not need to directly interact with the Expectations themselves.  Instead, when you run your Checkpoint it will handle the use of the Expectations in any of its Expectation Suites to Validate the data indicated in its Batch Request/s.  This will be done under the hood, with the <TechnicalTag relative="../" tag="validation_result" text="Validation Results" /> that are generated being passed along to the Checkpoint's (optional) <TechnicalTag relative="../" tag="action" text="Actions" /> for further processing.


## Features

### Versatility out of the box

Great Expectations' built-in library includes more than 50 common Expectations, such as:

* `expect_column_values_to_not_be_null`
* `expect_column_values_to_match_regex`
* `expect_column_values_to_be_unique`
* `expect_column_values_to_match_strftime_format`
* `expect_table_row_count_to_be_between`
* `expect_column_median_to_be_between`

For a full list of available Expectations, please check out the [Expectation Gallery](https://greatexpectations.io/expectations).

### Customization

Expectations are especially useful when they capture critical aspects of data understanding that analysts and practitioners know based on its *semantic* meaning. It's common to want to extend Great Expectations with application or domain specific Expectations. For example:

```bash
expect_column_text_to_be_in_english
expect_column_value_to_be_valid_icd_code
```

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Building custom Expectations is easy and allows your custom logic to become part of the validation, documentation, and even profiling workflows that make Great Expectations stand out. See the [guides on creating custom Expectations](/docs/guides/expectations/index#creating-custom-expectations) for more information on building Expectations and updating <TechnicalTag relative="../" tag="data_context" text="Data Context" /> configurations to automatically load <TechnicalTag relative="../" tag="batch" text="Batches" /> of data with custom <TechnicalTag relative="../" tag="data_asset" text="Data Assets" />.

### Distributional Expectations

Distributional Expectations rely on expected distributions or "partition objects", which are built from intervals for continuous data or categorical classes and their associated weights, in order to help identify when new datasets or samples may be different from expected.  Distributional Expectations represent specific Expectation types, such as `expect_column_kl_divergence_to_be_less_than`.  You should use Distributional Expectations in the same way as other Expectations: to help accelerate identification of risks and changes to a modeled system or disruptions to a complex upstream data feed.

For more information, please see our [reference guide on Distributional Expectations](../reference/expectations/distributional_expectations.md).

### Conditional Expectations

Conditional Expectations are those that are intended to be applied not to an entire dataset, but to a particular subset of the data.  Additionally, Conditional Expectations include those where what one expects of one variable depends on the value of another.  An example of this would be an Expectation that a column not have a null value only if another column's value falls into a specific subset.  Conditional Expectations represent a facet of map Expectations, including such things as `expect_column_values_to_be_in_set`.

Conditional Expectations are currently only available for the Pandas backend.

For more information on these Expectations, please see [our reference guide for Conditional Expectations](../reference/expectations/conditional_expectations.md).

## Limitations

Unfortunately, not all Expectations are implemented for all source data systems.  To see a list of what Expectations are implemented for a given backend, please reference our [guide to implemented Expectations](../reference/expectations/implemented_expectations.md).

Likewise, Conditional Expectations are considered **experimental** and are only available for the Pandas backend; not for the Spark and SQLAlchemy backends.  You can reference our [documentation on Conditional Expectations](../reference/expectations/conditional_expectations.md) for more information.

## API basics

### How to access

You may directly access Expectations as part of the interactive workflow for creating new Expectations.  For further details on this process, please see our guide on [how to create and edit Expectations with instant feedback from a sample Batch of data](../guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md).  

### How to create

Generating Expectations is one of the most important parts of using Great Expectations effectively, and there are a variety of methods for generating and encoding Expectations. When Expectations are encoded in the Great Expectations format, they become shareable and persistent sources of truth about how data was expected to behave-and how it actually
did.

There are several paths to generating Expectations:

1. Automated inspection of datasets. Currently, the profiler mechanism in Great Expectations produces Expectation Suites that can be used for validation. In some cases, the goal is [profiling](../terms/profiler.md) your data, and in other cases automated inspection can produce Expectations that will be used in validating future batches of data.
2. Expertise. Rich experience from subject-matter experts, Analysts, and data owners is often a critical source of Expectations. Interviewing experts and encoding their tacit knowledge of common distributions, values, or failure conditions can be can excellent way to generate Expectations.
3. Exploratory Analysis. Using Great Expectations in an exploratory analysis workflow (e.g. within Jupyter Notebooks)is an important way to develop experience with both raw and derived datasets and generate useful and testable Expectations about characteristics that may be important for the data's eventual purpose, whether reporting or feeding another downstream model or data system.

For more information on these methods, please see our [overview guide for creating Expectations](../guides/expectations/create_expectations_overview.md#the-create-expectations-process).

### Configuration

Most of the time you will not need to directly interact with an Expectation's configurations.  However, advanced users may have circumstances in which it is desirable to define Expectations based purely on domain knowledge, without comparing against underlying data.  To do this, you will need to directly write an Expectation's configuration.  For details on how to do this, please reference our guide on [how to create and edit Expectations based on domain knowledge, without inspecting data directly](../guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly.md).

The other occasion when you may want to edit an Expectation's configuration is when you need to edit the `result_format` of a <TechnicalTag relative="../" tag="custom_expectation" text="Custom Expectation" />  The `result_format` parameter may be either a string or a dictionary which specifies the fields to return in `result`.  For further details, please see [our reference guide on the result_format parameter](../reference/expectations/result_format.md).

### Results

All Expectations return a JSON-serializable dictionary when evaluated, which consists of four standard (though optional, depending on the type of Expectation in question) arguments.  These are: `result_format`, `include_config`, `catch_exceptions`, and `meta`.  For a more detailed explanation as to what each of these arguments consists of and which Expectations use them, please see [our reference guide on standard arguments](../reference/expectations/standard_arguments.md).

## More details

### Expectation Concepts: Domain and Success Keys

A **domain** makes it possible to address a specific set of data, such as a *table*, *query result*, *column* in a table or dataframe, or even a Metric computed on a previous Batch of data.  It does this by describing the locale of data to which a Metric or Expectation applies.

A domain is defined by a set of key-value pairs. The **domain keys** are the keys that uniquely define the domain for an Expectation. They vary depending on the Expectation; for example, many Expectations apply to data in a single `column`, but others apply to data from multiple columns or to properties that do not apply to a column at all.

An Expectation also defines **success keys** that specify the values of metrics that determine when the Expectation will succeed.

For example, the `expect_column_values_to_be_in_set` Expectation relies on the `batch_id`, `table`, `column`, and `row_condition` **domain keys** to determine what data are described by a particular configuration, and the `value_set` and `mostly` **success keys** to evaluate whether the Expectation is actually met for that data.

**Note**: The *batch_id* and *table* domain keys are often omitted when running a validation, because the Expectation is being applied to a single batch and table. However, they must be provided in cases where they could be ambiguous.

**Metrics** use a similar concept: they also use the same kind of **domain keys** as Expectations, but instead of success keys, we call the keys that determine a Metric's value its **value keys**.

