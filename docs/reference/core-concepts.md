---
title: Great Expectations Core Concepts
---

import TOCInline from '@theme/TOCInline';

Great Expectations is all about helping you understand data better, so you can communicate with your team and others
about what you've built and what you expect. Great Expectations delivers three key features: **expectations validate
data quality**, **tests are docs, and docs are tests**, and **automatic profiling of data**. This guide helps you
understand *how* Great Expectations does that by describing the core concepts used in the tool. The guide aims for
precision, which can sometimes make it a bit dense, so we include examples of common use cases to help build intuition.

Below, you'll find a brief introduction to the big ideas you'll need to understand how Great Expectations works, information
on some of the defining design decisions in the tool, and links to more detailed documentation on concepts.

**Table of Contents**

<TOCInline toc={toc} />

## Key ideas

If you only have time to remember a few key ideas about Great Expectations, make them these:

It all starts with `Expectations`. An Expectation is how we communicate the way data *should* appear. It's also how
Profilers communicate what they *learn* about data, and what Data Docs uses to *describe* data or *diagnose* problems.
When lots of Expectations are grouped together to define a kind of data asset, like "monthly taxi rides", we call it
an `Expectation Suite`.

`Datasources` are the first thing you'll need to configure to use Great Expectations. A Datasource brings together a way of interacting with data (like a database or spark cluster) and some specific data (a description of that taxi ride data for last month). With a Datasource, you can get a Batch of data or a Validator that can evaluate expectations on
data.

When you're deploying Great Expectations, you'll use a `Checkpoint` to run a validation, testing whether data meets expectations, and potentially performing other actions like building and saving a Data Docs site, sending a notification, or signaling a pipeline runner.

Great Expectations makes it possible to maintain state about data pipelines using `Stores`. A Store is a generalized way of keeping Great Expectations objects, like Expectation Suites, Validation Results, Metrics, Checkpoints, or even Data Docs sites. Stores, and other configuration, is managed using a `Data Context`. The Data Context configuration is usually stored as a yaml file or declared in your pipeline directly, and you should commit the configuration to version control to share it with your team.

## Concepts in the codebase

This section describes links to explanations of the foundational concepts used to integrate Great Expectations into your code. It is a glossary
of the main concepts and classes you will encounter while using Great Expectations.

* [Checkpoints and Actions](./checkpoints-and-actions)
* [Data Context](./data-context)
* [Data discovery](./data-discovery)
* [Data Docs](./data-docs)
* [Datasources](./datasources)
* [Dividing Data Assets into Batches](./dividing-data-assets-into-batches)
* [Evaluation Parameters](./evaluation-parameters)
* [Expectations](./expectations/expectations)
  * [Conditional Expectations](./expectations/conditional-expectations)
  * [Distributional Expectations](./expectations/distributional-expectations)
  * [Implemented Expectations](./expectations/implemented-expectations)
  * [Result format](./expectations/result-format)
  * [Standard arguments](./expectations/standard-arguments)
* [Expectation Suite operations](./expectation-suite-operations)
* [Metrics](./metrics)
* [Profilers](./profilers)
* [Stores](./stores)
* [Validation](./validation)


## Design decisions


Great Expectations is designed to help you think and communicate clearly about your data. To do that, we need to rely on
some specific ideas about *what* we're protecting. You usually do not need to think about these nuances to use Great
Expectations, and many users never think about what *exactly* makes a Data Asset or Batch. But we think it can be
extremely useful to understand the design decisions that guide Great Expectations.

Great Expectations protects **Data Assets**. A **Data Asset** is a logical collection of records. Great Expectations
consumes and creates **metadata about Data Assets**.

- For example, a Data Asset might be *a user table in a database*, *monthly financial data*, *a collection of event log
  data*, or anything else that your organization uses.

How do you know when a collection of records is *one* Data Asset instead of two Data Assets or when two collections of
records are really part of the same Data Asset? In Great Expectations, we think the answer lies in the user. Great
Expectations opens insights and enhances communication while protecting against pipeline risks and data risks, but that
revolves around a *purpose* in using some data (even if that purpose starts out as "I want to understand what I have
here"!).

We recommend that you call a collection of records a Data Asset when you would like to track metadata (and especially, *
Expectations*) about it. **A collection of records is a Data Asset when it's worth giving it a name.**

Since the purpose is so important for understanding when a collection of records is a Data Asset, it immediately follows
that *Data Assets are not necessarily disjoint*. The same data can be in multiple Data Assets. You may have different
Expectations of the same raw data for different purposes or produce documentation tailored to specific analyses and
users.

- Similarly, it may useful to describe subsets of a Data Asset as new Data Assets. For example, if we have a Data Asset
  called the "user table" in our data warehouse, we might also have a different Data Asset called the "Canadian User
  Table" that includes data only for some users.

Not all records in a Data Asset need to be available at the same time or place. A Data Asset could be built from *
streaming data* that is never stored, *incremental deliveries*, *analytic queries*, *incremental updates*, *replacement
deliveries*, or from a *one-time* snapshot.

That implies that a Data Asset is a **logical** concept. Not all of the records may be **accessible** at the same time.
That highlights a very important and subtle point: no matter where the data comes from originally, Great Expectations
validates **batches** of data. A **batch** is a discrete subset of a Data Asset that can be identified by a some
collection of parameters, like the date of delivery, value of a field, time of validation, or access control
permissions.

- Batches often correspond to deliveries of data or runs of an ETL pipeline, but they do not have to. For example, an
  analyst studying New York City taxi data might take one logical view into the data where each batch is a month's
  delivery. But if the analyst selects data from the dataset based on other criteria for her analysis--the time of the
  ride and number of passengers, for example--then each batch corresponds to the specific query she runs. In that case,
  the Expectations she creates may have more to do with the analysis she is running than aggregate characteristics of
  the taxi data.

In some cases **the thing that "makes a batch a batch" is the act of attending to it--for example by validating or
profiling the data**. It's all about **your** Expectations.

Great Expectations provides a mechanism to automatically generate expectations, using a feature called a **Profiler**. A Profiler builds an **Expectation Suite** from one or more **Data Assets**. It may also validates the data against the newly-generated Expectation Suite to return a **Validation Result**. There are several Profilers included with Great Expectations.

A Profiler makes it possible to quickly create a starting point for generating expectations about a Dataset. For example, during the `init` flow, Great Expectations currently uses the **UserConfigurableProfiler** to demonstrate important features of **Expectations** by creating and validating an Expectation Suite that has several different kinds of expectations built from a small sample of data. A Profiler is also critical to generating the Expectation Suites used during profiling.

## Rule-Based Profilers

**Rule-Based profilers** allow users to provide a highly configurable specification which is composed of **Rules** to use in order to build an **Expectation Suite** by profiling existing data.

Imagine you have a table of Sales that comes in every month. You could profile last month's data, inspecting it in order to automatically create a number of expectations that you can use to validate next month's data.  

A **Rule** in a rule-based profiler could say something like "Look at every column in my Sales table, and if that column is numeric, add an `expect_column_values_to_be_between` Expectation to my Expectation Suite, where the `min_value` for the Expectation is the minimum value for the column, and the `max_value` for the Expectation is the maximum value for the column."

Each rule in a rule-based profiler has three types of components:

1. **DomainBuilders**: A DomainBuilder will inspect some data that you provide to the Profiler, and compile a list of Domains for which you would like to build expectations
1. **ParameterBuilders**: A ParameterBuilder will inspect some data that you provide to the Profiler, and compile a dictionary of Parameters that you can use when constructing your ExpectationConfigurations
1. **ExpectationConfigurationBuilders**: An ExpectationConfigurationBuilder will take the Domains compiled by the DomainBuilder, and assemble ExpectationConfigurations using Parameters built by the ParameterBuilder

In the above example, imagine your table of Sales has twenty columns, of which five are numeric:
* Your **DomainBuilder** would inspect all twenty columns, and then yield a list of the five numeric columns
* You would specify two **ParameterBuilders**: one which gets the min of a column, and one which gets a max. Your Profiler would loop over the Domain (or column) list built by the **DomainBuilder** and use the two ParameterBuilders to get the min and max for each column.
* Then the Profiler loops over Domains built by the DomainBuilder and uses the **ExpectationConfigurationBuilders** to add a `expect_column_values_to_between` column for each of these Domains, where the `min_value` and `max_value` are the values that we got in the ParameterBuilders.

In addition to Rules, a rule-based profiler enables you to specify **Variables**, which are global and can be used in any of the Rules. For instance, you may want to reference the same BatchRequest or the same tolerance in multiple Rules, and declaring these as Variables will enable you to do so. 

#### Example Config:
```yaml
variables:
  my_last_month_sales_batch_request: # We will use this BatchRequest in our DomainBuilder and both of our ParameterBuilders so we can pinpoint the data to Profile
    datasource_name: my_sales_datasource
    data_connector_name: monthly_sales
    data_asset_name: sales_data
    data_connector_query:
      index: -1
  mostly_default: 0.95 # We can set a variable here that we can reference as the `mostly` value for our expectations below
rules:
  my_rule_for_numeric_columns: # This is the name of our Rule
    domain_builder:
      batch_request: $variables.my_last_month_sales_batch_request # We use the BatchRequest that we specified in Variables above using this $ syntax
      class_name: SemanticTypeColumnDomainBuilder # We use this class of DomainBuilder so we can specify the numeric type below
      semantic_types:
        - numeric
    parameter_builders:
      - parameter_name: my_column_min
        class_name: MetricParameterBuilder
        batch_request: $variables.my_last_month_sales_batch_request
        metric_name: column.min # This is the metric we want to get with this ParameterBuilder
        metric_domain_kwargs: $domain.domain_kwargs # This tells us to use the same Domain that is gotten by the DomainBuilder. We could also put a different column name in here to get a metric for that column instead.
      - parameter_name: my_column_max
        class_name: MetricParameterBuilder
        batch_request: $variables.my_last_month_sales_batch_request
        metric_name: column.max
        metric_domain_kwargs: $domain.domain_kwargs
    expectation_configuration_builders:
      - expectation_type: expect_column_values_to_be_between # This is the name of the expectation that we would like to add to our suite
        class_name: DefaultExpectationConfigurationBuilder
        column: $domain.domain_kwargs.column
        min_value: $parameter.my_column_min.value # We can reference the Parameters created by our ParameterBuilders using the same $ notation that we use to get Variables
        max_value: $parameter.my_column_max.value
        mostly: $variables.mostly_default
```

You can see another example config containing multiple rules here: [alice_user_workflow_verbose_profiler_config.yml](https://github.com/great-expectations/great_expectations/blob/develop/tests/rule_based_profiler/alice_user_workflow_verbose_profiler_config.yml)

This config is used in the below diagram to provide a better sense of how the different parts of the Profiler config fit together. [You can see a larger version of this file here.](https://github.com/great-expectations/great_expectations/blob/develop/docs/guides/images/rule_based_profiler_public_interface_diagram.png)
![Rule-Based Profiler Public Interface Diagram](../guides/images/rule_based_profiler_public_interface_diagram.png)

### Next Steps
- You can try out a tutorial that walks you through the set-up of a Rule-Based Profiler here: [How to create a new Expectation Suite using Rule Based Profilers](../guides/expectations/advanced/how-to-create-a-new-expectation-suite-using-rule-based-profilers)
