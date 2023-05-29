---
id: profiler
title: Profiler
hoverText: Generates Metrics and candidate Expectations from data.
---

import TechnicalTag from '../term_tags/_tag.mdx';

A Profiler generates <TechnicalTag relative="../" tag="metric" text="Metrics" /> and candidate <TechnicalTag relative="../" tag="expectation" text="Expectations" /> from data.

A Profiler creates a starting point for quickly generating Expectations.

There are several Profilers included with Great Expectations; conceptually, each Profiler is a checklist of questions which will generate an Expectation Suite when asked of a Batch of data.

## Relationship to other objects

A Profiler builds an Expectation Suite from one or more Data Assets. Many Profiler workflows will also include a step that <TechnicalTag relative="../" tag="validation" text="Validates" /> the data against the newly-generated Expectation Suite to return a <TechnicalTag relative="../" tag="validation_result" text="Validation Result" />.

## Use cases

Profilers come into use when it is time to configure Expectations for your project.  At this point in your workflow you can configure a new Profiler, or use an existing one to generate Expectations from a <TechnicalTag relative="../" tag="batch" text="Batch" /> of data.

For details on how to configure a customized Profiler, see our guide on [how to create a new expectation suite using a Profiler](../guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers.md).

## Profiler types

There are multiple types of Profilers built in to Great Expectations.  Below is a list with overviews of each one.  For more information, you can view their docstrings and source code in the `great_expectations\profile` [folder on our GitHub](https://github.com/great-expectations/great_expectations/tree/develop/great_expectations/profile).

### Profiler

The Custom Profiler allows you to directly configure a customized Profiler through a YAML configuration.  Profilers allow you to integrate organizational knowledge about your data into the profiling process. For example, a team might have a convention that all columns **named** "id" are primary keys, whereas all columns ending with the **suffix** "_id" are foreign keys. In that case, when the team using Great Expectations first encounters a new dataset that followed the convention, a Profiler could use that knowledge to add an `expect_column_values_to_be_unique` Expectation to the "id" column (but not, for example an "address_id" column).

For details on how to configure a customized Profiler, see our guide on [how to create a new expectation suite using a Profiler](../guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers.md).

## Create

It is unlikely that you will need to create a customized Profiler by extending an existing Profiler with a subclass.  Instead, you should work with a Profiler which can be fully configured in a YAML configuration file.

Configuring a custom Profiler is covered in the following section.  See also [How to create a new expectation suite using a Profiler](../guides/expectations/advanced/how_to_create_a_new_expectation_suite_using_rule_based_profilers.md), or [the full source code for that guide](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/expectations/advanced/multi_batch_rule_based_profiler_example.py) on our GitHub as an example.

## Configure Profilers

**Profilers** allow users to provide a highly configurable specification which is composed of **Rules** to use in order to build an **Expectation Suite** by profiling existing data.

Imagine you have a table of Sales that comes in every month. You could profile last month's data, inspecting it in order to automatically create a number of expectations that you can use to validate next month's data.  

A **Rule** in a Profiler could say something like "Look at every column in my Sales table, and if that column is numeric, add an `expect_column_values_to_be_between` Expectation to my Expectation Suite, where the `min_value` for the Expectation is the minimum value for the column, and the `max_value` for the Expectation is the maximum value for the column."

Each rule in a Profiler has three types of components:

1. **DomainBuilders**: A DomainBuilder will inspect some data that you provide to the Profiler, and compile a list of Domains for which you would like to build expectations
1. **ParameterBuilders**: A ParameterBuilder will inspect some data that you provide to the Profiler, and compile a dictionary of Parameters that you can use when constructing your ExpectationConfigurations
1. **ExpectationConfigurationBuilders**: An ExpectationConfigurationBuilder will take the Domains compiled by the DomainBuilder, and assemble ExpectationConfigurations using Parameters built by the ParameterBuilder

In the above example, imagine your table of Sales has twenty columns, of which five are numeric:
* Your **DomainBuilder** would inspect all twenty columns, and then yield a list of the five numeric columns
* You would specify two **ParameterBuilders**: one which gets the min of a column, and one which gets a max. Your Profiler would loop over the Domain (or column) list built by the **DomainBuilder** and use the two `ParameterBuilders` to get the min and max for each column.
* Then the Profiler loops over Domains built by the `DomainBuilder` and uses the **ExpectationConfigurationBuilders** to add a `expect_column_values_to_between` column for each of these Domains, where the `min_value` and `max_value` are the values that we got in the `ParameterBuilders`.

In addition to Rules, a Profiler enables you to specify **Variables**, which are global and can be used in any of the Rules. For instance, you may want to reference the same `BatchRequest` or the same tolerance in multiple Rules, and declaring these as Variables will enable you to do so. 

Below is an example configuration based on this discussion:

```yaml title="YAML configuration"
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
