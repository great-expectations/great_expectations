---
id: data_assistant
title: Data Assistant
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';

<UniversalMap setup='inactive' connect='inactive' create='active' validate='inactive'/>

A Data Assistant is a pre-configured utility that simplifies the creation of <TechnicalTag tag="expectation" text="Expectations" />. A Data Assistant can help you determine a starting point when working with a large, new, or complex dataset by asking questions and then building a list of relevant <TechnicalTag tag="metric" text="Metrics" /> from the answers to those questions. Branching question paths based on your responses ensure that additional, relevant Metrics are not missed. The result is a comprehensive collection of Metrics that can be saved, reviewed as graphical plots, or used by the Data Assistant to generate a set of proposed Expectations.

Data Assistants allow you to introspect multiple <TechnicalTag tag="batch" text="Batches" /> and create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> from the aggregated Metrics of those Batches.  They provide convenient, visual representations of the generated Expectations to assist with identifying outliers in the corresponding parameters.  They can be accessed from your <TechnicalTag tag="data_context" text="Data Context" />, and provide a good starting point for building Expectations or performing initial data exploration.

## Relationships to other objects

A Data Assistant implements a pre-configured <TechnicalTag tag="profiler" text="Rule Based Profiler" /> in order to gather Metrics and propose an Expectation Suite based on the introspection of the Batch or Batches contained in a provided <TechnicalTag tag="batch_request" text="Batch Request" />.

## Use cases

Data Assistants are an ideal starting point for creating Expectations.  When you're working with unfamiliar data, a Data Assistant can provide an overview by introspecting the data and generating a series of relevant Expectations using estimated parameters for you to review. When you use the `"flag_outliers"` value for the `estimation` parameter, your generated Expectations have parameters that disregard values that the Data Assistant identifies as outliers. To create a graphical representation of the generated Expectations, use the Data Assistant's `plot_metrics()` method. Reviewing the Data Assistant's results can help you identify outliers in your data.

When you're working with familiar, good data, a Data Assistant can use the `"exact"` value for the `estimation` parameter to provide comprehensive Expectations that reflect the values found in the provided data.

## Profiling

Data Assistants implement pre-configured Rule-Based Profilers, but they can also provide extended functionality. You can call them directly from your Data Context.  This ensures that they provide a quick method of creating Expectations and <TechnicalTag tag="profiling" text="Profiling" /> your data. The rules implemented by a Data Assistant are fully exposed in the parameters for its `run(...)` method. You can use a Data Assistant easily without making any customizations, or you can customize the behavior to meet the specific requirements f your organization.

## Multi-Batch introspection

To provide a representative analysis of the provided data, a Data Assistant can automatically process multiple Batches from a single Batch Request.

## Visual plots for Metrics

In a Jupyter Notebook, you can use the `plot_metrics()` method of a Data Assistant's result object to generate a visual representation of your Expectations, the values that were assigned to their parameters, and the Metrics that informed those values.  This can help you with your exploratory data analysis and help you refine your Expectations, while providing complete transparency into the information used by the Data Assistant to build your Expectations.

## API basics

Data Assistants can be accessed from your Data Context. To select a Data Assistant in a Jupyter Notebook, enter `context.assistants.` and use code completion.  All Data Assistants have a `run(...)` method that takes in a Batch Request and numerous optional parameters, and then loads the results into an Expectation Suite for future use.

To access the Onboarding Data Assistant, use `context.assistants.onboarding`.

:::note For more information about the Onboarding Data Assistant, see [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md).
:::

## Configuration

Data Assistants are pre-configured. You provide the Batch Request, and some optional parameters in the Data Assistant's `run(...)` method.

## Related documentation

Data Assistants are multi-batch aware. For more information about using a single or multiple Batches of data in a Batch Request, see [How to choose between working with a single or multiple Batches of data](../guides/connecting_to_your_data/how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md)

To take advantage of the multi-batch awareness of Data Assistants, your <TechnicalTag tag="datasource" text="Datasources" /> need to be configured so that you can acquire multiple Batches in a single Batch Request. To configure your Datasources to return multiple Batches, see the following documentation:
- [How to configure a Pandas Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource.md)
- [How to configure a Spark Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource.md)
- [How to configure a SQL Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource.md)

To request multiple Batches in a single Batch Request, see [How to get one or more Batches of data from a configured Datasource](../guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.md).

To learn more about working with the Onboarding Data Assistant, see [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md).