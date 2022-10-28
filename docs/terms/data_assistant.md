---
id: data_assistant
title: Data Assistant
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';
import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';
import CreateHeader from '/docs/images/universal_map/_um_create_header.mdx';

<UniversalMap setup='inactive' connect='inactive' create='active' validate='inactive'/>

## Overview

### Definition

A Data Assistant is a utility that asks questions about your data, gathering information to describe what is observed, and then presents <TechnicalTag tag="metric" text="Metrics" /> and proposes <TechnicalTag tag="expectation" text="Expectations" /> based on the answers.

### Features and promises

Data Assistants allow you to introspect multiple <TechnicalTag tag="batch" text="Batches" /> and create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> from the aggregated Metrics of those Batches.  They provide convenient, visual representations of the generated Expectations to assist with identifying outliers in the corresponding parameters.  They are convenient to access from your <TechnicalTag tag="data_context" text="Data Context" />, and provide an excellent starting point for building Expectations or performing initial data exploration.

### Relationships to other objects

A Data Assistant implements a pre-configured <TechnicalTag tag="profiler" text="Rule Based Profiler" /> in order to gather Metrics and propose an Expectation Suite based on the introspection of the Batch or Batches contained in a provided <TechnicalTag tag="batch_request" text="Batch Request" />.

## Use cases

<CreateHeader/>

Data Assistants are an ideal starting point for creating your Expectations.  If you are working with data that you are not familiar with, a Data Assistant can give you an overview by introspecting it and generating a series of relevant Expectations using estimated parameters for you to review. If you use the `"flag_outliers"` value for the `estimation` parameter your generated Expectations will have parameters that disregard values that the Data Assistant identifies as outliers. Using the Data Assistant's `plot_metrics()` method will then give you a graphical representation of the generated Expectations.  This will further assist you in spotting outliers in your data when reviewing the Data Assistant's results.

Even when working with data that you are familiar with and know is good, a Data Assistant can use the `"exact"` value for the `estimation` parameter to provide comprehensive Expectations that exactly reflect the values found in the provided data.

## Features

### Easy profiling

Data Assistants implement pre-configured Rule-Based Profilers under the hood, but also provide extended functionality.  They are easily accessible: You can call them directly from your Data Context.  This ensures that they will always provide a quick, simple entry point to creating Expectations and <TechnicalTag tag="profiling" text="Profiling" /> your data.  However, the rules implemented by a Data Assistant are also fully exposed in the parameters for its `run(...)` method.  This means that while you can use a Data Assistant easily out of the box, you can also customize it behavior to take advantage of the domain knowledge possessed by subject-matter experts.

### Multi-Batch introspection

Data Assistants leverage the ability to process multiple Batches from a single Batch Request to provide a representative analysis of the provided data.  With previous Profilers you would only be able to introspect a single Batch at a time.  This meant that the Expectation Suite generated would only reflect a single Batch.  If you had many Batches of data that you wanted to build inter-related Expectations for, you would have needed to run each Batch individually and then manually compare and update the Expectation parameters that were generated.  With a Data Assistant, that process is automated.  You can provide a Data Assistant multiple Batches and get back Expectations that have parameters based on, for instance, the mean or median value of a column on a per-Batch basis. 

### Visual plots for Metrics

 When working in a Jupyter Notebook you can use the `plot_metrics()` method of a Data Assistant's result object to generate a visual representation of your Expectations, the values that were assigned to their parameters, and the Metrics that informed those values.  This assists in exploratory data analysis and fine-tuning your Expectations, while providing complete transparency into the information used by the Data Assistant to build your Expectations.

## API basics

Data Assistants can be easily accessed from your Data Context.  In a Jupyter Notebook, you can enter `context.assistants.` and use code completion to select the Data Assistant you wish to use.  All Data Assistants have a `run(...)` method that takes in a Batch Request and numerous optional parameters, the results of which can be loaded into an Expectation Suite for future use.

The Onboarding Data Assistant is an ideal starting point for working with Data Assistants.  It can be accessed from `context.assistants.onboarding`, or from the <TechnicalTag tag="cli" text="CLI" /> command `great_expectations suite new --profile`.

:::note For more information on the Onboarding Data Assistant, see the guide:
- [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md)
:::

### Configuration

Data Assistants come pre-configured!  All you need to provide is a Batch Request, and some optional parameters in the Data Assistant's `run(...)` method.

## More details

### Design motivation

Data Assistants were designed to make creating Expectations easier for users of Great Expectations.  A Data Assistant will help solve the problem of "where to start" when working with a large, new, or complex dataset by greedily asking questions according to a set theme and then building a list of all the relevant Metrics that it can determine from the answers to those questions.  Branching question paths ensure that additional relevant Metrics are gathered on the groundwork of the earlier questions asked.  The result is a comprehensive gathering of Metrics that can then be saved, reviewed as graphical plots, or used by the Data Assistant to generate a set of proposed Expectations.

### Additional documentation

Data Assistants are multi-batch aware out of the box.  However, not every use case requires multiple Batches.  For more information on when it is best to work with either a single Batch or multiple Batches of data in a Batch Request, please see the following guide:
- [How to choose between working with a single or multiple Batches of data](../guides/connecting_to_your_data/how_to_choose_between_working_with_a_single_or_multiple_batches_of_data.md)

To take advantage of the multi-batch awareness of Data Assistants, your <TechnicalTag tag="datasource" text="Datasources" /> need to be configured so that you can acquire multiple Batches in a single Batch Request.  For guidance on how to configure your Datasources to be capable of returning multiple Batches, please see the following documentation that matches the Datasource type you are working with:
- [How to configure a Pandas Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource.md)
- [How to configure a Spark Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource.md)
- [How to configure a SQL Datasource](../guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource.md)

For guidance on how to request multiple Batches in a single Batch Request, please see the guide:
- [How to get one or more Batches of data from a configured Datasource](../guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource.md)

For an overview of working with the Onboarding Data Assistant, please see the guide:
- [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md)