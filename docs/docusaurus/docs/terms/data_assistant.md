---
id: data_assistant
title: Data Assistant
---
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

A Data Assistant is a pre-configured utility that simplifies the creation of <TechnicalTag tag="expectation" text="Expectations" />. A Data Assistant can help you determine a starting point when working with a large, new, or complex dataset by asking questions and then building a list of relevant <TechnicalTag tag="metric" text="Metrics" /> from the answers to those questions. Branching question paths based on your responses ensure that additional, relevant Metrics are not missed. The result is a comprehensive collection of Metrics that can be saved, reviewed as graphical plots, or used by the Data Assistant to generate a set of proposed Expectations.

Data Assistants allow you to introspect multiple <TechnicalTag tag="batch" text="Batches" /> and create an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> from the aggregated Metrics of those Batches.  They provide convenient, visual representations of the generated Expectations to assist with identifying outliers in the corresponding parameters.  They can be accessed from your <TechnicalTag tag="data_context" text="Data Context" />, and provide a good starting point for building Expectations or performing initial data exploration.

## Relationships to other objects

A Data Assistant implements a pre-configured <TechnicalTag tag="profiler" text="Profiler" /> in order to gather Metrics and propose an Expectation Suite based on the introspection of the Batch or Batches contained in a provided <TechnicalTag tag="batch_request" text="Batch Request" />.

## Use cases

Data Assistants are an ideal starting point for creating Expectations.  When you're working with unfamiliar data, a Data Assistant can provide an overview by introspecting the data and generating a series of relevant Expectations using estimated parameters for you to review. When you use the `"flag_outliers"` value for the `estimation` parameter, your generated Expectations have parameters that disregard values that the Data Assistant identifies as outliers. To create a graphical representation of the generated Expectations, use the Data Assistant's `plot_metrics()` method. Reviewing the Data Assistant's results can help you identify outliers in your data.

When you're working with familiar, good data, a Data Assistant can use the `"exact"` value for the `estimation` parameter to provide comprehensive Expectations that reflect the values found in the provided data.

## Profiling

To provide a representative analysis of the provided data, a Data Assistant can automatically process multiple Batches from a single Batch Request.

## Multi-Batch introspection

Data Assistants leverage the ability to process multiple Batches from a single Batch Request to provide a representative analysis of the provided data.  With previous Profilers you would only be able to introspect a single Batch at a time.  This meant that the Expectation Suite generated would only reflect a single Batch.  If you had many Batches of data that you wanted to build inter-related Expectations for, you would have needed to run each Batch individually and then manually compare and update the Expectation parameters that were generated.  With a Data Assistant, that process is automated.  You can provide a Data Assistant multiple Batches and get back Expectations that have parameters based on, for instance, the mean or median value of a column on a per-Batch basis. 

## Visual plots for Metrics

When working in a Jupyter Notebook you can use the `plot_metrics()` method of a Data Assistant's result object to generate a visual representation of your Expectations, the values that were assigned to their parameters, and the Metrics that informed those values.  This assists in exploratory data analysis and fine-tuning your Expectations, while providing complete transparency into the information used by the Data Assistant to build your Expectations.

Data Assistants can be accessed from your Data Context. To select a Data Assistant in a Jupyter Notebook, enter `context.assistants.` and use code completion.  All Data Assistants have a `run(...)` method that takes in a Batch Request and numerous optional parameters, and then loads the results into an Expectation Suite for future use.

To access the Onboarding Data Assistant, use `context.assistants.onboarding`.

:::note For more information about the Onboarding Data Assistant, see [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md).
:::

## Configure

Data Assistants are pre-configured. You provide the Batch Request, and some optional parameters in the Data Assistant's `run(...)` method.

## Related documentation

To learn more about working with the Onboarding Data Assistant, see [How to create an Expectation Suite with the Onboarding Data Assistant](../guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.md).
