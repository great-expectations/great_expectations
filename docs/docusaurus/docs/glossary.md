---
id: glossary
title: "Concepts"
displayed_sidebar: 'learn'
slug: '/conceptual_guides/'
---

[**Action:**](/docs/oss/terms/action) A Python class with a run method that takes a Validation Result and does something with it

[**Batch:**](/docs/oss/terms/batch) A selection of records from a Data Asset.

[**Batch Request:**](/docs/oss/terms/batch_request) Provided to a Data Source in order to create a Batch.

[**Checkpoint:**](/docs/oss/terms/checkpoint) The primary means for validating data in a production deployment of Great Expectations.

[**Checkpoint Store:**](/docs/oss/terms/checkpoint_store) A connector to store and retrieve information about means for validating data in a production deployment of Great Expectations.

[**Custom Expectation:**](/docs/oss/terms/custom_expectation) An extension of the `Expectation` class, developed outside of the Great Expectations library.

[**Data Asset:**](/docs/oss/terms/data_asset) A collection of records within a Data Source which is usually named based on the underlying data system and sliced to correspond to a desired specification.

[**Data Assistant:**](/docs/oss/terms/data_assistant) A utility that asks questions about your data, gathering information to describe what is observed, and then presents Metrics and proposes Expectations based on the answers.

[**Data Context:**](/docs/oss/terms/data_context) The primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.

[**Data Docs:**](/docs/oss/terms/data_docs) Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[**Data Docs Store:**](/docs/oss/terms/data_docs_store) A connector to store and retrieve information pertaining to Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[**Data Source:**](/docs/oss/terms/datasource) Provides a standard API for accessing and interacting with data from a wide variety of source systems.

[**Evaluation Parameter:**](/docs/oss/terms/evaluation_parameter) A dynamic value used during Validation of an Expectation which is populated by evaluating simple expressions or by referencing previously generated metrics.

[**Evaluation Parameter Store:**](/docs/oss/terms/evaluation_parameter_store) A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.

[**Execution Engine:**](/docs/oss/terms/execution_engine) A system capable of processing data to compute Metrics.

[**Expectation:**](/docs/oss/terms/expectation) A verifiable assertion about data.

[**Expectation Store:**](/docs/oss/terms/expectation_store) A connector to store and retrieve information about collections of verifiable assertions about data.

[**Expectation Suite:**](/docs/oss/terms/expectation_suite) A collection of verifiable assertions about data.

[**Metric:**](/docs/oss/terms/metric) A computed attribute of data such as the mean of a column.

[**MetricProviders:**](/docs/conceptual_guides/metricproviders) Generate and register Metrics to support Expectations, and they are an important part of the Expectation software development kit (SDK).

[**Metric Store:**](/docs/oss/terms/metric_store) A connector to store and retrieve information about computed attributes of data, such as the mean of a column.

[**Plugin:**](/docs/oss/terms/plugin) Extends Great Expectations' components and/or functionality.

[**Renderer:**](/docs/oss/terms/renderer) A method for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.

[**Store:**](/docs/oss/terms/store) A connector to store and retrieve information about metadata in Great Expectations.

[**Supporting Resource:**](/docs/oss/terms/supporting_resource) A resource external to the Great Expectations code base which Great Expectations utilizes.

[**Validation:**](/docs/oss/guides/validation/validate_data_overview) The act of applying an Expectation Suite to a Batch.

[**Validation Result:**](/docs/oss/terms/validation_result) Generated when data is Validated against an Expectation or Expectation Suite.

[**Validation Result Store:**](/docs/oss/terms/validation_result_store) A connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.

[**Validator:**](/docs/oss/terms/validator) Used to run an Expectation Suite against data.

