---
id: glossary
title: "Glossary of Terms"
---

[**Action:**](./terms/action) A Python class with a run method that takes a Validation Result and does something with it

[**Batch:**](./terms/batch) A selection of records from a Data Asset.

[**Batch Request:**](./terms/batch_request) Provided to a Datasource in order to create a Batch.

[**CLI:**](./terms/cli) Command Line Interface

[**Checkpoint:**](./terms/checkpoint) The primary means for validating data in a production deployment of Great Expectations.

[**Checkpoint Store:**](./terms/checkpoint_store) A connector to store and retrieve information about means for validating data in a production deployment of Great Expectations.

[**Custom Expectation:**](./terms/custom_expectation) An extension of the `Expectation` class, developed outside of the Great Expectations library.

[**Data Asset:**](./terms/data_asset) A collection of records within a Datasource which is usually named based on the underlying data system and sliced to correspond to a desired specification.

[**Data Connector:**](./terms/data_connector) Provides the configuration details based on the source data system which are needed by a Datasource to define Data Assets.

[**Data Context:**](./terms/data_context) The primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.

[**Data Docs:**](./terms/data_docs) Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[**Data Docs Store:**](./terms/data_docs_store) A connector to store and retrieve information pertaining to Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[**Datasource:**](./terms/datasource) Provides a standard API for accessing and interacting with data from a wide variety of source systems.

[**Evaluation Parameter:**](./terms/evaluation_parameter) A dynamic value used during Validation of an Expectation which is populated by evaluating simple expressions or by referencing previously generated metrics.

[**Evaluation Parameter Store:**](./terms/evaluation_parameter_store) A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.

[**Execution Engine:**](./terms/execution_engine) A system capable of processing data to compute Metrics.

[**Expectation:**](./terms/expectation) A verifiable assertion about data.

[**Expectation Store:**](./terms/expectation_store) A connector to store and retrieve information about collections of verifiable assertions about data.

[**Expectation Suite:**](./terms/expectation_suite) A collection of verifiable assertions about data.

[**Metric:**](./terms/metric) A computed attribute of data such as the mean of a column.

[**Metric Store:**](./terms/metric_store) A connector to store and retrieve information about computed attributes of data, such as the mean of a column.

[**Plugin:**](./terms/plugin) Extends Great Expectations' components and/or functionality.

[**Profiler:**](./terms/profiler) Generates Metrics and candidate Expectations from data.

[**Profiling:**](./terms/profiler) The act of generating Metrics and candidate Expectations from data.

[**Renderer:**](./terms/renderer) A method for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.

[**Store:**](./terms/store) A connector to store and retrieve information about metadata in Great Expectations.

[**Supporting Resource:**](./terms/supporting_resource) A resource external to the Great Expectations code base which Great Expectations utilizes.

[**Validation:**](./guides/validation/validate_data_overview) The act of applying an Expectation Suite to a Batch.

[**Validation Result:**](./terms/validation_result) Generated when data is Validated against an Expectation or Expectation Suite.

[**Validation Result Store:**](./terms/validation_result_store) A connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.

[**Validator:**](./terms/validator) Used to run an Expectation Suite against data.

