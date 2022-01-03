---
id: glossary
title: Glossary
---

**[Batch](/docs/terms/batch):** 
A selection of records from a Data Asset.


**[Batch Request](/docs/terms/batch_request):** 
Provided to a Datasource in order toto create a Batch.


**[Checkpoint](/docs/terms/checkpoint):** 
The primary means for validating data in a production deployment of Great Expectations.


**[CLI](/docs/terms/cli):** 
Command Line Interface


**[Custom Expectation](/docs/terms/custom_expectation):** 
An extension of the `Expectation` class, developed outside of the Great Expectations library.


**[Data Asset](/docs/terms/data_asset):** 
A collection of records within a Datasource which is usually named based on the underlying data system and sliced to correspond to a desired specification.


**[Data Connector](/docs/terms/data_connector):** 
Provides the configuration details based on the source data system which are needed by a Datasource to define Data Assets.


**[Data Context](/docs/terms/data_context):** 
The primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.


**[Data Docs](/docs/terms/data_docs):** 
Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.


**[Datasource](/docs/terms/datasource):** 
Provides a standard API for accessing and interacting with data from a wide variety of source systems.


**[Evaluation Parameter](/docs/terms/evaluation_parameter):** 
A parameter used during Validation of an Expectation to reference simple expressions or previousl generated metrics.


**[Evaluation Parameter Store](/docs/terms/evaluation_parameter_store):** 
A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.


**[Execution Engine](/docs/terms/execution_engine):** 
A system capable of processing data to compute Metrics


**[Expectation](/docs/terms/expectation):** 
A verifiable assertion about data.


**[Expectation Store](/docs/terms/expectation_store):** 
A connector to store and retrieve information about collections of verifiable assertions about data.


**[Metric](/docs/terms/metric):** 
A computed attribute of data such as the mean of a column.


**[Metric Store](/docs/terms/metric_store):** 
A connector to store and retrieve information about computed attributes of data, such as the mean of a column.


**Profiler:** 
Generates candidate Expectations (and sometimes other artifacts) from data.


**Renderer:** 
A class for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.


**Store:** 
A connector to store and retrieve information about metadata in Great Expectations.


**Supporting Resource:** 
A resource external to the Great Expectations code base which Great Expectations utilizes.


**Validation:** 
The act of applying an Expectation Suite to a Batch.


**Validation Action:** 
A Python class with a run method that takes the result of validating a Batch against an Expectation Suite and does something with it


**Validation Operator:** 
An internal object that can trigger Validation Actions after Validation completes.


**Validation Result:** 
An object generated when data is Validated against an Expectation Suite.


**Validation Result Store:** 
A connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.


**Validator:** 
The object responsible for running an Expectation Suite against data.
