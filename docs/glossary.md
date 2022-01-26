---
id: glossary
title: "Glossary of Terms"
---
**Batch:** A selection of records from a Data Asset.

**Batch Request:** Provided to a Datasource in order to create a Batch.

**CLI:** Command Line Interface

**Catalog Asset:** A collection of records within a Datasource which is usually named based on the underlying data system and sliced in different ways to correspond to multiple desired specifications.

**Checkpoint:** The primary means for validating data in a production deployment of Great Expectations.

**Custom Expectation:** An extension of the `Expectation` class, developed outside of the Great Expectations library.

**Data Asset:** A collection of records within a Datasource which is usually named based on the underlying data system and sliced to correspond to a desired specification.

**Data Connector:** Provides the configuration details based on the source data system which are needed by a Datasource to define Data Assets.

**Data Context:** The primary entry point for a Great Expectations deployment, with configurations and methods for all supporting components.

**Data Docs:** Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

**Datasource:** Provides a standard API for accessing and interacting with data from a wide variety of source systems.

**Evaluation Parameter:** A parameter used during Validation of an Expectation to reference simple expressions or previously generated metrics.

**Evaluation Parameter Store:** A connector to store and retrieve information about parameters used during Validation of an Expectation which reference simple expressions or previously generated metrics.

**Execution Engine:** A system capable of processing data to compute Metrics.

**Expectation:** A verifiable assertion about data.

**Expectation Store:** A connector to store and retrieve information about collections of verifiable assertions about data.

**Expectation Suite:** A collection of verifiable assertions about data.

**Metric:** A computed attribute of data such as the mean of a column.

**Metric Store:** A connector to store and retrieve information about computed attributes of data, such as the mean of a column.

**Profiler:** Generates Metrics and candidate Expectations from data.

**Renderer:** A method for converting Expectations, Validation Results, etc. into Data Docs or other output such as email notifications or slack messages.

**Store:** A connector to store and retrieve information about metadata in Great Expectations.

**Supporting Resource:** A resource external to the Great Expectations code base which Great Expectations utilizes.

**Validation:** The act of applying an Expectation Suite to a Batch.

**Validation Action:** A Python class with a run method that takes the result of validating a Batch against an Expectation Suite and does something with it

**Validation Operator:** Triggers Validation Actions after Validation completes.

**Validation Result:** Generated when data is Validated against an Expectation Suite.

**Validation Result Store:** A connector to store and retrieve information about objects generated when data is Validated against an Expectation Suite.

**Validator:** Used to run an Expectation Suite against data.

