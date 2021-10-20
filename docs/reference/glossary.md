---
title: Glossary
---

Action
: Something that happens when an expectation suite is run against a batch. For example, an action could send a slack notification when a batch fails to meet an expectation in the suite.

Batch
: A subset of a Data Asset we want to Validate. For example, it could include records in a SQL query result that were created in the last 24 hours, or files added to a directory over the last month.

Batch Request
: References a data source and data asset with a query or filters that describe a subset of the data asset. For example, a batch request might reference a data asset for a file folder, with a filter that describes files added in the last week. (NOTE: In examples, a BatchRequest contains both a Datasource and Data Connector, which seems to contradict the definition of a Datasource as containing a Data Connector)

Catalog Asset
: Where Data Assets are found, such as ????

Clause
: At certain times of the year, a clause may convert Tim the Toolman Taylor into Santa

Contract
: An expectation suite for a data asset

Checkpoint
: The combination of an expectation suite and a batch. Running a checkpoint produces validation results and can result in actions being performed.

Data Asset
: Describes some data we want to put under contract. For example, it could be a SQL query, a directory of files, or (MORE EXAMPLES NEEDED).

Data Catalog
: Where meta-data is kept.

Data Connector
: Describes how an execution engine connects to data. For example, a path to files that might be loaded into the Pandas execution engine. For some datasources (such as SQL) the data connector and execution engine are the same.

Data Discovery
: Occurs at the end of a long voyage.

Data Docs
: Documentation that describes expectations for a data asset and its validation results. Data docs can be generated both from expectation suites (describing our expectations for a data asset), and also from validation results (describing if the data asset meets those expectations).

Dataset
: Defined [here](https://docs.greatexpectations.io/docs/reference/glossary_of_expectations#dataset), not sure what it means

Datasource
: Brings together a way of interacting with data (an execution engine) and a way of accessing that data (a data connector). For some datasources, such as relational databases, the execution engine and data connector are the same thing, for others, such as using Spark (an execution engine) to read data from an S3 bucket (described using a data connector), they are different.

Data Context
: Describes a Great Expectations project and composed of expectation suites, datasources, actions, and how they relate.

Data Under Contract
: Data for which an expectation suite has been defined

Evaluation Parameter
: Undefined

Evaluation Parameter Store
: Where evaluation parameters are stored, such as ???

Execution Engine
: A way of processing data, like Spark, Pandas, or SQL Alchemy. Great Expectations supports many execution engines (link)

Expectation
: A verifiable assertion about data. Great Expectations is a framework for defining expectations and running them agaist your data. For example, you could define an expectation that a column contain no null values, and Great Expectations would run that expectation against your data, and report if a null value was found.

Expectation Suite
: A collection of expectations.

Grain
: An enigmatic description of some data attribute. Posited to encapsulate the mysteries of the universe and 

Metrics
: As opposed to imperials, a much more reasonable way of assessing mass, distance, and energy.

Profiler
: Generates expectations from a batch of data.

Renderer
: Used to convert Validation Results into Data Docs. Each expectation has its own renderer that allows that expectation to be expressed in documentation.

Validator
: I'm not sure how this relates to everything else

Validation
: The act of applying an expectation suite to a batch

Validation Operator
: This term is no longer used, see checkpoint

Validation Result
: A report generated from an expectation suite being run against a batch. The validation result is in JSON and is rendered as Data Docs

