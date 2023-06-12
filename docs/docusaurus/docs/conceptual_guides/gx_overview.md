---
title: Get to know Great Expectations
---

import PythonVersion from '/docs/components/_python_version.mdx';

Great Expectations (GX) is a framework for defining and running Expectations against your data. Similar to assertions in traditional Python unit tests, Expectations provide a flexible, declarative language for describing expected behaviours. Unlike traditional unit tests which describe the expected behaviour of code given a specific input, Expectations are applied to the input data itself. For example, you can define an Expectation that a column contains no null values. When GX runs that Expectation on your data it generates a report which indicates if a null value was found.


## Source Data

GX uses the term Source Data when referring to data in its original format, and the term Source Data System when referring to the storage location for Source Data. No matter the format of your Source Data, or where your Source Data System is hosted, you can use GX to ensure the quality and consistency of your data.

## Get started with Great Expectations

Working with GX for the first time can be broken down into four steps.  First, you will install GX along with any dependencies specific to your Source Data.  Second, you will connect your Source Data System to GX.  Next, you will define your Expectations for the connected data. Finally, you will use GX to Validate those Expectations.

Installation and Setup (Install GX, create a Data Context, configure Stores, Data Docs, and Credentials)-> Connect to data (Connect to Source Data with Datasources, Add Data Assets to describe sets of records) -> Create Expectations -> Validate data (Retrieve data with a Batch Request, retrieve Expectations from an Expectations Store, define a checkpoint, run the checkpoint)

### Install GX

GX is a library for <PythonVersion />, and you can use pip to install it. If you're using a Source Data format such as SQL or hosting your Source Data in an environment such as AWS, special pip commands are provided.  These bundle this installation of GX with the installation of additional Python dependencies used to access specific data formats and hosting environments.

<!-- To install GX and additional dependencies, see Installation guides. -->

### Configure your Data Context

A GX project is defined by the configurations in a Data Context.  In Python, the Data Context object also serves as your entry point for the GX API, providing convenience methods to further configure and interact with GX.  Your Data Context manages various classes and limits the objects you need to directly manage to get GX working on your data.

The configurations in your Data Context will consist of Stores, Credentials, and Data Docs.  Store configurations describe locations where metadata information used by GX resides.  Credentials for Source Data Systems that require them can be provided in a special Store and accessed through string substitution so that you do not have to include them in plain text configurations elsewhere.  Data Docs configurations tell GX where to produce the human-readable output that is generated when GX validates data.  All of this information will be contained within and accessible through your Data Context.

The following are the available Data Context types:
- Ephemeral Data Context: Exists in memory, and does not persist beyond the current Python session.
- Filesystem Data Context: Exists as a folder and configuration files.  Its contents persist between Python sessions.
- Cloud Data Context: Supports persistence between Python sessions, but additionally serves as the entry point for Great Expectations Cloud.

<!-- To initialize and instantiate a Data Context, see Setting up a GX environment. -->

#### Optional configurations for metadata Stores and Data Docs

Part of setting up a GX project includes defining where you want any persisting information used by GX to reside, if the default locations do not suit your needs.  This information primarily consists of configurations for Stores and Data Docs. 

Stores are connectors to the metadata GX uses, such as:
- The Datasource and Data Asset configurations that tell GX how to connect to your Source Data System.
- The Expectations you have specified about your Source Data.
- The Metrics that are recorded when GX validates certain Expectations against your Source Data.
- The Checkpoints you have configured for validating your Expectations.
- The Validation Results that GX returns when you run a Checkpoint.
- Credentials, which can be stored in a special file that GX references for string substitutions in other configurations.  Credentials can also be stored as environment variables outside of GX.

A Data Context includes default configurations for your Store locations.  The default location of Stores in a Filesystem or Cloud Data Context is nested within the same folder hierarchy that contains the Data Context itself.  An Ephemeral Data Context will default to keeping your Stores in memory.  You can update these defaults to specify other storage locations, such as hosting them in a shared cloud environment or external folders.

<!-- To configure Stores, see Metadata Stores -->

Data Docs are human-readable documentation about your Expectations and Validation Results.  They exist as webpages, and your Data Context includes a default configuration for building them locally for Filesystem Data Contexts, in memory for Ephemeral Data Contexts, and online for Cloud Data Contexts.  You can configure where your Data Docs are hosted.  You can also add configurations for multiple Data Docs sites, and specify what information each Data Docs site provides.

<!-- To configure Data Docs, see Data Docs. -->


### GX connects to your Source Data System

Your Source Data System could be CSV files in a folder, a PostgreSQL database hosted on AWS, or any combination of numerous formats and environments.  Regardless of your Source Data's format and where it resides, GX provides a unified API for working with it.  To implement this API you define Datasources and Data Assets in GX.  These configurations tell GX how to connect to your Source Data.

Datasources connect GX to your Source Data System.  Regardless of the original format of your Source Data, a Datasource lets you access it.

Data Assets are collections of records within a Datasource.  You can further partition these collections of records into subsets of data that GX calls Batches.  For instance, you could define a Data Asset for a SQL Datasource as the selection of all records from last year in a given table.  You could then partition that Data Asset into Batches of data that correspond to the records for individual months of the year.

A Batch Request is used to specify one or more Batches within a Data Asset.  This provides flexibility in how you work with the data described by a single Data Asset.  For instance, GX can automate the process of running statistical analyzes for multiple Batches of data.  This is possible because you can provide a Batch Request that corresponds to multiple Batches in a Data Asset.  Alternatively, you can specify a single Batch from that same Data Asset so that you do not need to re-run the analyzes on all of your data when you are only interested in a single subset.

In the example of a Data Asset that has been split into months, this would let you build a statistical model off of each month of your existing data.  Then, after an update, you could specify that you only wanted to run your analysis on the most recent month's data.

<!-- For more information about Datasources and Data Assets, see Connecting to data. -->


### GX lets you describe your Source Data with Expectations

An Expectation is a verifiable assertion about Source Data.  Expectations enhance communication about your data by describing the state it should conform to in a way that can be referenced by anyone with access to them.  They also improve quality for data applications by giving you a verifiable standard to measure your data against.  They help you take the implicit assumptions about your data and make them explicit.  Having defined Expectations helps reduce the need to consult with domain experts about uncertainties and lets you avoid leaving insights about data in isolated silos by explicitly stating that information in a way that others can reference.

Expectations can be built off of the domain knowledge of subject matter experts, interactively while introspecting a sample set of data, or by using one of GX's powerful tools: the Data Assistant.  A Data Assistant is a utility that automates the process of building Expectations by asking questions about your data, gathering information to describe what is observed, and then presenting Metrics and proposed Expectations based on the answers it has determined.

GX comes with a built-in library of more than 50 common Expectations, including:
- `expect_column_values_to_not_be_null`
- `expect_column_values_to_match_regex`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_match_strftime_format`
- `expect_table_row_count_to_be_between`
- `expect_column_median_to_be_between`

You can create custom Expectations when you need something more specialized for your data validation.  You can also use custom Expectations contributed by other GX community members by installing them as Plugins.

For a list of available Expectations, see [the Expectation Gallery](https://greatexpectations.io/expectations/).

<!-- To define Expectations, see Creating Expectations. -->

### Data Validation with GX

A Checkpoint is the primary means for validating data in a production deployment of GX.  Checkpoints provide an abstraction for bundling a Batch (or Batches) of data with an Expectation Suite (or several), and then running those Expectations against the paired data.

One of the most powerful features of Checkpoints is that you can configure them to run Actions. The Validation Results generated when a Checkpoint runs determine what Actions are performed. Typical use cases include sending email, Slack, or custom notifications. Another common use case is updating Data Docs sites. Actions can be used to do anything you are capable of programming in Python. Actions are a versatile tool for integrating Checkpoints in your pipeline's workflow.

<!-- To use Checkpoints to validate your data, see Validating data. -->
<!-- To integrate GX with your processes, see Including GX in a data pipeline. -->

#### Validation Results

The Validation Results returned by GX tell you how your data corresponds to what you expected of it.  You can view this information in the Data Docs that are configured in your Data Context.  Evaluating your Validation Results helps you identify issues with your data.  If the Validation Results show that your data meets your Expectations, you can confidently use it.

If your data fails to meet your Expectations, your data might have a quality issue that you need to address, or you might need to modify your Expectations to correspond to an unexpected reality reflected in your data.  After refining your data or your Expectations you can reuse the Checkpoint you created to Validate your data.

## The iterative GX workflow

Ensuring data quality is an ongoing process.  When new data comes in or existing data is updated, GX lets you reuse your existing Expectation Suites and Checkpoints to validate the new or updated Source Data.  This lets you maintain records of your data's adherence to Expectations and track trends in the quality of the validated data.  These records can help you determine if failed Expectations are due to outliers in new data, a more systemic problem, or the need to update your understanding of the data you are working with.

This cycle of data validation is known as the iterative GX workflow.

 Validate Data -> Evaluate Validation Results (Accept data, improve data, or refine Expectations) -> Iterate (after you add new Datasources or update the data in existing ones)

You can use Validation Results to reject bad data, or to refine it and improve its quality to meet your standards.  Alternatively, you can use Validation Results to refine your Expectations to match your improved understanding of how your data has changed.

<!-- For more details, see our guide on using GX iteratively. -->
