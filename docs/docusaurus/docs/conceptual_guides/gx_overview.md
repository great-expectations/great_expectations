---
title: Great Expectations
---

import PythonVersion from '/docs/components/_python_version.mdx';

Great Expectations is a framework for defining Expectations and running them against your data. Like assertions in traditional Python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, GX applies Expectations to data instead of code. For example, you could define an Expectation that a column contains no null values, and GX would run that Expectation against your data, and report if a null value was found.

Before you've even installed GX, working with GX starts with data.  Specifically, data you care about enough for quality to be important.  We call your data, in its original format, your **Source Data**.  The combination of your data in its original format and the location at which it is stored is your **Source Data System**.

Your Source Data System could be CSV files in a folder, or a PostgreSQL database hosted on AWS, or any combination of numerous formats and environments.  Regardless of what your Source Data System is or where it resides, your next step in assuring its quality is implementing GX.

---

## Getting started with Great Expectations

You have data.  You care about your data.  You've made the wise decision to use Great Expectations to catch potential quality concerns with your data.  So the next thing you need to know is: _What next?_

Getting started with GX can be broken down into four steps.  First, you will install GX and create a GX project.  Next, you will connect that project to your Source Data System.  Third, you will define your Expectations for the connected data. Finally, you will use GX to Validate those Expectations.

Installation and Setup -> Connect to data -> Create Expectations -> Validate data

## Install Great Expectations

GX is a library for <PythonVersion />, and can be installed using `pip`, a library installer bundled with Python installations.  Depending on your Source Data System, you may need to install GX with some additional dependencies (generally for SQL Source Data Systems, or environments like AWS).  We streamline this process by providing special `pip` commands for specific combinations of additional dependencies.

For details on how to install GX and how to include any additional dependencies for your Source Data System, see [our installation guides](landing page: Setting up a GX environment->Installation and Dependencies).


## Set up your GX Data Context

A Great Expectations project stores its configurations in a **Data Context**.  In Python, the Data Context object also serves as your entry point for the GX API, allowing you to further configure and interact with GX.  Your Data Context manages various classes and helps limit the objects you need to directly manage to get GX working on your data.

GX provides three types of Data Contexts:
- An **Ephemeral Data Context**: This Data Context exists in memory, and will not persist beyond the current Python session.
- A **Filesystem Data Context**: This Data Context exists as a folder and configuration files, allowing its contents to persist between Python sessions.
- A **Cloud Data Context**: This Data Context also supports persistence between Python sessions, but additionally serves as the entry point for GX Cloud.

<!-- For details on how to initialize and instantiate a Data Context, see [our Data Context guides](landing page: Setting up a GX environment->Data Contexts). -->


### (Optional) Configure your metadata Stores and Data Docs

**Stores** are connectors to the metadata GX uses.  This includes things like:
- The Datasource and Data Asset configurations that tell GX how to connect to your Source Data System.
- The Expectations you have specified about your Source Data.
- The Metrics that are recorded when GX validates certain Expectations against data.
- The Checkpoints you have configured for validating your Expectations.
- The Validation Results that GX returns when you run a Checkpoint.

A GX Data Context comes with default configurations in place for your Stores.  By default, these exist in the same folder hierarchy as your Data Context (or in memory, if you are using an Ephemeral Data Context).  However, you can update these if you want to change where your Store configurations exist, such as hosting them in a shared cloud environment like AWS.

<!-- For more information on configuring Stores, see [our guides on metadata Stores](landing page: setting up a GX environment->Metadata Stores). -->

**Data Docs** are human-readable documentation about your Expectations and Validation Results that GX generates for you.  They exist as webpages, and like Stores your Data Context will include a default configuration for building them locally for Filesystem Data Contexts, in memory for Ephemeral Data Contexts, and online for Cloud Data Contexts.  As with Stores, you can configure where your Data Docs are hosted.  You can also add configurations for multiple Data Docs sites, and specify what information each Data Docs site will present to its viewers.

<!-- For more information on configuring Data Docs, see our [Data Docs guides](landing page:Setting up a GX environment->Data Docs). -->


## Connect GX to your Source Data System

**Datasources** provide the interface between GX and your Source Data System.  Regardless of the original format of your Source Data, a Datasource will provide you a clear and consistent API for accessing that data in GX.

**Data Assets** are collections of records within a Datasource.  These collections can be further partitioned to a desired specification.  We call these subsets of data **Batches**.  For instance, you could define a Data Asset for a SQL Datasource as the selection of all records from last year in a given table.  Then you could further partition that Data Asset into Batches of data corresponding to the records for individual months of that year.

How you organize your Datasources into Data Assets and your Data Assets into Batches will impact how you can analyze them with GX.  Defining multiple Batches in a Data Asset will let GX automate the process of running statistical analyzes for each Batch.  It will also let you specify an individual Batch to work with.  In the example of a Data Asset that has been split into months, this would let you build a statistical model off of each month of your existing data.  Then, after an update, you could specify that you only wanted to run your analysis on the most recent month's data.

<!-- For more information on Datasources and Data Assets, see our [Connecting to data guides](Landing page: connecting to data). -->


## Define your Expectations

An **Expectation** is a verifiable assertion about data.  Expectations enhance communication about your data by describing the state it should conform to.  They also improve quality for data applications by giving you a verifiable standard to measure your data against.  They help you take the implicit assumptions about your data and make them explicit.  Having defined Expectations helps reduce the need to consult with domain experts about uncertainties and lets you avoid leaving insights about data in isolated silos.

Expectations can be built off of the domain knowledge of subject matter experts, interactively while introspecting a sample set of data, or by using one of GX's powerful tools: the Data Assistant.  A **Data Assistant** is a utility that automates the process of building Expectations by asking questions about your data, gathering information to describe what is observed, and then presenting Metrics and proposed Expectations based on the answers it has determined.

GX's built-in library includes more than 50 common Expectations, such as:
- `expect_column_values_to_not_be_null`
- `expect_column_values_to_match_regex`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_match_strftime_format`
- `expect_table_row_count_to_be_between`
- `expect_column_median_to_be_between`

GX also allows you to create custom Expectations, should you need something more specialized than what is available.  You can even use custom Expectations contributed by other GX community members by installing those Expectations as Plugins.

For a full list of available Expectations, see [the Expectation Gallery](https://greatexpectations.io/expectations/).

<!-- For details on how to define Expectations, see our [Creating Expectations guides](landing page: Creating Expectations). -->

## Validate your data

A **Checkpoint** is the primary means for validating data in a production deployment of GX.  Checkpoints provide an abstraction for bundling a Batch (or Batches) of data with an Expectation Suite (or several), and then running those Expectations against the paired data.  

One of the most powerful features of Checkpoints is that they can be configured to run **Actions**, which will do some process based on the Validation Results generated when a Checkpoint is run. Typical uses include sending email, slack, or custom notifications. Another common use case is updating Data Docs sites. However, Actions can be created to do anything you are capable of programing in Python. This gives you an incredibly versatile tool for integrating Checkpoints in your pipeline's workflow!

<!-- For more details on using Checkpoints to validate your data, see our [Validating data guides](landing page: validating data). -->
<!-- For more details on integrating GX with your processes, see our [guide on including GX in a data pipeline](). -->

### Evaluate Validation Results

The **Validation Results** returned by GX function as a report on how well your data actually corresponds to what you expected of it.  You will be able to view this information in the Data Docs that are configured in your Data Context.  Evaluating your Validation Results will help you spot issues with your data.  You can then use that knowledge to decide how to proceed.  If the Validation Results show that your data meets your Expectations, you can confidently put it to use.

If your data _fails_ to meet your Expectations, then you may determine that your data has a quality issue you need to address.  Or you may realize that your Expectations need to be refined to correspond to an unexpected reality reflected in your data.  After refining your data (or your Expectations) you can reuse the Checkpoint you had created to Validate your data once more.

## The iterative GX workflow

Ensuring data quality is an ongoing process.  When new data comes in or existing data is updated, GX lets you re-use your existing Expectation Suites and Checkpoints to validate the new or updated Source Data.  This permits you to maintain records of your data's adherence to Expectations and track trends in the quality of the validated data.  These records can help you determine if failed Expectations are due to outliers in new data, a more systemic problem, or the need to update your understanding of the data you are working with.

We call this cycle of validating data the iterative GX workflow.

 Validate Data -> Evaluate Validation Results (Accept data, improve data, or refine Expectations) -> Iterate (after you add new Datasources or update the data in existing ones)

Whether Validation Results reveal that your data continues to meet defined expectations, that a problem exists with the quality of your data, or that your previous understanding of your data hasn't caught up with something that has changed: now you know!

You can use this knowledge to reject bad data, or to refine it and improve its quality to meet your standards.  Or you may use this knowledge to refine your Expectations to match your improved understanding of how your data has changed.  All of these uses are valuable parts of maintaining data quality over time.

<!-- For more details, see our guide on using GX iteratively. -->
