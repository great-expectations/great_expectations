---
title: Great Expectations overview
---

This overview is for new users of Great Expectations (GX) and those looking for an understanding of its components and its primary workflows. It does not require an in-depth understanding of GX code, and is an ideal place to start before moving to more advanced topics, or if you want a better understanding of GX functionality.

## What is GX

GX is a framework for describing data using expressive tests and then validating that the data meets those criteria.

## GX core components

GX is built around the following five core components:

- **[Data Sources:](#data-sources)** Connect to your data, and organize data for testing.
- **[Expectations:](#expectations)** Identify the standards to which your data should conform.
- **[Validation Definitions:](#validation-definitions)** Link a set of Expectations to a specific set of data.
- **[Checkpoints:](#checkpoints)** Facilitate the integration of GX into data pipelines by allowing you to run automated actions based on the results of validations.
- **[Data Context:](#data-context)** Manages the settings and metadata for a GX project, and provides an entry point to the GX Python API.


## Data Sources

Data Sources connect GX to data such as CSV files in a folder, a PostgreSQL database hosted on AWS, or any combination of data formats and environments. Regardless of the format of your Data Asset or where it resides, Data Sources provide GX with a unified API for working with it.

### Data Assets

Data Assets are collections of records within a Data Source, like tables in a database or files in a cloud storage bucket.  A Data Source tells GX how to connect to your data and Data Assets tell GX how to organize that data.

Data Assets should be defined in a way that makes sense for your data and your use case. For instance, you could define a Data Asset based on a SQL view that joins multiple tables or selects a subset of a table, such as all of the records with a given status in a specific field. 

### Batches

All validation in GX is performed on Batches of data. You can validate the entire data asset as a single batch, or you can partition the data asset into multiple batches and validate each one separately. 

### Batch Definitions

A Batch Definition tells GX how to organize the records in a Data Asset into Batches for retrieval. For example, if a table is updated with new records each day, you could define each day's worth of data as a different Batch. Batch Definitions allow you to retrieve a specific Batch based on parameters provided at runtime.

Multiple Batch Definitions can be added to a Data Asset.  That feature allows you to apply different Expectations to different subsets of the same data.  For instance, you could define one Batch Definition that returns all the records within a Data Asset.  You might then configure a second Batch Definition to only return the most recent day's records.  And you could also create a Batch Definition that returns all the records for a given year and month which you only specify at runtime in a script.

## Expectations

An Expectation is a verifiable assertion about data.  Similar to assertions in traditional Python unit tests, Expectations provide a flexible, declarative language for describing expected behaviors. Unlike traditional unit tests which describe the expected behavior of code given a specific input, Expectations apply to the input data itself. For example, you can define an Expectation that a column contains no null values. When GX runs that Expectation on your data it generates a report which indicates if a null value was found.

Expectations can be built directly from the domain knowledge of subject matter experts, interactively while introspecting a set of data, or through automated tools provided by GX.

For a list of available Expectations, see [the Expectation Gallery](https://greatexpectations.io/expectations/).

### Expectation Suites

Expectation Suites are collections of Expectations describing your data.  When GX validates data, an Expectation Suite helps streamline the process by running all the contained Expectations against that data.

You can define multiple Expectation Suites for the same data to cover different use cases, and you can apply the same Expectation Suite to different Data Assets.

## Validation Definitions

Validation Definitions tell GX what Expectations to apply to specific data for validation.  It connects a Data Asset's Batch Definition to a specific Expectation Suite.

Because an Expectation Suite is decoupled from a specific source of data, you can apply the same Expectation Suite against different data by reusing it in different Validation Definitions.

The same holds true for Batch Definitions: because they are decoupled from a specific Expectation Suite, you can run multiple Expectation Suites against the same Batch of data by reusing the Batch Definition in different Validation Definitions.  As an example, you could have one Validation Definition that links a permissive Expectation Suite to a Batch Definition.  Then you could have a second Validation Definition that links a more strict Expectation Suite to that same Batch Batch Definition to verify different quality parameters.

In Python, Validation Definition objects also provide the API for running their defined validation and returning Validation Results.

### Validation Results

The Validation Results returned by GX tell you how your data corresponds to what you expected of it. You can view this information in the Data Docs that are configured in your Data Context. Evaluating your Validation Results helps you identify issues with your data. If the Validation Results show that your data meets your Expectations, you can confidently use it.

## Checkpoints

A Checkpoint is the primary means for validating data in a production deployment of GX. Checkpoints allow you to run a list of Validation Definitions with shared parameters and then pass the Validation Results to a list of automated Actions.

### Actions

One of the most powerful features of Checkpoints is that you can configure them to run Actions. The Validation Results generated when a Checkpoint runs determine what Actions are performed. Typical use cases include sending email, Slack messages, or custom notifications. Another common use case is updating Data Docs sites. Actions can be used to do anything you are capable of programming in Python. Actions are a versatile tool for integrating Checkpoints in your pipeline's workflow.

## Data Context

A Data Context manages the settings and metadata for a GX project.  In Python, the Data Context object serves as the entry point for the GX API and manages various classes to limit the objects you need to directly manage yourself.  A Data Context contains all the metadata used by GX, the configurations for GX objects, and the output from validating data.

The following are the available Data Context types:
- **Ephemeral Data Context:** Exists in memory, and does not persist beyond the current Python session.
- **File Data Context:** Exists as a folder and configuration files. Its contents persist between Python sessions.
- **Cloud Data Context:** Supports persistence between Python sessions, but additionally serves as the entry point for GX Cloud.

### The GX API

A Data Context object in Python provides methods for configuring and interacting with GX.  These methods and the objects and additional methods accessed through them compose the GX public API.

For more information, see [The GX API reference](/reference/api_reference.md).

### Stores

Stores contain the metadata GX uses.  This includes configurations for GX objects, information that is recorded when GX validates data, and credentials used for accessing data sources or remote environments.  GX utilizes one Store for each type of metadata, and the Data Context contains the settings that tell GX where that Store should reside and how to access it.

### Data Docs

Data Docs are human-readable documentation generated by GX.  Data Docs describe the standards that you expect your data to conform to, and the results of validating your data against those standards.  The Data Context manages the storage and retrieval of this information.

You can configure where your Data Docs are hosted.  Unlike Stores, you can define configurations for multiple Data Docs sites.  You can also specify what information each Data Doc site provides, allowing you to format and provide different Data Docs for different use cases.