.. _reference__core_concepts:


#####################################
Core Concepts
#####################################

Great Expectations is all about helping you understand data better, so you can communicate with your team and others about what you've built and what you expect. Great Expectations delivers three key features: **expectations validate data quality**, **tests are docs, and docs are tests**, and **automatic profiling of data**. This guide helps you understand *how* Great Expectations does that by describing the core concepts used in the tool. The guide aims for precision, which can sometimes make it a bit dense, so we include examples of common use cases to help build intuition.

Below, you'll find a brief introduction to the big ideas you'll need to understand how Great Expectations works, a more detailed definition of each of the core concepts (focused around how they're represented in the codebase), information on some of the defining design decisions in the tool, and links to more detailed documentation on concepts.


.. contents:: :depth: 2

************************************************
Key Ideas
************************************************

If you only have time to remember a few key ideas about Great Expectations, make them these:

It all starts with ``Expectations``. An Expectation is how we communicate the way data *should* appear. It's also how Profilers communicate what they *learn* about data, and what Data Docs uses to *describe* data or *diagnose* problems. When lots of Expectations are grouped together to define a kind of data asset, like "monthly taxi rides", we call it an ``Expectation Suite``.

``Datasources`` are the first thing you'll need to configure to use Great Expectations. A Datasource brings together a way of interacting with data (like a database or spark cluster) and some specific data (a description of that taxi ride data for last month). With a Datasource, you can get a Batch of data or a Validator that can evaluate expectations on data.

When you're deploying Great Expectations, you'll use a ``Checkpoint`` to run a validation, testing whether data meets expectations, and potentially performing other actions like building and saving a Data Docs site, sending a notification, or signaling a pipeline runner.

Great Expectations makes it possible to maintain state about data pipelines using ``Stores``. A Store is a generalized way of keeping Great Expectations objects, like Expectation Suites, Validation Results, Metrics, Checkpoints, or even Data Docs sites. Stores, and other configuration, is managed using a ``Data Context``. The Data Context configuration is usually stored as a yaml file or declared in your pipeline directly, and you should commit the configuration to version control to share it with your team.


************************************************
Concepts in the Codebase
************************************************

This section describes the foundational concepts used to integrate Great Expectations into your code. It is a glossary of the main concepts and classes you will encounter while using Great Expectations.

.. _reference__core_concepts__expectations:

Expectations and Metrics
=============================

An **Expectation** is a declarative statements that a computer can evaluate, and that is semantically meaningful to humans, like ``expect_column_values_to_be_unique`` or ``expect_column_mean_to_be_between``.  Expectations are implemented as classes that provide a rich interface to the rest of the library to support validation, profiling, and translation. Those implementations provide a ``_validate`` method that determines whether the Expectation is satisfied by some data, and other methods to support the Great Expectations features.

A **Metric** is any observable property of data. Expectations are usually evaluated using metrics. A metric could be something simple, like a single statistic (for example the mean of a column), or something more complicated, like the histogram of observed values over some distribution. A ``MetricProvider`` provides the critical translation layer between an individual metric and computing infrastructure that can compute it, like a database or local Pandas DataFrame.

An **Expectation Configuration** describes a specific Expectations for data. It combines an Expectation and specific parameters to make it possible to evaluate whether the expectation is true for some specific data. For example, they might provide expected values for a column or the name of a column whose values should be unique. See :ref:`domain and success keys <reference__core_concepts__expectations__domain_and_success_keys>` for more information about how an Expectation Configuration works.

.. _reference__core_concepts__expectations__expectation_suites:

An **Expectation Suite** combines multiple Expectation Configurations into an overall description of a Data Asset. Expectation Suites should have names corresponding to the kind of data they define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/expectations/expectations.rst
   /reference/core_concepts/metrics.rst
   /reference/core_concepts/expectations/expectation_suite_operations.rst
   /reference/core_concepts/expectations/distributional_expectations.rst
   /reference/core_concepts/expectations/standard_arguments.rst
   /reference/core_concepts/expectations/result_format.rst
   /reference/core_concepts/expectations/implemented_expectations.rst
   /reference/core_concepts/conditional_expectations.rst

.. _reference__core_concepts__glossary__datasources:

Datasources
=================

A **Datasource** is the primary way that you configure data access in Great Expectations. A Datasource can be a simple metadata layer that adds information about a database you provide to Great Expectations at runtime, or it can handle one or more connections to external datasources, including identifying available data assets and batches.

An **Execution Engine** is configured as part of a datasource. The Execution Engine provides the computing resources that will be used to actually perform validation. Great Expectations can take advantage of many different Execution Engines, such as Pandas, Spark, or SqlAlchemy, and can translate the same Expectations to validate data using different engines.

A **Data Connector** facilitates access to an external data store, such as a database, filesystem, or cloud storage. The Data Connector can inspect an external data store to *identify available Batches*, *build Batch Definitions using Batch Identifiers*, and *translate Batch Definitions to Execution Engine-specific Batch Specs*. See the :ref:`Data Connectors reference <reference__core_concepts__datasources__data_connector>` for more information including descriptions of Batch Definition and Batch Spec.

A **Batch** is reference to a set of data, with metadata about it. *The Batch is the fundamental building block for accessing data using Great Expectations*, but is not the data itself. Instantiating a Batch does not necessarily "fetch" the data by immediately running a query or pulling data into memory. Instead, think of a Batch as a wrapper that includes the information that you will need to fetch the right data when it’s time to validate.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/datasource.rst
   /reference/core_concepts/dividing_data_assets_into_batches.rst


.. _reference__core_concepts__validation:


Validation
===================

A **Validator** uses an Execution Engine and Expectation Suite to validate whether data meets expectations. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis to build or edit Expectations.

An **Expectation Validation Result** captures the output of checking an expectation against data. It describes whether the data met the expectation, and will usually include Metrics from the data that were used to evaluate the Expectation, such as the percentage of unique values or observed mean.

An **Expectation Suite Validation Result** combines multiple Expectation Validation Results and metadata about the validation into a single report.

A **Checkpoint** facilitates running a validation as well as configurable **Actions** such as updating Data Docs, sending a notification to your team about validation results, or storing a result in a shared S3 bucket. The Checkpoint makes it easy to add Great Expectations to your project by tracking configuration about what data, Expectation Suite, and Actions should be run when.

.. attention::

  Checkpoints are an evolution of the soon-to-be-deprecated Validation Operators.


.. toctree::
   :maxdepth: 2

   /reference/core_concepts/validation.rst
   /reference/core_concepts/checkpoints_and_actions.rst

.. _reference__core_concepts__data_contexts:


Data Context
===================

.. _reference__core_concepts__data_context__data_context:

A **Data Context** manages configuration for your project. The DataContext configuration is usually stored in yaml or code and committed to version control. It provides a simple way to configure resources including Datasources, Checkpoints, Data Docs Sites, and Stores.


A **Store** provides a consistent API to manage access to Expectations, Expectation Suite Validation Results and other Great Expectations assets, making it easy to share resources across a team that uses AWS, Azure, GCP, local storage, or something else entirely.

.. _reference__core_concepts__data_context__stores:

A **Plugin** customizes Great Expectations by making a new python package or module available at runtime.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_context.rst
   /reference/core_concepts/evaluation_parameters.rst


.. _reference__core_concepts__data_docs:

Data Docs
===================

With Great Expectations, your tests are your docs, and your docs are your tests. Data Docs makes it possible to produce clear visual descriptions of what you expect, what you observe, and how they differ.

A **Site Builder** orchestrates the construction of individual pages from raw Great Expectations objects, construction of an index, and storage of resources on a Store Backend.

A **Page Builder** converts core Great Expectations objects, such as Expectation Suite Validation Results, into HTML or JSON documents that can be rendered in environments such as a web browser or Slack, using both a Renderer and a

A **Renderer** converts core Great Expectations objects, such as Expectation Suite Validation Results, into an intermediate JSON-based form that includes the relevant *semantic* translation from Expectations but may not include all required formatting for the final document.

.. attention::

    While Data Docs results are extremely robust, we plan to reorganize the internal API for building data docs in the future to provide a more flexible API for extending functionality. Rendering logic is now handled by individual Expectations themselves, making it much easier to customize how your Expectations are documented.

- An **Expectation Suite Renderer** creates a page that shows what you expect from data. Its language is *prescriptive*, for example translating a fully-configured ``expect_column_values_to_not_be_null`` expectation into the English phrase, "column ``address`` values must not be null, at least 80% of the time."

- A **Validation Result Renderer** produces an overview of the result of validating a batch of data with an Expectation Suite. Its language is *diagnostic*; it shows the difference between observed and expected values.

- A **Descriptive Renderer** details the observed metrics produced from a validation *without comparing them to specific expected values*. Its language is descriptive; it can be a critical part of a data discovery process.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_docs.rst

.. _reference__core_concepts__profiling:

Profilers
===================

A **Profiler** uses an Execution Engine to build a new Expectation Suite. It can use zero, one, or more Batches of data to decide which Expectations to include in the new Suite. It can use other information, like a schema or an idea. A profiler may be used to create basic high-level expectations based on a schema even without data, to create specific Expectations based on team conventions or statistical properties in a dataset, or even to generate Expectation Suites specifically designed to be rendered by a Descriptive Renderer for data discovery.

- For example, a **Suite Builder Profiler** reviews characteristics of a sample Batch of data and proposes candidate expectations to help jumpstart new users to Great Expectations.

- The **Descriptive Profiler** produces Expectation Suites whose Expectations are *always (vacuously) true*. A Descriptive Profiler is not intended to produce Expectation Suites that are useful for production Validation. Instead, its goal is to use Expectations to build a collection of Metrics that are useful for understanding data.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_discovery.rst
   /reference/core_concepts/profilers.rst


*****************************************
Design Decisions
*****************************************

Great Expectations is designed to help you think and communicate clearly about your data. To do that, we need to rely on some specific ideas about *what* we're protecting. You usually do not need to think about these nuances to use Great Expectations, and many users never think about what *exactly* makes a Data Asset or Batch. But we think it can be extremely useful to understand the design decisions that guide Great Expectations.

Great Expectations protects **Data Assets**. A **Data Asset** is a logical collection of records. Great Expectations consumes and creates **metadata about Data Assets**.

- For example, a Data Asset might be *a user table in a database*, *monthly financial data*, *a collection of event log data*, or anything else that your organization uses.

How do you know when a collection of records is *one* Data Asset instead of two Data Assets or when two collections of records are really part of the same Data Asset? In Great Expectations, we think the answer lies in the user. Great Expectations opens insights and enhances communication while protecting against pipeline risks and data risks, but that revolves around a *purpose* in using some data (even if that purpose starts out as "I want to understand what I have here"!).

We recommend that you call a collection of records a Data Asset when you would like to track metadata (and especially, *Expectations*) about it. **A collection of records is a Data Asset when it's worth giving it a name.**

Since the purpose is so important for understanding when a collection of records is a Data Asset, it immediately follows that *Data Assets are not necessarily disjoint*. The same data can be in multiple Data Assets. You may have different Expectations of the same raw data for different purposes or produce documentation tailored to specific analyses and users.

- Similarly, it may useful to describe subsets of a Data Asset as new Data Assets. For example, if we have a Data Asset called the "user table" in our data warehouse, we might also have a different Data Asset called the "Canadian User Table" that includes data only for some users.

Not all records in a Data Asset need to be available at the same time or place. A Data Asset could be built from *streaming data* that is never stored, *incremental deliveries*, *analytic queries*, *incremental updates*, *replacement deliveries*, or from a *one-time* snapshot.

That implies that a Data Asset is a **logical** concept. Not all of the records may be **accessible** at the same time. That highlights a very important and subtle point: no matter where the data comes from originally, Great Expectations validates **batches** of data. A **batch** is a discrete subset of a Data Asset that can be identified by a some collection of parameters, like the date of delivery, value of a field, time of validation, or access control permissions.

- Batches often correspond to deliveries of data or runs of an ETL pipeline, but they do not have to. For example, an analyst studying New York City taxi data might take one logical view into the data where each batch is a month's delivery. But if the analyst selects data from the dataset based on other criteria for her analysis--the time of the ride and number of passengers, for example--then each batch corresponds to the specific query she runs. In that case, the Expectations she creates may have more to do with the analysis she is running than aggregate characteristics of the taxi data.

In some cases **the thing that "makes a batch a batch" is the act of attending to it--for example by validating or profiling the data**. It's all about **your** Expectations.
