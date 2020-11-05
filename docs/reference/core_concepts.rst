.. _reference__core_concepts:


#####################################
Core Great Expectations Concepts
#####################################

This guide describes the core concepts used in Great Expectations. It aims for precision, even when that makes the text dense, but includes examples of typical use cases to help provide intuition. Understanding how Great Expectations uses these concepts helps deliver Great Expectations' key features: **expectations validate data quality**, **tests are docs, and docs are tests**, and **automatic profiling of data**.

*****************************************
Background: Logical Data Assets
*****************************************

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

************************************************
Great Expectations Concepts in the Codebase
************************************************

This section describes the foundational concepts used to integrate Great Expectations into your code.

.. _reference__core_concepts__expectations:

*************
Expectations
*************

Expectations are assertions for data. They help accelerate data engineering and increase analytic integrity, by making it possible to answer a critical question: *what can I expect of my data?*

.. _reference__core_concepts__expectations__expectations:

An **Expectation** is a declarative statements that a computer can evaluate, and that are semantically meaningful to humans, like ``expect_column_values_to_be_unique`` or ``expect_column_mean_to_be_between``.  Expectations are implemented as classes that provide a rich interface to the rest of the library to support validation, profiling, and translation.

**Expectation Implementations** provide the critical translation layer between what we expect and how to verify the expectation in data or express it in :ref:`data_docs`. Expectation Implementations provide different methods for specific execution engines where the actual expectation is executed.

**Expectation Configurations** describe specific Expectations for data. They combine an Expectation and specific parameters to make it possible to evaluate whether the expectation is true for some specific data. For example, they might provide expected values for a column or the name of a column whose values should be unique. See :ref:`domain and success keys for more information about key parts of an Expectation Configuration <reference__core_concepts__expectations__domain_and_success_keys>`.

.. _reference__core_concepts__expectations__expectation_suites:

**Expectation Suites** combine multiple Expectation Configurations into an overall description of a Data Asset. Expectation Suites should have names corresponding to the kind of data they define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/expectations/expectations.rst
   /reference/core_concepts/expectations/distributional_expectations.rst
   /reference/core_concepts/expectations/standard_arguments.rst
   /reference/core_concepts/expectations/result_format.rst
   /reference/core_concepts/expectations/implemented_expectations.rst

.. _reference__core_concepts__data_access:

*************
Data Access
*************

An **Execution Engine** provides the computing resources that will be used to actually perform validation. Great Expectations can take advantage of many different Execution Engines, such as Pandas, Spark, or SqlAlchemy, and even translate the same Expectations to validate data using different engines.

A **Data Connector** facilitates access to an external data store, such as a database, filesystem, or cloud storage. The Data Connector can inspect an external data store to *identify available partitions*, *build batch definitions using parameters such as partition names*, and *translate batch definitions to Execution Engine-specific Batch Specs*. See the :ref:`Data Connectors reference <reference__core_concepts__data_access>` for more information including descriptions of Batch Definition and Batch Spec.

An **Execution Environment** pairs an Execution Engine and one or more Data Connectors that will make Batches from Data Assets available for Validation and Profiling.

A **Batch** is reference to a collection of data, an Execution Engine, and metadata. The Batch is a fundamental building block for accessing data using Great Expectations, but is not the data itself. Instantiating a Batch does not necessarily "fetch" the data by immediately running a query or pulling data into memory. Instead, think of a Batch as a cache that includes the information that you will need to fetch the right data when it’s time to validate.

A **Batch Definition** includes information required to precisely identify a set of data from the external data source that should be translated into a Batch. Specifically, a Batch Definition includes the Data Asset name, Data Connector name, and Execution Environment name, as well as other information including the Partition Definition.

A **Batch Spec** provides specific instructions *for an Execution Engine* about how to access data referred to by a Batch. The Batch Spec could reference a specific database table, the most recent log file delivered to S3, or a subset of one of those objects, for example just the first 10,000 rows.

**Batch Markers** provide additional metadata about a batch to help evaluate reproducibility, such as the timestamp at which it was created or hash of a ``DataFrame``.

.. attention::

    As a best practice, a Batch Spec *should be as explicit as possible*. For example, if using a database, rather than choosing a Batch Spec that defines a generic query relying on a function such as ``NOW()``, choose a query that is fully parameterized ``$start < date AND date <= $end``. More specific Batch Specs make it easier to track the data that was validated and may help take advantage of reproducibility guarantees of external data systems. Batch Spec Generators help make this process easy by allowing stable Batch Parameters to be translated into specific Batch Specs.


.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_access.rst

.. _reference__core_concepts__validation:

*****************
Validation
*****************

A **Validator** uses an Execution Engine and Expectation Suite to validate whether data meets expectations. An **Interactive Validator** can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis to build or edit an Expectation Suite.

An **Expectation Validation Result** captures the output of checking an expectation against data. It describes whether the data met the expectation, and additional metrics from the data such as the percentage of unique values or observed mean.

.. _reference__core_concepts__validation__expectation_validation_result:

An **Expectation Suite Validation Result** combines multiple Expectation Validation Results and metadata about the validation into a single report.

A **Metric** is a value produced by Great Expectations when evaluating one or more batches of data, such as an observed mean or distribution of data. Metrics can be addressed in Great Expectations using standardized names that refer to the specific Batch and Expectation that produced them. Metrics use :ref:`domain and value keys <reference__core_concepts__expectations__domain_and_success_keys>` similarly to the domain and success keys of Expectations.

.. _reference__core_concepts__validation__validation_operator:

A **Validation Operator** stitches together resources provided by the Data Context to provide an easy way to deploy Great Expectations in your environment. It executes configurable **Action** such as updating Data Docs, sending a notification to your team about validation results, or storing a result in a shared S3 bucket.

.. _reference__core_concepts__validation__checkpoints:

A **Checkpoint** is a configuration for a Validation Operator that specifies which Batches of data and Expectation Suites should be validated.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/validation.rst
   /reference/core_concepts/validation_result.rst
   /reference/core_concepts/metrics.rst
   /reference/core_concepts/validation_operators_and_actions.rst

.. _reference__core_concepts__data_contexts:

*************
Data Context
*************

.. _reference__core_concepts__data_context__data_context:

A **Data Context** brings together resources available using Great Expectations, making it possible to easily manage configurations for resources such as Datasources, Validation Operators, Data Docs Sites, and Stores.

A **Data Context Configuration** is usually stored as a yaml file that can be committed to source control to ensure that all the settings related to your validation are appropriately versioned and visible to your team. It can flexibly describe plugins and other customizations for accessing datasources or building data docs sites.

.. _reference__core_concepts__data_context__stores:

A **Store** provides a consistent API to manage access to Expectations, Expectation Suite Validation Results and other Great Expectations assets, making it easy to share resources across a team that uses AWS, Azure, GCP, local storage, or something else entirely.

.. _reference__core_concepts__data_context__metrics:

A **Metric Store** facilitates saving any metric or statistic generated during validation, for example making it easy to create a dashboard showing key output from running Great Expectations.

.. _reference__core_concepts__data_context__evaluation_parameter_stores:

An **Evaluation Parameter Store** is a kind of Metric Store that makes it possible to build expectation suites that depend on values from other batches of data, such as ensuring that the number of rows in a downstream dataset equals the number of unique values from an upstream one. A Data Context can manage a store to facilitate that validation scenario.

**Plugins** are python packages and modules that can be dynamically loaded by the Data Context to support additional functionality, such as a new type of Expectation or Store Backend.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_context.rst
   /reference/core_concepts/evaluation_parameters.rst


.. _reference__core_concepts__data_docs:

**************************
Data Docs
**************************

With Great Expectations, your tests are your docs, and your docs are your tests. Data Docs makes it possible to produce clear visual descriptions of what you expect, what you observe, and how they differ.

.. attention::

    While Data Docs results are extremely robust, we plan to reorganize the internal API for building data docs in the future to provide a more flexible API for extending functionality.

A **Site Builder** orchestrates the construction of individual pages from raw Great Expectations objects, construction of an index, and storage of resources on a Store Backend.

A **Page Builder** converts core Great Expectations objects, such as Expectation Suite Validation Results, into HTML or JSON documents that can be rendered in environments such as a web browser or Slack, using both a Renderer and a

A **Renderer** converts core Great Expectations objects, such as Expectation Suite Validation Results, into an intermediate JSON-based form that includes the relevant *semantic* translation from Expectations but may not include all required formatting for the final document.

- An **Expectation Suite Renderer** creates a page that shows what you expect from data. Its language is *prescriptive*, for example translating a fully-configured ``expect_column_values_to_not_be_null`` expectation into the English phrase, "column ``address`` values must not be null, at least 80% of the time."

- A **Validation Result Renderer** produces an overview of the result of validating a batch of data with an Expectation Suite. Its language is *diagnostic*; it shows the difference between observed and expected values.

- A **Descriptive Renderer** details the observed metrics produced from a validation *without comparing them to specific expected values*. Its language is descriptive; it can be a critical part of a data discovery process.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_docs.rst
   /reference/core_concepts/data_discovery.rst

.. _reference__core_concepts__profiling:

***********
Profilers
***********

A **Profiler** uses an Execution Engine to build a new Expectation Suite. It can use zero, one, or more Batches of data to decide which Expectations to include in the new Suite. A profiler may be used to create basic high-level expectations based on a schema even without data, to create specific Expectations based on team conventions or statistical properties in a dataset, or even to generate Expectation Suites specifically designed to be rendered by a Descriptive Renderer for data discovery.

- For example, a **Suite Builder Profiler** reviews characteristics of a sample Batch of data and proposes candidate expectations to help jumpstart new users to Great Expectations.

- The **Descriptive Profiler** produces Expectation Suites whose Expectations are *always (vacuously) true*. A Descriptive Profiler is not intended to produce Expectation Suites that are useful for production Validation. Instead, its goal is to use Expectations to build a collection of Metrics that are useful for understanding data.

.. toctree::
   :maxdepth: 2


   /reference/core_concepts/data_discovery.rst
   /reference/core_concepts/profilers.rst
