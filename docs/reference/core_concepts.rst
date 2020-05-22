.. _reference__core_concepts:


#############
Core concepts
#############


*************
Expectations
*************

Expectations are assertions for data. They help accelerate data engineering and increase analytic integrity, by making it possible to answer a critical question: *what can I expect of my data?*

**Expectations** are declarative statements that a computer can evaluate, and that are semantically meaningful to humans, like ``expect_column_values_to_be_unique`` or ``expect_column_mean_to_be_between``.

**Expectation Configurations** describe specific Expectations for data. They combine an Expectation and specific parameters to make it possible to evaluate whether the expectation is true for some specific data. For example, they might provide expected values for a column or the name of a column whose values should be unique.

**Expectation Implementations** provide the critical translation layer between what we expect and how to verify the expectation in data or express it in :ref:`data_docs`. Expectation Implementations are tailored for specific validation engines where the actual expectation is executed.

**Expectation Suites** combine multiple Expectation Configurations into an overall description of a dataset. Expectation
Suites should have names corresponding to the kind of data they define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/expectations/expectations.rst
   /reference/core_concepts/expectations/distributional_expectations.rst
   /reference/core_concepts/expectations/standard_arguments.rst
   /reference/core_concepts/expectations/result_format.rst
   /reference/core_concepts/expectations/glossary_of_expectations.rst
   /reference/core_concepts/expectations/implemented_expectations.rst

**********
Validation
**********

Great Expectations makes it possible to validate your data against an Expectation Suite. Validation produces a detailed report of how the data meets your expectations -- and where it doesn’t.

.. attention::

   The DataAsset class will be refactored and renamed in an upcoming release of Great Expectations to make it easier to create custom expectations and ensure Expectation Implementations are consistent across different validation engines.

A **DataAsset** is a Great Expectations object that can create and validate Expectations against specific data. DataAssets are connected to data. A DataAsset can evaluate Expectations wherever you access your data, using different **ValidationEngines** such as Pandas, Spark, or SqlAlchemy.

An **Expectation Validation Result** captures the output of checking an expectation against data. It describes whether
the data met the expectation, and additional metrics from the data such as the percentage of unique values or observed mean.

An **Expectation Suite Validation Result** combines multiple Expectation Validation Results and metadata about the
validation into a single report.

A **Metric** is a value produced by Great Expectations when evaluating one or more batches of data, such as an
observed mean or distribution of data.

A **Validation Operator** stitches together resources provided by the Data Context to provide an easy way to deploy Great Expectations in your environment. It executes configurable **Action**s such as updating Data Docs, sending a notification to your team about validation results, or storing a result in a shared S3 bucket.

A **Checkpoint** is a configuration for a Validation Operator that specifies which Batches of data and Expectation Suites should be validated.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/validation.rst
   /reference/core_concepts/validation_operators_and_actions.rst
   /reference/core_concepts/validation_result.rst
   /reference/core_concepts/metric_reference.rst
   /reference/core_concepts/metrics.rst


*************
Data Context
*************

A **Data Context** stitches together all the features available with Great Expectations, making it possible to easily 
manage configurations for resources such as Datasources, Validation Operators, Data Docs Sites, and Stores.

A **Data Context Configuration** is a yaml file that can be committed to source control to ensure that all the settings 
related to your validation are appropriately versioned and visible to your team. It can flexibly describe plugins and other customizations for accessing datasources or building data docs sites.

A **Store** provides a consistent API to manage access to Expectations, Expectation Suite Validation Results and other Great Expectations assets, making it easy to share resources across a team that uses AWS, Azure, GCP, local storage, or something else entirely.

An **Evaluation Parameter** Store makes it possible to build expectation suites that depend on values from other batches
of data, such as ensuring that the number of rows in a downstream dataset equals the number of unique values from an upstream one. A Data Context can manage a store to facilitate that validation scenario.

A **Metric** Store makes facilitates saving any metric or statistic generated during validation, for example making it easy to create a dashboard showing key output from running Great Expectations.


.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_context.rst
   /reference/core_concepts/evaluation_parameters.rst

************
Datasources
************

.. attention::

   Datasource configuration will be changing soon to make it easier to:

   - adjust configuration for where data is stored and validated independently.
   - understand the roles of Batch Kwargs, Batch Kwargs Generators, Batch Parameters, and Batch Markers.

Datasources, Batch Kwargs Generators, Batch Parameters, and Batch Kwargs make it easier to connect Great Expectations to your data. Together, they address questions such as:

- How do I get data into my Great Expectations DataAsset?
- How do I tell my Datasource how to access my specific data?
- How do I use Great Expectations to store Batch Kwargs configurations or logically describe data when I need to build
equivalent Batch Kwargs for different datasources?
- How do I know what data is available from my datasource?

A **Datasource** is a connection to a **Validation Engine** (a compute environment such as Pandas, Spark, or a SQL-compatible database) and one or more data storage locations. For a SQL database, the Validation Engine and data storage locations will be the same, but for Spark or Pandas, you may be reading data from a remote location such as an S3 bucket but validating it in a cluster or local machine. The Datasource produces Batches of data that Great Expectations can validate in that environment.

**Batch Kwargs** are specific instructions for a Datasource about what data should be prepared as a Batch for
validation. The Batch could reference a specific database table, the most recent log file delivered to S3, or a subset of one of those objects, for example just the first 10,000 rows.

A **Batch Kwargs Generator** produces datasource-specific Batch Kwargs. The most basic Batch Kwargs Generator simply stores Batch Kwargs by name to make it easy to retrieve them and obtain batches of data. But Batch Kwargs Generators can be more powerful and offer stronger guarantees about reproducibility and compatibility with other tools, and help identify available Data Assets and Partitions by inspecting a storage environment.

**Batch Parameters** provide instructions for how to retrieve stored Batch Kwargs or build new Batch Kwargs that reflect partitions, deliveries, or slices of logical data assets.

**Batch Markers** provide additional metadata a batch to help ensure reproducitiblity, such as the timestamp at which it was created.


.. toctree::
   :maxdepth: 2

   /reference/core_concepts/datasource.rst
   /reference/core_concepts/datasource_reference.rst
   /reference/core_concepts/batch_kwargs_generator.rst


**************************
Data Docs
**************************

With Great Expectations, your tests are your docs, and your docs are your tests. Data Docs makes it possible to produce clear visual descriptions of what you expect, what you observe, and how they differ.

An **Expectation Suite Renderer** creates a page that shows what you expect from data. Its language is prescriptive, for example translating a fully-configured ``expect_column_values_to_not_be_null`` expectation into the English phrase, "column ``address`` values must not be null, at least 80% of the time."

A **Validation Result Renderer** produces an overview of the result of validating a batch of data with an Expectation 
Suite. It shows the difference between observed and expected values.

A **Profiling Renderer** details the observed metrics produced from a validation without comparing them to 
specific expected values. It provides a detailed look into what Great Expectations learned about your data.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_docs.rst
   /reference/core_concepts/profiling.rst

*********
Profiling
*********

Profiling helps you understand your data by describing it and even building expectation suites based on previous batches of data. Profiling lets you ask:

- What is this dataset like?

A **Profiler** reviews data assets and produces new Metrics, Expectation Suites, and Expectation Suite Validation Results that describe the data. A profiler can create a “stub” of high-level expectations based on what it sees in the data. Profilers can also be extended to create more specific expectations based on team conventions or statistical properties. Finally, Profilers can take advantage of metrics produced by Great Expectations when validating data to create useful overviews of data.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/profilers.rst
