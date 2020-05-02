.. _reference__core_concepts:


#############
Core concepts
#############


*************
Expectations
*************

Expectations are assertions for data. They help accelerate data engineering and increase analytic integrity, by making it possible to answer a critical question:

- What can I expect of my data?

**Expectations** are declarative statements that a computer can evaluate, and that are semantically meaningful to 
humans, like expect_column_values_to_be_unique or expect_column_mean_to_be_between.

**Expectation Configurations** describe specific Expectations for data. They combine an Expectation and specific 
parameters to make it possible to evaluate whether the expectation is true on a dataset. For example, they might provide expected values or the name of a column whose values should be unique.

**Expectation Suites** combine multiple Expectation Configurations into an overall description of a dataset. Expectation
Suites should have names corresponding to the kind of data they define, like “NPI” for National Provider Identifier data or “company.users” for a users table.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/custom_expectations.rst
   /reference/core_concepts/expectations.rst
   /reference/core_concepts/result_format.rst
   /reference/core_concepts/standard_arguments.rst

*************
Data Contexts
*************

A **Data Context** stitches together all the features available with Great Expectations, making it possible to easily 
manage configurations for datasources, and data docs sites and to store expectation suites and validations. Data Contexts also unlock more powerful features such as Evaluation Parameter Stores.

A **Data Context Configuration** is a yaml file that can be committed to source control to ensure that all the settings 
related to your validation are appropriately versioned and visible to your team. It can flexibly describe plugins and other customizations for accessing datasources or building data docs sites.

A **Store** allows you to manage access to Expectations, Validations and other Great Expectations assets in a 
standardized way, making it easy to share resources across a team that uses AWS, Azure, GCP, local storage, or something else entirely.

An **Evaluation Parameter** Store makes it possible to build expectation suites that depend on values from other batches
of data, such as ensuring that the number of rows in a downstream dataset equals the number of unique values from an upstream one. A Data Context can manage a store to facilitate that validation scenario.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_context.rst
   /reference/core_concepts/evaluation_parameters.rst

***********
Datasources
***********

Great Expectations lets you focus on your data, not writing tests. It validates your expectations no matter where the data is located.

Datasources, Generators, Batch Parameters and Batch Kwargs make it easier to connect Great Expectations to your data. Together, they address questions such as:

- How do I get data into my Great Expectations data asset?
- How do I tell my Datasource how to access my specific data?
- How do I use Great Expectations to store Batch Kwargs configurations or logically describe data when I need to build
equivalent Batch Kwargs for different datasources?
- How do I know what data is available from my datasource?

A **Datasource** is a connection to a compute environment (a backend such as Pandas, Spark, or a SQL-compatible 
database) and one or more storage environments. It produces batches of data that Great Expectations can validate in that environment.

**Batch Kwargs** are specific instructions for a Datasource about what data should be prepared as a “batch” for 
validation. The batch could be a specific database table, the most recent log file delivered to S3, or even a subset of one of those objects such as the first 10,000 rows.

**Batch Parameters** provide instructions for how to retrieve stored Batch Kwargs or build new Batch Kwargs that reflect
partitions, deliveries, or slices of logical data assets.

A **Batch Kwargs Generator** translates Batch Parameters to datasource-specific Batch Kwargs. A Batch Kwargs Generator 
can also identify data assets and partitions by inspecting a storage environment.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/datasource.rst
   /reference/core_concepts/datasource_reference.rst
   /reference/core_concepts/batch_kwargs_generator.rst

**********
Validation
**********
In addition to specifying Expectations, Great Expectations also allows you to validate your data against an Expectation Suite. Validation produces a detailed report of how the data meets your expectations -- and where it doesn’t.

DataAssets and Validations, answer the questions:

- How do I describe my Expectations to Great Expectations?
- Does my data meet my Expectations?

A **DataAsset** is a Great Expectations object that can create and validate Expectations against specific data. 
DataAssets are connected to data. They can evaluate Expectations wherever you access your data, using Pandas, Spark, or SqlAlchemy.

An **Expectation Validation Result** captures the output of checking an expectation against data. It describes whether 
the data met the expectation, and additional metrics from the data such as the percentage of unique values or observed mean.

An **Expectation Suite Validation Result** combines multiple Expectation Validation Results and metadata about the 
validation into a single report.

A **Metric** is simply a value produced by Great Expectations when evaluating one or more batches of data, such as an 
observed mean or distribution of data.

A **Validation Operator** stitches together resources provided by the Data Context to build mini-programs that 
demonstrate the full potential of Great Expectations. They take configurable Actions such as updating Data Docs, sending a notification to your team about validation results, or storing a result in a shared S3 bucket.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/validation.rst
   /reference/core_concepts/validation_operators_and_actions.rst
   /reference/core_concepts/validation_result.rst
   /reference/core_concepts/metric_reference.rst
   /reference/core_concepts/metrics.rst

**************************
Data Docs
**************************

With Great Expectations, your tests can update your docs, and your docs can validate your data. Data Docs makes it possible to produce clear visual descriptions of what you expect, what you observe, and how they differ. Does my data meet my expectations?

An **Expectation Suite Renderer** creates a page that shows what you expect from data. Its language is prescriptive, for
example translating a fully-configured expect_column_values_to_not_be_null expectation into “column “address” values must not be null, at least 80% of the time”

A **Validation Result Renderer** produces an overview of the result of validating a batch of data with an Expectation 
Suite. It shows the difference between observed and expected values.

A **Profiling Renderer** details the observed metrics produced from a validation without comparing them to 
specific expected values. It provides a detailed look into what Great Expectations learned about your data.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/data_docs.rst

**************************
Profiling
**************************
Profiling helps you understand your data by describing it and even building expectation suites based on previous batches of data. Profiling lets you ask:

- What is this dataset like?

A **Profiler** reviews data assets and produces new Expectation Suites and Expectation Suite Validation Results that 
describe the data. A profiler can create a “stub” of high-level expectations based on what it sees in the data. Profilers can also be extended to create more specific expectations based on team conventions or statistical properties. Finally, Profilers can take advantage of metrics produced by Great Expectations when validating data to create useful overviews of data.

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/profilers.rst
   /reference/core_concepts/profiling.rst


------------------------------------------------------

.. toctree::
   :maxdepth: 2

   /reference/core_concepts/custom_expectations.rst
   /reference/core_concepts/expectations.rst
   /reference/core_concepts/result_format.rst
   /reference/core_concepts/standard_arguments.rst

   /reference/core_concepts/data_context.rst
   /reference/core_concepts/evaluation_parameters.rst

   /reference/core_concepts/datasource.rst
   /reference/core_concepts/datasource_reference.rst
   /reference/core_concepts/batch_kwargs_generator.rst

   /reference/core_concepts/validation.rst
   /reference/core_concepts/validation_operators_and_actions.rst
   /reference/core_concepts/validation_result.rst
   /reference/core_concepts/metric_reference.rst
   /reference/core_concepts/metrics.rst

   /reference/core_concepts/data_docs.rst

   /reference/core_concepts/profilers.rst
   /reference/core_concepts/profiling.rst

