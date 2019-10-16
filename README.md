[![Build Status](https://travis-ci.org/great-expectations/great_expectations.svg?branch=develop)](https://travis-ci.org/great-expectations/great_expectations)
[![Coverage Status](https://coveralls.io/repos/github/great-expectations/great_expectations/badge.svg?branch=develop)](https://coveralls.io/github/great-expectations/great_expectations?branch=develop)
[![Documentation Status](https://readthedocs.org/projects/great-expectations/badge/?version=latest)](http://great-expectations.readthedocs.io/en/latest/?badge=latest)

<img align="right" src="./generic_dickens_protagonist.png">

Great Expectations
================================================================================

*Always know what to expect from your data.*


Quick Start
--------------------------------------------------------------------------------
[Getting Started](http://docs.greatexpectations.io/en/latest/getting_started.html) will teach you how to get up and running in minutes.

For full documentation, visit [Great Expectations on readthedocs.io](http://great-expectations.readthedocs.io/en/latest/).

[Down with Pipeline Debt!](https://medium.com/@expectgreatdata/down-with-pipeline-debt-introducing-great-expectations-862ddc46782a) explains the core philosophy behind Great Expectations. Please give it a read, and clap, follow, and share while you're at it.


What is great_expectations?
--------------------------------------------------------------------------------

Great Expectations helps teams save time and promote analytic integrity by offering a unique approach to automated testing: pipeline tests. Pipeline tests are applied to data (instead of code) and at batch time (instead of compile or deploy time). Pipeline tests are like unit tests for datasets: they help you guard against upstream data changes and monitor data quality.

Software developers have long known that automated testing is essential for managing complex codebases. Great Expectations brings the same discipline, confidence, and acceleration to data science and engineering teams.


Why would I use Great Expectations?
--------------------------------------------------------------------------------

To get more done with data, faster. Teams use great_expectations to

*  Save time during data cleaning and munging.
*  Accelerate ETL and data normalization.
*  Streamline analyst-to-engineer handoffs.
*  Streamline knowledge capture and requirements gathering from subject-matter experts.
*  Monitor data quality in production data pipelines and data products.
*  Automate verification of new data deliveries from vendors and other teams.
*  Simplify debugging data pipelines if (when) they break.
*  Codify assumptions used to build models when sharing with other
   teams or analysts.
*  Develop rich, shared data documention in the course of normal work.
*  Make implicit knowledge explicit.
*  etc., etc., etc.

Key features
--------------------------------------------------

**Expectations**

Expectations are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code.

Expectations include:
- `expect_table_row_count_to_equal`
- `expect_column_values_to_be_unique`
- `expect_column_values_to_be_in_set`
- `expect_column_mean_to_be_between`
- ...and many more

Great Expectations currently supports native execution of Expectations in three environments: pandas, SQL (through the SQLAlchemy core), and Spark. This approach follows the philosophy of "take the compute to the data." Future releases of Great Expectations will extend this functionality to other frameworks, such as dask and BigQuery.

**Automated data profiling**

Writing pipeline tests from scratch can be tedious and counterintuitive. Great Expectations jump starts the process by providing powerful tools for automated data profiling. This provides the double benefit of helping you explore data faster, and capturing knowledge for future documentation and testing.

**DataContexts and DataSources**

...allow you to configure connections your data stores, using names that point to concepts you’re already familiar with: “the `ml_training_results` bucket in S3,” “the `Users` table in Redshift.” Great Expectations provides convenience libraries to introspect most common data stores (Ex: SQL databases, data directories and S3 buckets.) We are also working to integrate with pipeline execution frameworks (Ex: airflow, dbt, dagster, prefect.io). The Great Expectations framework lets you fetch, validate, profile, and document your data in a way that’s meaningful within your existing infrastructure and work environment.

**Tooling for validation**

Evaluating Expectations against data is just one step in a typical validation workflow. Great Expectations makes the followup steps simple, too: storing validation results to a shared bucket, summarizing results and posting notifications to slack, handling differences between warnings and errors, etc.

Great Expectations also provides robust concepts of Batches and Runs. Although we sometimes talk informally about validating "dataframes" or "tables," it’s much more common to validate batches of new data—subsets of tables, rather than whole tables. DataContexts provide simple, universal syntax to generate, fetch, and validate Batches of data from any of your DataSources.

**Compile to Docs**

As of v0.7.0, Great Expectations includes new classes and methods to `render` Expectations to clean, human-readable documentation. Since docs are compiled from tests and you are running tests against new data as it arrives, your documentation is guaranteed to never go stale.


What does Great Expectations NOT do?
-------------------------------------------------------------

**Great Expectations is NOT a pipeline execution framework.**

We aim to integrate seamlessly with DAG execution tools like [Spark]( https://spark.apache.org/), [Airflow](https://airflow.apache.org/), [dbt]( https://www.getdbt.com/), [prefect](https://www.prefect.io/), [dagster]( https://github.com/dagster-io/dagster), [Kedro](https://github.com/quantumblacklabs/kedro), etc. We DON'T execute your pipelines for you.

**Great Expectations is NOT a data versioning tool.**
	
Great Expectations does not store data itself. Instead, it deals in metadata about data: Expectations, validation results, etc. If you want to bring your data itself under version control, check out tools like: [DVC](https://dvc.org/) and [Quilt](https://github.com/quiltdata/quilt).

**Great Expectations currently works best in a python/bash environment.** 

Great Expectations is python-based. You can invoke it from the command line without using a python programming environment, but if you're working in another ecosystem, other tools might be a better choice. If you're running in a pure R environment, you might consider [assertR](https://github.com/ropensci/assertr) as an alternative. Within the Tensorflow ecosystem, [TFDV](https://www.tensorflow.org/tfx/guide/tfdv) fulfills a similar function as Great Expectations.


How do I learn more?
--------------------------------------------------------------------------------

For full documentation, visit [Great Expectations on readthedocs.io](http://great-expectations.readthedocs.io/en/latest/).

[Down with Pipeline Debt!](https://medium.com/@expectgreatdata/down-with-pipeline-debt-introducing-great-expectations-862ddc46782a) explains the core philosophy behind Great Expectations. Please give it a read, and clap, follow, and share while you're at it.

For quick, hands-on introductions to Great Expectations' key features, check out our walkthrough videos:

* [Introduction to Great Expectations](https://www.youtube.com/watch?v=-_0tG7ACNU4)
* [Using Distributional Expectations](https://www.youtube.com/watch?v=l3DYPVZAUmw&t=20s)

Who maintains Great Expectations?
-------------------------------------------------------------

Great Expectations is under active development by James Campbell, Abe Gong, Eugene Mandel, Rob Lim, Taylor Miller, and help from many others.

What's the best way to get in touch with the Great Expectations team?
--------------------------------------------------------------------------------

If you have questions, comments, or just want to have a good old-fashioned chat about data pipelines, please hop on our public [Slack channel](https://greatexpectations.io/slack)

If you'd like hands-on assistance setting up Great Expectations, establishing a healthy practice of data testing, or adding functionality to Great Expectations, please see options for consulting help [here](https://greatexpectations.io/consulting/).

Can I contribute to the library?
--------------------------------------------------------------------------------

Absolutely. Yes, please. Start [here](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING.md) and please don't be shy with questions.
