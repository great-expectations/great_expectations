.. _intro:

Introduction
==================

.. toctree::
   :maxdepth: 2

*Always know what to expect from your data.*

What is Great Expectations?
----------------------------

Great Expectations helps teams save time and promote analytic integrity by \
offering a unique approach to automated testing: pipeline tests. Pipeline \
tests are applied to data (instead of code) and at batch time (instead of \
compile or deploy time). Pipeline tests are like unit tests for datasets: \
they help you guard against upstream data changes and monitor data quality.

Software developers have long known that automated testing is essential for \
managing complex codebases. Great Expectations brings the same discipline, \
confidence, and acceleration to data science and engineering teams.


Why would I use Great Expectations?
-----------------------------------

To get more done with data, faster. Teams use Great Expectations to

-  Save time during data cleaning and munging.
-  Accelerate ETL and data normalization.
-  Streamline analyst-to-engineer handoffs.
-  Streamline knowledge capture and requirements gathering from subject-matter experts.
-  Monitor data quality in production data pipelines and data products.
-  Automate verification of new data deliveries from vendors and other teams.
-  Simplify debugging data pipelines if (when) they break.
-  Codify assumptions used to build models when sharing with other
   teams or analysts.
-  Develop rich, shared data documention in the course of normal work.
-  Make implicit knowledge explicit.
-  ... and many more


Key features
--------------------------------------------------

**Expectations**

	Expectations are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code.

	Great Expectations currently supports native execution of Expectations in three environments: pandas, SQL (through the SQLAlchemy core), and Spark. This approach follows the philosophy of "take the compute to the data." Future releases of Great Expectations will extend this functionality to other frameworks, such as dask and BigQuery.

**Automated data profiling** : 

**DataContexts and DataSources**

	...allow you to configure connections your data stores, using names you’re already familiar with: “the ml_training_results bucket in S3,” “the Users table in Redshift.” Great Expectations provides convenience libraries to introspect most common data stores (Ex: SQL databases, data directories and S3 buckets.) Wea are also working to integrate with pipeline execution frameworks (Ex: airflow, dbt, dagster, prefect.io). The Great Expectations framework lets you fetch, validate, profile, and document your data in a way that’s meaningful within your existing infrastructure and work environment.

**Tooling for validation**

	Evaluating Expectations against data is just one step in a typical validation workflow. Great Expectations makes the followup steps simple, too: storing validation results to a shared bucket, summarizing results and posting notifications to slack, handling differences between warnings and errors, etc.

	Great Expectations also provides robust concepts of Batches and Runs. Although we sometimes talk informally about validating "dataframes" or "tables," it’s much more common to validate batches of new data—subsets of tables, rather than whole tables. DataContexts provide simple, universal syntax to generate, fetch, and validate Batches of data from any of your DataSources.

**Compile to Docs**

	As of v0.7.0, Great Expectations includes new classes and methods to `render` Expectations to clean, human-readable documentation. Since docs are compiled from tests and you are running tests against new data as it arrives, your documentation is guaranteed to never go stale.


Workflow advantages
-------------------

Most data science and data engineering teams end up building some form of pipeline testing, eventually. Unfortunately, many teams don't get around to it until late in the game, long after early lessons from data exploration and model development have been forgotten.

In the meantime, data pipelines often become deep stacks of unverified assumptions. Mysterious (and sometimes embarrassing) bugs crop up more and more frequently. Resolving them requires painstaking exploration of upstream data, often leading to frustrating negotiations about data specs across teams.

It's not unusual to see data teams grind to a halt for weeks (or even months!) to pay down accumulated pipeline debt. This work is never fun---after all, it's just data cleaning: no new products shipped; no new insights kindled. Even worse, it's re-cleaning old data that you thought you'd already dealt with. In our experience, servicing pipeline debt is one of the biggest productivity and morale killers on data teams.

We strongly believe that most of this pain is avoidable. We built Great Expectations to make it very, very simple to

1. set up and deploy your testing and documentation framework,
2. author Expectations through a combination of automated profiling and expert knowledge capture, and
3. systematically validate new data against them.

It's the best tool we know of for managing the complexity that inevitably grows within data pipelines. We hope it helps you as much as it's helped us.

Good night and good luck!


What does Great Expectations NOT do?
-------------------------------------------------------------

**Great Expectations is NOT a pipeline execution framework.**

	We aim to integrate seamlessly with DAG execution tools like `Spark <https://spark.apache.org/>`__, `Airflow <https://airflow.apache.org/>`__, `dbt <https://www.getdbt.com/>`__, `prefect <https://www.prefect.io/>`__, `dagster <https://github.com/dagster-io/dagster>`__, `Kedro <https://github.com/quantumblacklabs/kedro>`__, etc. We DON'T execute your pipelines for you.

**Great Expectations is NOT a data versioning tool.**
	
	Great expectations does not store data itself. Instead, it deals in metadata about data. If you want to bring your data itself under version control, check out tools like: `DVC <https://dvc.org/>`__ and `Quilt <https://github.com/quiltdata/quilt>`__

**Great Expectations provides a lightweight metadata store, but it also plays nice with other metadata tools.** 

**Great Expectations currently works best in a python/bash environment.** 
Alternatives: assertR, 


Who maintains Great Expectations?
-------------------------------------------------------------

Great Expectations is maintained by James Campbell, Abe Gong, Eugene Mandell and Rob Lim, with help from many others.

If you have questions, comments, or just want to have a good old-fashioned chat about data pipelines, please hop on our public Slack channel: https://tinyurl.com/great-expectations-slack

If you'd like to contribute to Great Expectations, please head to the :ref:`community` section.

If you'd like hands-on assistance setting up Great Expectations, establishing a healthy practice of data testing, or adding functionality to Great Expectations, please see options for consulting help `here <https://greatexpectations.io/consulting/>`__.

