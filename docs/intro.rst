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


Key features
--------------------------------------------------

**Expectations** are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code.

**Execution engines** : Following the philosophy of "take the compute to the data," Great Expectations supports native execution of Expectations in common data engineering frameworks: pandas, SQL (through the SQLAlchemy core), and Spark. Future releases of Great Expectations will extend this functionality to other frameworks, such as dask and BigQuery.

**Automated data profiling** : 

**DataContexts and DataSources** allow you to configure connections your data stores, using names you’re already familiar with: “the ml_training_results bucket in S3,” “the Users table in Redshift.” GE will soon provide convenience libraries to introspect most common data stores (SQL databases, data directories and buckets) and data execution frameworks (airflow, dbt, dagster, prefect.io). This lets you fetch, validate, profile, and document your data in a way that’s meaningful within your existing infrastructure and work environment.

**Tooling for validation** : Evaluating Expectations against data is just one step in a typical validation workflow. DataContexts will make it simple to post notifications to slack, store validation results to a shared bucket, connect to schedulers, loggers, etc.
Although we sometimes talk informally about validating “tables,” it’s much more common to validate batches of new data—-subsets of tables, rather than whole tables. DataContexts provide simple, universal syntax to generate, fetch, and validate Batches of data from any of your DataSources.

**Compile to Docs** : This release provides new methods and classes to `render` Expectations to clean, human-readable documentation. Since docs are compiled from tests and you are running tests against new data as it arrives, your documentation is guaranteed to never go stale.


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


Workflow advantages
-------------------

Most data science and data engineering teams end up building some form of pipeline testing, eventually. Unfortunately, many teams don't get around to it until late in the game, long after early lessons from data exploration and model development have been forgotten.

In the meantime, data pipelines often become deep stacks of unverified assumptions. Mysterious (and sometimes embarrassing) bugs crop up more and more frequently. Resolving them requires painstaking exploration of upstream data, often leading to frustrating negotiations about data specs across teams.

It's not unusual to see data teams grind to a halt for weeks (or even months!) to pay down accumulated pipeline debt. This work is never fun---after all, it's just data cleaning: no new products shipped; no new insights kindled. Even worse, it's re-cleaning old data that you thought you'd already dealt with. In our experience, servicing pipeline debt is one of the biggest productivity and morale killers on data teams.

We strongly believe that most of this pain is avoidable. We built Great Expectations to make it very, very simple to

1. set up and deploy your testing and documentation framework,
2. populate tests and documentation through a combination of automated profiling and tools for knowledge capture, and
3. systematically validate new data against them.

It's the best tool we know of for managing the complexity that inevitably grows within data pipelines. We hope it helps you as much as it's helped us.

Good night and good luck!


What does Great Expectations NOT do?
-------------------------------------------------------------

**Great Expectations is NOT a pipeline execution framework.**
We aim to integrate seamlessly with DAG execution tools like Spark, Airflow, dbt, prefect, dagster, kevu, etc. We DON'T execute your pipelines for you.

**Great Expectations is NOT a data versioning tool.** ALternatives: DVC, Quilt, Liquidata

**Great Expectations provides a lightweight metadata store, but it also plays nice with other metadata tools.** 

**Great Expectations currently works best in a python/bash environment.** 
Alternatives: assertR, 


Who maintains Great Expectations?
-------------------------------------------------------------

Great Expectations is maintained by James Campbell, Abe Gong, Eugene Mandell and Rob Lim, with help from many others.

If you'd like to contribute to Great Expectations, please start here.

If you'd like hands-on assistance setting up Great Expectations, establishing a healthy practice of data testing, or adding functionality to Great Expectations, please see options for consulting help here.

