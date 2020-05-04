.. _getting_started__set_up_data_docs:


Set Up data documentation
=========================

:ref:`Data Docs<data_docs>` is a feature of Great Expectations that creates data documentation by compiling expectations and validation results into HTML.

Data Docs produces a visual data quality report of what you expect from your data, and how the observed properties of your data differ from your expectations.
It helps to keep your entire team on the same page as data evolves.

Here is what the ``expect_column_distinct_values_to_be_in_set`` expectation about the `rating` column of the movie ratings table from the earlier example looks like in Data Docs:

.. image:: ../../images/exp_ratings_col_dist_val_set.png

This approach to data documentation has two significant advantages.

1. **Your docs are your tests** and **your tests are your docs.**
For engineers, Data Docs makes it possible to **automatically keep your data documentation in sync with your tests**.
This prevents documentation rot and can save a huge amount of time and pain maintaining documentation.

2. The ability to translate expectations back and forth between human and machine-readable formats opens up
many opportunities for domain experts and stakeholders who aren't engineers to collaborate more closely with
engineers on data applications.

Multiple sites can be configured inside a project, each suitable for a particular use case.
For example, some data teams use one site that has expectations and validation results from all the runs of their data pipeline for monitoring the pipeline's health,
and another site that has only the expectations for communicating with their downstream clients.
This is analogous to API documentation in software development.

To set up Data Docs for a project, an entry ``data_docs_sites`` must be defined in the project's configuration file.
By default Data Docs site files are published to the local filesystem here: ``great_expectations/uncommitted/data_docs/``.
You can see this by running:

.. code-block:: bash

    great_expectations docs build

To make a site available more broadly, a team member could configure Great Expectations to publish the site to a shared location,
such as a :ref:`AWS S3<publishing_data_docs_to_s3>`, GCS.

The site's configuration defines what to compile and where to store results.
Data Docs is very customizable - see the :ref:`Data Docs Reference<data_docs_reference>` for more information.
