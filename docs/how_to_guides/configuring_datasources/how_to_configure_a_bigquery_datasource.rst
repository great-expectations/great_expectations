.. _how_to_guides__configuring_datasources__how_to_configure_a_bigquery_datasource:

How to configure a BigQuery Datasource
=========================================================

This guide will help you add a BigQuery project (or a dataset) as a Datasource. This will allow you to validate tables and queries within this project. When you use a BigQuery Datasource, the validation is done in BigQuery itself. Your data is not downloaded.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`
  - Followed the `Google Cloud library guide <https://googleapis.dev/python/google-api-core/latest/auth.html>`_ for authentication
  - Installed the pybigquery package for the BigQuery sqlalchemy dialect (``pip install pybigquery``)

Steps
-----


1. Run the following CLI command to begin the interactive Datasource creation process:

.. code-block:: bash

    great_expectations datasource new


2. Choose "Big Query" from the list of database engines, when prompted.
3. Identify the connection string you would like Great Expectations to use to connect to BigQuery, using the examples below and the `PyBigQuery <https://github.com/mxmzdlv/pybigquery>`_ documentation.

    If you want GE to connect to your BigQuery project (without specifying a particular dataset), the URL should be:

    .. code-block:: python

        "bigquery://project-name"


    If you want GE to connect to a particular dataset inside your BigQuery project, the URL should be:


    .. code-block:: python

        "bigquery://project-name/dataset-name"


    If you want GE to connect to one of the Google's public datasets, the URL should be:

    .. code-block:: python

        "bigquery://project-name/bigquery-public-data"

5. Enter the connection string when prompted, and finish completing the interactive prompts.
6. Should you need to modify your connection string you can manually edit the
   ``great_expectations/uncommitted/config_variables.yml`` file.


Additional notes
----------------

Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

Additional resources
--------------------
