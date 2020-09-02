.. _tutorials__getting_started__initialize_a_data_context:

Set up the tutorial data and initialize a Data Context
======================================================

In Great Expectations, your :ref:`Data Context <reference__core_concepts__data_context__data_context>` manages your project configuration. Using a Data Context is almost always the fastest way to get up and running, even though some teams don't need every component of a Data Context.


Install Great Expectations
-----------------------------------------------

If you haven't already, install Great Expectations.

We recommend deploying within a virtual environment. If you're not familiar with virtual environments, pip, jupyter notebooks,
or git, you may want to check out the :ref:`supporting_resources` section before continuing.

.. raw:: html

   The command to install is especially great <a href="https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/pip_install_great_expectations.png" target="_blank">if you're a Dickens fan</a>:
   <br/>
   <br/>

.. code-block:: bash

    pip install great_expectations

You'll also need to install a few other dependencies into your virtual environment:

.. code-block:: bash

   pip install SQLAlchemy psycopg2-binary


If you intend to develop within Great Expectations (e.g. to contribute back to the project), and would like to install it from a git branch or a fork, check out :ref:`contributing_setting_up_your_dev_environment` in the contributor documentation.

Preparation instructions
------------------------

For this tutorial, we will use a simplified version of the NYC taxi ride data. We prepared a Docker image that contains a local Postgres database with the data pre-loaded, so you can easily get up and running without any local dependencies.

To avoid confusion during the tutorial, we recommend you follow these steps:

#. Make sure you have `Docker <https://www.docker.com/>`_ installed

#. Clone the `ge_tutorials <https://github.com/superconductive/ge_tutorials>`_ repository and start up the container with the Postgres database containing the data:

.. code-block:: bash

   git clone https://github.com/superconductive/ge_tutorials
   cd ge_tutorials/ge_getting_started_tutorial
   docker-compose up

You will now have a Postgres database running with some pre-loaded data! In case you're looking to connect to the database, you'll find instructions in the `README <https://github.com/superconductive/ge_tutorials/tree/main/ge_getting_started_tutorial>`_ in the repository.

About the data
-----------------------------------------------

The `NYC taxi data <https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page>`_ is an open data set which is updated every month. Each record in the data corresponds to one taxi ride and contains information such as the pick up and drop-off location, the payment amount, and the number of passengers, among others.

In this tutorial, we provide two tables, each with a 10,000 row sample of the Yellow Taxi Trip Records set:

* **yellow_tripdata_sample_2019_01**: a sample of the January 2019 taxi data
* **yellow_tripdata_staging**: a sample of the February 2019 taxi data, loaded to a "staging" table so we can validate it before promoting it to a permanent table

If we compare the ``passenger_count`` column in the January and February data, we find a significant difference: The February data contains a large proportion of rides with 0 passengers, which seems unexpected.

.. admonition:: The data problem we're solving in this tutorial

    In this tutorial, we will be creating an Expectation Suite for this example data set that allows us to assert that we generally expect at least 1 passenger per taxi ride based on what we see in the January 2019 data. We will then use that Expectation Suite to catch the data quality issue in the February 2019 staging data.


Run ``great_expectations init``
-----------------------------------------------

First, we want to create a separate project directory ``ge_example/`` for our tutorial project. The ``ge_tutorials`` repo contains the final version of this tutorial, but we want to start from scratch here!

.. code-block:: bash

    cd ..
    mkdir ge_example
    cd ge_example

When you installed Great Expectations, you also installed the Great Expectations :ref:`command line interface (CLI) <command_line>`. It provides helpful utilities for deploying and configuring Data Contexts, plus a few other convenience methods.

To initialize your Great Expectations deployment for the project, run this command in the terminal from the ``ge_example/`` directory.

.. code-block:: bash

    great_expectations init


You should see this:

.. code-block::

      ___              _     ___                  _        _   _
     / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
    | (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
     \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                    |_|
                 ~ Always know what to expect from your data ~

    Let's configure a new Data Context.

    First, Great Expectations will create a new directory:

        great_expectations
        |-- great_expectations.yml
        |-- expectations
        |-- checkpoints
        |-- notebooks
        |-- plugins
        |-- .gitignore
        |-- uncommitted
            |-- config_variables.yml
            |-- documentation
            |-- validations

    OK to proceed? [Y/n]: 

**Let's pause there for a moment and take a look under the hood.**

The ``great_expectations/`` directory structure
-----------------------------------------------

Once you finish going through ``init``, your ``great_expectations/`` directory will contain all of the important components of a local Great Expectations deployment:


* ``great_expectations.yml`` will contain the main configuration your deployment.
* The ``expectations/`` directory will store all your :ref:`Expectations` as JSON files. If you want to store them somewhere else, you can change that later.
* The ``notebooks/`` directory is for helper notebooks to interact with Great Expectations.
* The ``plugins/`` directory will hold code for any custom plugins you develop as part of your deployment.
* The ``uncommitted/`` directory contains files that shouldn't live in version control. It has a ``.gitignore`` configured to exclude all its contents from version control. The main contents of the directory are:

  * ``uncommitted/config_variables.yml``, which will hold sensitive information, such as database credentials and other secrets.
  * ``uncommitted/documentation``, which will contains :ref:`Data Docs <reference__core_concepts__data_docs>` generated from Expectations, Validation Results, and other metadata.
  * ``uncommitted/validations``, which will hold :ref:`Validation Results <reference__core_concepts__validation__expectation_validation_result>` generated by Great Expectations.

Back in your terminal, go ahead and hit ``Enter`` to proceed.
