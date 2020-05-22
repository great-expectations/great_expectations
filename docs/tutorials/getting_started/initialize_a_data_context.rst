.. _getting_started__initialize_a_data_context:

Initialize a Data Context
===============================================

In Great Expectations, your :ref:`Data Context` manages boilerplate configuration. Using a Data Context is almost always the fastest way to get up and running, even though some teams don't need every component of a Data Context.


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

    $ pip install great_expectations

To install from a git branch, use the following command (replace ``develop`` below with the name of the branch you want to use):

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ cd great_expectations/
    $ git checkout develop
    $ pip install -e .

To install from a git fork, use the following command (replace ``great-expectations`` below with the name of the fork, which is usually your github username):

.. code-block:: bash

    $ pip install -e .
    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install great_expectations/

If you intend to develop within the Great Expectations (e.g. to contribute back to the project), check out :ref:`contributing_setting_up_your_dev_environment` in the contributor documentation.

Downloading Example Data
-----------------------------------------------
For this tutorial, we will use a sample dataset released by the `Centers of Medicare and Medicaid <https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination>`_.
It includes the National Provider Identifier (NPI) information for unique health care providers in the United States that you can download from an S3 bucket.

Before you download the data, we recommend you create a test directory where your data is eventually going to be stored. In our case we will use the name ``ge_example_project``

.. code-block:: bash
   $ mkdir ge_example_project
   $ cd ge_example_project
   $ mkdir data
   $ cd data

To download this project use the following ``wget`` command, which should download it to your current directory. Make sure you download the file and unzip it in a directory that you can access. You will need it in initializing the great_expectations data context.

.. code-block:: bash

    $ wget https://superconductive-public.s3.amazonaws.com/data/npi/weekly/npidata_pfile_20200511-20200517.csv.gz
    $ gunzip npidata_pfile_20200511-20200517.csv.gz


Run ``great_expectations init``
-----------------------------------------------

When you installed Great Expectations, you also installed the Great Expectations :ref:`command line interface (CLI) <command_line>`. It provides helpful utilities for deploying and configuring DataContexts, plus a few other convenience methods.

To initialize your Great Expectations deployment for the project, run this command in the terminal from the ``example_dickens_data_project/`` directory.

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

    In a few minutes you will see Great Expectations in action on your data!

    First, Great Expectations will create a new directory:

    #FIXME: Adjust this here, and in init_messages.py

In a few minutes you will see Great Expectations in action on your data!

First, Great Expectations will create a new directory:

        great_expectations
        |-- expectations
        |-- great_expectations.yml
        |-- checkpoints
        |-- notebooks
        |   |-- pandas
        |   |-- spark
        |   |-- sql
        |-- plugins
        |   |-- ...
        |-- uncommitted
            |-- config_variables.yml
            |-- ...

    OK to proceed? [Y/n]:

Let's pause there for a moment.

Once you finish going through ``init``, your ``great_expectations/`` directory will contains all of the important components of a Great Expectations deployment, in miniature:


* ``great_expectations.yml`` will contain the main configuration your deployment.
* The ``expectations/`` directory will store all your :ref:`Expectations` as JSON files. If you want to store them somewhere else, you can change that later.
* The ``notebooks/`` directory is for helper notebooks to interact with Great Expectations.
* The ``plugins/`` directory will hold code for any custom plugins you develop as part of your deployment.
* The ``uncommitted/`` directory contains files that shouldn't live in version control. It has a ``.gitignore`` configured to exclude all its contents from version control. The main contents of the directory are:

  * ``uncommitted/config_variables.yml``, which will hold sensitive information, such as database credentials and other secrets.
  * ``uncommitted/validations``, which will hold :ref:`Validation Results` generated by Great Expectations.
  * ``uncommitted/documentation``, which will contains :ref:`Data Docs` generated from Expectations, Validation Results, and other metadata.

Back in your terminal, go ahead and hit ``Enter`` to proceed.
