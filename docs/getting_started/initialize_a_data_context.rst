.. _getting_started__initialize_a_data_context:

Initialize a Data Context
===============================================

In Great Expectations, your ``DataContext`` manages boilerplate configuration. Using a DataContext is almost always the fastest way to get up and running, even though some teams don't need every component of a DataContext.

If you really want to learn the components of Great Expectations without a DataContext, check out `A magic-free introduction to Great Expectations.`_


Install Great Expectations
-----------------------------------------------

If you haven't already, install Great Expectations.

We recommend deploying within a virtual environment. If you're not familiar with pip, virtual environments, notebooks,
or git, you may want to check out the :ref:`supporting_resources` section before continuing.

.. raw:: html

   The command to install is especially great <a href="https://great-expectations-web-assets.s3.us-east-2.amazonaws.com/pip_install_great_expectations.png" target="_blank">if you're a Dickens fan</a>:
   <br/>
   <br/>

.. code-block:: bash

    $ pip install great_expectations

To install from a git branch:

.. code-block:: bash

    #FIXME:
    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install great_expectations/

To install from a git fork:

.. code-block:: bash

    $ git clone https://github.com/great-expectations/great_expectations.git
    $ pip install great_expectations/

If you intend to develop within the Great Expectations (e.g. to contribute back to the project), check out :ref:`contributing_setting_up_your_dev_environment` in the contributor documentation.

Pick a data project
-----------------------------------------------

Most of the features of Great Expectations will make more sense if you see them in the context of real data.

For this tutorial, we will use the data in a simple example project based on the works of Charles Dickens. To download this sample dataset:

.. code-block:: bash

    git clone https://github.com/superconductive/example-dickens-data-project
    cd ge_example_project

The project is laid out as follows:

.. code-block:: bash

    .
    ├── README.md
    ├── data
    │   └── notable_works_by_charles_dickens.csv
    ├── notebooks
    │   └── explore_and_predict_stuff_about_dickens_novels.ipynb
    └── pipeline
        ├── explore_and_predict_stuff_about_dickens_novels.py
        └── title_length_prediction_pipeline.py

If this is your very first time with Great Expectations, we encourage you to use this example, so that there's no discrepancy between what you see in the tutorial and in your own environment. Later, you'll want to follow similar steps with data of your own.


Run ``great_expectations init``
-----------------------------------------------

Great Expectations' :ref:`command line interface (CLI) <command_line>` is the best way to set up a new deployment. Run this command in the terminal in the root of the ``example_dickens_data_project`` directory:

.. code-block:: bash

    great_expectations init

The command creates a ``great_expectations`` subdirectory, structured like this:

.. code-block:: bash

    great_expectations
    ...
    ├── expectations
    ...
    ├── great_expectations.yml
    ├── notebooks
    ...
    ├── .gitignore
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   └── local_site
        └── validations

in the current directory. The team member who runs it, commits the generated directory into the version control. The contents of ``great_expectations`` look like this. This command only needs to be run once per deployment of Great Expectations.

* The ``great_expectations/great_expectations.yml`` configuration file defines how to access the project's data, expectations, validation results, etc.
* The ``expectations`` directory is where the expectations are stored as JSON files.
* The ``uncommitted`` directory is the home for files that should not make it into the version control - it is configured to be excluded in the ``.gitignore`` file. Each team member will have their own content of this directory. In Great Expectations, files should not go into the version control for two main reasons:

  * They contain sensitive information. For example, to allow the ``great_expectations/great_expectations.yml`` configuration file, it must not contain any database credentials and other secrets. These secrets are stored in the ``uncommitted/config_variables.yml`` that is not checked in.

  * They are not a "primary source of truth" and can be regenerated. For example, ``uncommitted/documentation`` contains generated data documentation (this article will cover data documentation in a later section).

