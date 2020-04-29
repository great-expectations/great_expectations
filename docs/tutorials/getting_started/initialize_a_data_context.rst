.. _getting_started__initialize_a_data_context:

Initialize a Data Context
===============================================

In Great Expectations, your ``DataContext`` manages boilerplate configuration. Using a DataContext is almost always the fastest way to get up and running, even though some teams don't need every component of a DataContext.

If you really want to learn the components of Great Expectations without a DataContext, check out `A magic-free introduction to Great Expectations.`_


Install Great Expectations
-----------------------------------------------

If you haven't already, install Great Expectations.

We recommend deploying within a virtual environment. If you're not familiar with virtual environments, pip, notebooks,
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

Get the ``example-dickens-data-project``
-----------------------------------------------

For this tutorial, we will use a simple example project based on the works of Charles Dickens. If this is your very first time with Great Expectations, we encourage you to use this example, so that there's no discrepancy between what you see in the tutorial and in your own environment. Later, you can follow similar steps with data and code of your own.

To download this project:

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


Quick orientation to this project: 
# FIXME: What are the main components of this project?

# FIXME: What does the data itself look like?

Run ``great_expectations init``
-----------------------------------------------

When you installed Great Expectations, you also installed the Great Expectations :ref:`command line interface (CLI) <command_line>`. It provides helpful utilities for deploying and configuring DataContexts, plus a few other convenience methods.

To initialize your Great Expectations deployment for the project, run this command in the terminal from the ``example_dickens_data_project/`` directory.

.. code-block:: bash

    great_expectations init


You should see this:

.. code-block:: bash

      ___              _     ___                  _        _   _             
     / __|_ _ ___ __ _| |_  | __|_ ___ __  ___ __| |_ __ _| |_(_)___ _ _  ___
    | (_ | '_/ -_) _` |  _| | _|\ \ / '_ \/ -_) _|  _/ _` |  _| / _ \ ' \(_-<
     \___|_| \___\__,_|\__| |___/_\_\ .__/\___\__|\__\__,_|\__|_\___/_||_/__/
                                    |_|                                      
                 ~ Always know what to expect from your data ~             

    In a few minutes you will see Great Expectations in action on your data!
    
    First, Great Expectations will create a new directory:
    
    #FIXME: Adjust this here, and in init_messages.py

        great_expectations
        |-- expectations
        |-- great_expectations.yml
        |-- notebooks
        |   |-- pandas
        |   |-- spark
        |   |-- sql
        |-- plugins
        |   |-- ...
        |-- uncommitted
            |-- config_variables.yml
            |-- ...

    #FIXME: Here's what it should look like:

        great_expectations
        ├── great_expectations.yml
        ├── expectations
        ├── notebooks
        ├── plugins
        ├── .gitignore
        └── uncommitted
            ├── config_variables.yml
            ├── documentation
            └── validations

    OK to proceed? [Y/n]: 


Let's pause there for a moment.
    
Once you finish going through ``init``, your ``great_expectations/`` directory will contains all of the important components of a Great Expectations deployment, in miniature:

* ``great_expectations.yml`` will define Data Sources, Expectations, Validation Results, etc. for the deployment.
* The ``expectations/`` directory will store all your Expectations as JSON files. If you want to store them somewhere else, you can change that later.
* The ``uncommitted/`` directory contains files that shouldn't live in version control. It has a ``.gitignore`` configured to exclude all its contents from version control. The main contents of the directory are:

  * ``uncommitted/config_variables.yml``, which should hold sensitive information, such as database credentials and other secrets.
  * ``uncommitted/validations``, which will hold Validation Results generated by Great Expectations.
  * ``uncommitted/documentation``, which will hold contains data documentation generated from Expectations, Validation Results, and other metadata.

Back in your terminal, go ahead and hit ``Enter`` to proceed.