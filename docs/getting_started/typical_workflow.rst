.. _typical_workflow:

Typical Workflow
===============================================

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD


Setting up a project
----------------------------------------

Great Expectations provides a default project framework that simplifies operations such as connecting to data sources;
fetching, profiling and validating batches of data; and compiling to human-readable documentation.


By default, everything in the Great Expectations deployment framework will be expressed in a directory structure
within a ``great_expectations/`` folder within your version control system. To create this folder, navigate to the
root of your project directory in a terminal and run:

.. code-block:: bash

    great_expectations init

The command line interface (CLI) will scaffold and populate the configuration
and other artifacts necessary to get started with Great Expectations. This can
be run to start a new project and to onboard a teammate to an existing project.


If you inspect the ``great_expectations/`` directory after the init command has run, it should contain:

.. code-block:: bash

    great_expectations
    ├── .gitignore
    ├── datasources
    ├── expectations
    ├── fixtures
    ├── great_expectations.yml
    ├── notebooks
    │   ├── pandas
    │   ├── spark
    │   └── sql
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   └── local_site
        ├── samples
        └── validations


Creating expectation suites
----------------------------------------

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD


Editing expectation suites
----------------------------------------

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD


Deploying validation into a pipeline
----------------------------------------

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD


Reacting to validation results
----------------------------------------

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD

