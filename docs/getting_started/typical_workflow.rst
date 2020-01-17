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
        └── validations


Creating expectation suites
----------------------------------------

CLI
`great_expectations suite new`

stores as JSON filed and renders the expectation suite into an HTML page in Data Docs.


Reviewing expectation suites
----------------------------------------

DataDocs

Editing expectation suites
----------------------------------------

`great_expectations suite edit`

    - This command compiles a jupyter notebook from the JSON Expectation suite.
    - Because this notebook is compiled fromt the source-of-truth JSON, it can be treated as discardable.
4. In the jupyter notebook, run all the expectation cells you wish to retain in the suite.


Deploying validation into a pipeline
----------------------------------------

You end up creating one or multiple expectation suites for various data assets in your pipeline - a file, a Pandas or Spark dataframe, a result of a SQL query. Depending on the technolog ... your pipeline uses Airflow, a custom script, a cron job...
you will

Test this batch of data against this expectation suite. If the data meets the expectations in the suite, great. If some expectations are not met, you want to save the validation result for review, maybe stop the pipeline from continuing its run and notify 

Reacting to validation results
----------------------------------------

TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD
TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD TBD





1. Checkout the master branch.
2. Create a branch with a representative name. This will be used to create a Pull Request (PR) back into the master branch.
3. Edit the suite using the command `great_expectations edit-suite`. **Note** in a near term release (0.9.0) this command will be renamed to `great_expectations suite edit`.
    - This command compiles a jupyter notebook from the JSON Expectation suite.
    - Because this notebook is compiled fromt the source-of-truth JSON, it can be treated as discardable.
4. In the jupyter notebook, run all the expectation cells you wish to retain in the suite.
5. You can adjust or add additional expectations in this notebook.
6. Be sure to run the last cells in the notebook which save the modifed suite to disk as JSON.