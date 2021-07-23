.. _command_line:

.. warning:: This doc is spare parts: leftover pieces of old documentation.
  It's potentially helpful, but may be incomplete, incorrect, or confusing.

##############################################################
How to use the Great Expectations command line interface (CLI)
##############################################################

.. toctree::
   :maxdepth: 2

After reading this guide, you will know:

* How to create a Great Expectations project
* How to add new datasources
* How to add and edit expectation suites
* How to build and open Data Docs

The Great Expectations command line is organized using a **<NOUN> <VERB>** syntax.
This guide is organized by nouns (datasource, suite, docs) then verbs (new, list, edit, etc).

Basics
======

There are a few commands that are critical to your everyday usage of Great Expectations.
Please note that the V3 (Batch Request) API can be accessed with many of these commands by adding the ``--v3-api`` flag after ``great_expectations``. Please see our how-to guides and use the ``--help`` flag for more complete descriptions of the new functionality.
This is a list of the most common commands you'll use in order of how much you'll probably use them:

* ``great_expectations suite edit``
* ``great_expectations suite new``
* ``great_expectations suite list``
* ``great_expectations suite delete``
* ``great_expectations docs build``
* ``great_expectations docs clean``
* ``great_expectations checkpoint new``
* ``great_expectations checkpoint list``
* ``great_expectations checkpoint run``
* ``great_expectations checkpoint script``
* ``great_expectations datasource list``
* ``great_expectations datasource profile``
* ``great_expectations datasource delete``
* ``great_expectations validation-operator run``
* ``great_expectations init``

You can get a list of Great Expectations commands available to you by typing ``great_expectations --help``.
Each noun command and each verb sub-command has a description, and should help you find the thing you need.

.. note::

    All Great Expectations commands have help text. As with most posix utilities, you can try adding ``--help`` to the end.
    For example, by running ``great_expectations suite new --help`` you'll see help output for that specific command.

.. code-block:: bash

    $ great_expectations --help
    Usage: great_expectations [OPTIONS] COMMAND [ARGS]...

      Welcome to the great_expectations CLI!

      Most commands follow this format: great_expectations <NOUN> <VERB>
      The nouns are: datasource, docs, project, suite
      Most nouns accept the following verbs: new, list, edit

      In particular, the CLI supports the following special commands:

      - great_expectations init : create a new great_expectations project
      - great_expectations datasource profile : profile a datasource
      - great_expectations docs build : compile documentation from expectations

    Options:
      --version      Show the version and exit.
      -v, --verbose  Set great_expectations to use verbose output.
      --help         Show this message and exit.

    Commands:
      datasource  datasource operations
      docs        data docs operations
      init        initialize a new Great Expectations project
      project     project operations
      suite       expectation suite operations


great_expectations init
==============================

To add Great Expectations to your project run the ``great_expectations init`` command in your project directory.
This will run you through a very short interactive experience to
connect to your data, show you some sample expectations, and open Data Docs.

.. note::

        You can install the Great Expectations python package by typing ``pip install great_expectations``, if you don't have it already.

.. code-block:: bash

    $ great_expectations init
      ...

After this command has completed, you will have the entire Great Expectations directory structure with all the code you need to get started protecting your pipelines and data.

great_expectations docs
==============================

``great_expectations docs build``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``great_expectations docs build`` command builds your Data Docs site.
You'll use this any time you want to view your expectations and validations in a web browser.

.. code-block:: bash

    $ great_expectations docs build
    Building Data Docs...
    The following Data Docs sites were built:
    - local_site:
       file:///Users/dickens/my_pipeline/great_expectations/uncommitted/data_docs/local_site/index.html

``great_expectations docs build --site_name <YOUR_SITE>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, ``great_expectations docs build`` command builds all the Data Docs sites in a project.
If you wish to only build once site you may use the ``--site_name`` argument and pass in the name of the site from your configuration.

.. code-block:: bash

    $ great_expectations docs build --site_name s3_site
    Building Data Docs...
    The following Data Docs sites were built:
       - s3_site: https://s3.amazonaws.com/my-ge-bucket/index.html

``great_expectations docs clean``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``great_expectations docs clean`` command deletes your Data Docs site.
Specify site_name to delete specific site or all to delete all
To rebuild, just use the build command.

.. code-block:: bash

    $ great_expectations docs clean --site-name local_site

    $ great_expectations docs clean --all=y

great_expectations suite
==============================

All command line operations for working with expectation suites are here.

``great_expectations suite list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Running ``great_expectations suite list`` gives a list of available expectation suites in your project:

.. code-block:: bash

    $ great_expectations suite list
    3 expectation suites found:
        customer_requests.warning
        customer_requests.critical
        churn_model_input

``great_expectations suite new``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. attention::

  In the next major release ``suite new`` command will no longer create a sample suite.
  Instead, ``suite new`` will create an empty suite.
  Additionally the ``--empty`` flag will be deprecated.
  The existing behavior of automatic creation of a demo suite is now in the command ``suite demo``.

Create a new expectation suite.
Just as writing SQL queries is far better with access to data, so are writing expectations.
These are best written interactively against some data.

To this end, this command interactively helps you choose some data, creates the new suite, adds sample expectations to it, and opens up Data Docs.

.. important::

    The sample suites generated **are not meant to be production suites** - they are examples only.

    Great Expectations will choose a couple of columns and generate expectations about them to demonstrate some examples of assertions you can make about your data.

.. code-block:: bash

    $ great_expectations suite new
    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv

    Name the new expectation suite [npi.warning]:

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.

    Press Enter to continue
    :

    Generating example Expectation Suite...
    Building Data Docs...
    The following Data Docs sites were built:
    - local_site:
       file:///Users/dickens/Desktop/great_expectations/uncommitted/data_docs/local_site/index.html
    A new Expectation suite 'npi.warning' was added to your project

To edit this suite you can click the **How to edit** button in Data Docs, or run the command: ``great_expectations suite edit npi.warning``.
This will generate a jupyter notebook and allow you to add, remove or adjust any expectations in the sample suite.

.. important::

    Great Expectations generates working jupyter notebooks when you make new suites and edit existing ones.
    This saves you tons of time by avoiding all the necessary boilerplate.

    Because these notebooks can be generated at any time from the expectation suites (stored as JSON) you should **consider the notebooks to be entirely disposable artifacts**.

    They are put in your ``great_expectations/uncommitted`` directory and you can delete them at any time.

    Because they can expose actual data, we strongly suggest leaving them in the ``uncommitted`` directory to avoid potential data leaks into source control.


``great_expectations suite new --suite <SUITE_NAME>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already know the name of the suite you want to create you can skip one of the interactive prompts and specify the suite name directly.


.. code-block:: bash

    $ great_expectations suite new --suite npi.warning
    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv
    ... (same as above)


``great_expectations suite new --empty``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. attention::

  In the next major release ``suite new`` command will no longer create a sample suite.
  Instead, ``suite new`` will create an empty suite.
  Therefore the ``--empty`` flag will be deprecated.

If you prefer to skip the example expectations and start writing expectations in a new empty suite directly in a jupyter notebook, add the ``--empty`` flag.

.. code-block:: bash

    $ great_expectations suite new --empty
    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv

    Name the new expectation suite [npi.warning]: npi.warning
    A new Expectation suite 'npi.warning' was added to your project
    Because you requested an empty suite, we\'ll open a notebook for you now to edit it!
    If you wish to avoid this you can add the `--no-jupyter` flag.

    [I 14:55:15.992 NotebookApp] Serving notebooks from local directory: /Users/dickens/Desktop/great_expectations/uncommitted
    ... (jupyter opens)


``great_expectations suite delete --suite <SUITE_NAME>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already know the name of the suite you want to delete you can skip one of the interactive prompts and specify the suite name directly.


.. code-block:: bash

    $ great_expectations suite delete --suite npi.warning
    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv
    ... (same as above)


``great_expectations suite new --empty --no-jupyter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you prefer to disable Great Expectations from automatically opening the generated jupyter notebook, add the ``--no-jupyter`` flag.

.. code-block:: bash

    $ great_expectations suite new --empty --no-jupyter

    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv

    Name the new expectation suite [npi.warning]: npi.warning
    A new Expectation suite 'npi.warning' was added to your project
    To continue editing this suite, run jupyter notebook /Users/taylor/Desktop/great_expectations/uncommitted/npi.warning.ipynb

You can then run jupyter.


``great_expectations suite edit``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Edit an existing expectation suite.
Just as writing SQL queries is far better with access to data, so are authoring expectations.
These are best authored interactively against some data.
This best done in a jupyter notebook.

.. note::
  BatchKwargs define what data to use during editing.

   - When suites are created through the CLI, the original batch_kwargs are stored in a piece of metadata called a citation.
   - The edit command uses the most recent batch_kwargs as a way to know what data should be used for the interactive editing experience.
   - It is often desirable to edit the suite on a different chunk of data.
   - To do this you can edit the batch_kwargs in the generated notebook.

To this end, this command interactively helps you choose some data, generates a working jupyter notebook, and opens up that notebook in jupyter.

.. code-block:: bash

    $ great_expectations suite edit npi.warning
    [I 15:22:18.809 NotebookApp] Serving notebooks from local directory: /Users/dickens/Desktop/great_expectations/uncommitted
    ... (juypter runs)

.. important::

    Great Expectations generates working jupyter notebooks when you make new suites and edit existing ones.
    This saves you tons of time by avoiding all the necessary boilerplate.

    Because these notebooks can be generated at any time from the expectation suites (stored as JSON) you should **consider the notebooks to be entirely disposable artifacts**.

    They are put in your ``great_expectations/uncommitted`` directory and you can delete them at any time.

    Because they can expose actual data, we strongly suggest leaving them in the ``uncommitted`` directory to avoid potential data leaks into source control.


``great_expectations suite edit <SUITE_NAME> --no-jupyter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you prefer to disable Great Expectations from automatically opening the generated jupyter notebook, add the ``--no-jupyter`` flag.

.. code-block:: bash

    $ great_expectations suite edit npi.warning --no-jupyter
    To continue editing this suite, run jupyter notebook /Users/dickens/Desktop/great_expectations/uncommitted/npi.warning.ipynb

You can then run jupyter.


``great_expectations suite scaffold <SUITE_NAME>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To facilitate fast creation of suites this command helps you write boilerplate using simple heuristics.
Much like the ``suite new`` and ``suite edit`` commands, you will be prompted interactively to choose some data from one of your datasources.

.. important::

    The suites generated here **are not meant to be production suites** - they are scaffolds to build upon.

    Great Expectations will choose which expectations **might make sense** for a column based on the type and cardinality of the data in each selected column.

    You will definitely want to edit the suite to hone it after scaffolding.


To create a new suite called "npi_distribution" in a project that has a single files-based ``PandasDatasource``:

.. code-block:: bash

    $ great_expectations suite scaffold npi_distribution
    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : npi.csv
    ...jupyter opens

You'll then see jupyter open a scaffolding notebook.
Run the first cell in the notebook that loads the data.
You don't need to worry about what's happening there.

The next code cell in the notebook presents you with a list of all the columns found in your selected data.
To select which columns you want to scaffold expectations on, simply uncomment them to include them.

Run the next few code cells to see the scaffolded suite in Data Docs.

You may keep the scaffold notebook open and iterate on the included and excluded columns and expectations to get closer to the kind of suite you want.

.. important::

    Great Expectations generates working jupyter notebooks.
    This saves you tons of time by avoiding all the necessary boilerplate.

    Because these notebooks can be generated at any time from the expectation suites (stored as JSON) you should **consider the notebooks to be entirely disposable artifacts**.

    They are put in your ``great_expectations/uncommitted`` directory and you can delete them at any time.

Because the scaffolder is not very smart, you will want to edit this suite to tune the parameters and make any adjustments such as removing expectations that don't make sense for your use case.

``great_expectations suite scaffold <SUITE_NAME> --no-jupyter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you wish to skip opening the scaffolding notebook in juypter you can use this optional flag.

The notebook will be created in your ``great_expectations/uncommitted`` directory.

.. code-block:: bash

    suite scaffold npi_distributions --no-jupyter
    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : npi.csv
    To continue scaffolding this suite, run `jupyter notebook uncommitted/scaffold_npi_distributions.ipynb`



``great_expectations suite demo``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a sample expectation suite.
Just as writing SQL queries is far better with access to data, so are writing expectations.
These are best written interactively against some data.

To this end, this command interactively helps you choose some data, creates the new suite, adds sample expectations to it, and opens up Data Docs.

.. important::

    The sample suites generated **are not meant to be production suites** - they are examples only.

    Great Expectations will choose a couple of columns and generate expectations about them to demonstrate some examples of assertions you can make about your data.

.. code-block:: bash

    $ great_expectations suite demo
    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi.csv

    Name the new expectation suite [npi.warning]:

    Great Expectations will choose a couple of columns and generate expectations about them
    to demonstrate some examples of assertions you can make about your data.

    Press Enter to continue
    :

    Generating example Expectation Suite...
    Building Data Docs...
    The following Data Docs sites were built:
    - local_site:
       file:///Users/dickens/Desktop/great_expectations/uncommitted/data_docs/local_site/index.html
    A new Expectation suite 'npi.warning' was added to your project

To edit this suite you can click the **How to edit** button in Data Docs, or run the command: ``great_expectations suite edit npi.warning``.
This will generate a jupyter notebook and allow you to add, remove or adjust any expectations in the sample suite.

.. important::

    Great Expectations generates working jupyter notebooks when you make new suites and edit existing ones.
    This saves you tons of time by avoiding all the necessary boilerplate.

    Because these notebooks can be generated at any time from the expectation suites (stored as JSON) you should **consider the notebooks to be entirely disposable artifacts**.

    They are put in your ``great_expectations/uncommitted`` directory and you can delete them at any time.

    Because they can expose actual data, we strongly suggest leaving them in the ``uncommitted`` directory to avoid potential data leaks into source control.


great_expectations validation-operator
=======================================

All command line operations for working with :ref:`validation operators <validation_operators_and_actions>` are here.

``great_expectations validation-operator list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Running ``great_expectations validation-operator list`` gives a list of
validation operators configured in your project:

.. code-block:: bash

    $ great_expectations validation-operator list
    ... (YOUR VALIDATION OPERATORS)

``great_expectations validation-operator run``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two modes to run this command:

1. Interactive (good for development):
**************************************************************

Specify the name of the validation operator using the ``--name`` argument and
the name of the expectation suite using the ``--suite`` argument.

The cli will help you specify the batch of data that you want to validate
interactively.

If you want to call a validation operator to validate one batch of data against
one expectation suite, you can invoke this command:

``great_expectations validation-operator run --name <VALIDATION_OPERATOR_NAME> --suite <SUITE_NAME>``

Use the `--name` argument to specify the name of the validation operator you want to run. This has to be the name
of one of the validation operators configured in your project. You can list the names by calling
the ``great_expectations validation-operator list`` command or by examining the ``validation_operators`` section in your project's
``great_expectations.yml`` config file.

Use the `--suite` argument to specify the name of the expectation suite you want the validation operator to validate the
batch of data against. This has to be the name of one of the expectation suites that exist in your project. You can look up the names by calling
the `suite list` command.

The command will help you specify the batch of data that you want the validation operator to validate interactively.

.. code-block:: bash

    $ great_expectations validation-operator --name action_list_operator --suite npi.warning

    Let us help you specify the batch of data you want the validation operator to validate.

    Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)
    : data/npi_small.csv
    Validation succeeded!

2. Non-interactive (good for production):
**************************************************************

If you want run a validation operator non-interactively, use the `--validation_config_file` argument to specify the path of the validation configuration JSON file.

``great_expectations validation-operator run ----validation_config_file <VALIDATION_CONFIG_FILE_PATH>``

This file can be used to instruct a validation operator to validate multiple batches of data and use multiple expectation suites to validate each batch.

.. note::
    A validation operator can validate multiple batches of data and use multiple expectation suites to validate each batch.
    For example, you might want to validate 3 source files, with 2 tiers of suites each, one for a warning suite and one for a critical stop-the-presses hard failure suite.

This command exits with 0 if the validation operator ran and the "success" attribute in its return object is True.
Otherwise, the command exits with 1.

.. Tip:: This is an excellent way to use call Great Expectations from within your pipeline if your pipeline code can run shell commands.

A validation config file specifies the name of the validation operator in your project and
the list of batches of data that you want the operator to validate.
Each batch is defined using ``batch_kwargs``.
The ``expectation_suite_names`` attribute for each batch specifies the list of names of expectation suites that the validation
operator should use to validate the batch.

Here is an example validation config file:

.. code-block:: json

    {
      "validation_operator_name": "action_list_operator",
      "batches": [
        {
          "batch_kwargs": {
            "path": "/Users/me/projects/my_project/data/data.csv",
            "datasource": "my_filesystem_datasource",
            "reader_method": "read_csv"
          },
          "expectation_suite_names": ["suite_one", "suite_two"]
        },
        {
          "batch_kwargs": {
            "query": "SELECT * FROM users WHERE status = 1",
            "datasource": "my_redshift_datasource"
          },
          "expectation_suite_names": ["suite_three"]
        }
      ]
    }

.. code-block:: bash

    $ great_expectations validation-operator run --validation_config_file my_val_config.json
    Validation succeeded!


great_expectations datasource
==============================

All command line operations for working with :ref:`datasources <reference__core_concepts__datasources>` are here.
A datasource is a connection to data and a processing engine.
Examples of a datasource are:
- csv files processed in pandas or Spark
- a relational database such as Postgres, Redshift or BigQuery

``great_expectations datasource list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command displays a list of your datasources and their types.
These can be found in your ``great_expectations/great_expectations.yml`` config file.

.. code-block:: bash

    $ great_expectations datasource list
    [{'name': 'files_datasource', 'class_name': 'PandasDatasource'}]


``great_expectations datasource new``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This interactive command helps you connect to your data.

.. code-block:: bash

    $ great_expectations datasource list
    What data would you like Great Expectations to connect to?
    1. Files on a filesystem (for processing with Pandas or Spark)
    2. Relational database (SQL)
    : 1

    What are you processing your files with?
        1. Pandas
        2. PySpark
    : 1

    Enter the path (relative or absolute) of the root directory where the data files are stored.
    : data

    Give your new data source a short name.
     [data__dir]: npi_drops
    A new datasource 'npi_drops' was added to your project.

If you are using a database you will be guided through a series of prompts that collects and verifies connection details and credentials.


``great_expectations datasource delete <datasource_name>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This command deletes specified datasources.

.. code-block:: bash

    $ great_expectations datasource delete files_datasource


``great_expectations datasource profile``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For details on profiling, see this :ref:`reference document<profiling_reference>`

.. caution:: Profiling is a beta feature and is not guaranteed to be stable. YMMV

great_expectations checkpoint
==============================

A checkpoint is a bundle of one or more batches of data with one or more Expectation Suites.
A checkpoint can be as simple as one batch of data paired with one Expectation Suite.
A checkpoint can be as complex as many batches of data across different datasources paired with one or more Expectation Suites each.

.. tip::
    Checkpoints are an ideal way to embed Great Expectations into your pipeline or use Great Expectations adjacent to your pipeline.
    If you have shell access in your pipeline you can use the ``checkpoint run`` command.
    If you do not have shell access or prefer a python script you can use the ``checkpoint script`` command to generate a python file to run a checkpoint.

``great_expectations checkpoint new <CHECKPOINT> <SUITE>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Interactively configure a new checkpoint.

A checkpoint is stored in a yaml file in the ``great_expectations/checkpoints/`` directory in your project.
After creation, a checkpoint can be run with the ``great_expectations checkpoint run`` command.

Similar to other commands, this command interactively helps you choose some data for your checkpoint.

If your project has a suite named ``npi.warning`` and you wish to create a checkpoint called ``source_tables`` in your project you run this as follows:

.. code-block:: bash

    $ great_expectations checkpoint new source_tables npi.warning
    ...(Interactively choose some data)
    A checkpoint named `source_tables` was added to your project!
      - To edit this checkpoint edit the checkpoint file: great_expectations/checkpoints/source_tables.yml
      - To run this checkpoint run `great_expectations checkpoint run source_tables`

The new checkpoint file (``great_expectations/checkpoints/source_tables.yml``) will look like this:

.. code-block:: yaml

    # This checkpoint was created by the command `great_expectations checkpoint new`.
    #
    # It can be run with the `great_expectations checkpoint run` command.
    # You can edit this file to add batches of data and expectation suites.
    #
    # For more details please see
    # https://docs.greatexpectations.io/en/latest/command_line.html#great-expectations-checkpoint-new-checkpoint-suite
    validation_operator_name: action_list_operator
    # Batches are a list of batch_kwargs paired with a list of one or more suite
    # names. A checkpoint can have one or more batches. This makes deploying
    # Great Expectations in your pipelines easy!
    batches:
      - batch_kwargs:
          path: /Users/me/pipeline/source_files/npi.csv
          datasource: files_datasource
          reader_method: read_csv
        expectation_suite_names: # one or more suites may validate against a single batch
          - npi.warning

You can edit this file to add batches of data and expectation suites across your project. To find out more see this guide:  :ref:`how_to_guides__validation__how_to_add_validations_data_or_suites_to_a_checkpoint`

If you are using a SQL datasource you will be guided through a series of prompts that helps you choose a table or write a SQL query.

.. tip:: A custom SQL query can be very handy if for example you wanted to validate all records in a table with timestamps.

For example, imagine you want to protect a machine learning model that looks at insurance claims
from last 90 days to predict costs with a checkpoint named ``cost_model_protection``.
If you have built a suite called ``cost_model_assumptions`` and a have a postgres datasource with a ``claims`` table with an ``claim_timestamp`` column and you wanted to validate claims that occurred in the last 90 days you might do something like:

.. code-block:: bash

    $ great_expectations checkpoint new cost_model_protection cost_model_assumptions
    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Which table would you like to use? (Choose one)
    1. claims (table)
    Do not see the table in the list above? Just type the SQL query
    : SELECT * FROM claims WHERE claim_timestamp > now() - interval '90 day';
    A checkpoint named `cost_model_protection` was added to your project!
      - To edit this checkpoint edit the checkpoint file: great_expectations/checkpoints/cost_model_protection.yml
      - To run this checkpoint run `great_expectations checkpoint run cost_model_protection`

This checkpoint can then be run nightly before your model makes cost predictions!

``great_expectations checkpoint list``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
List the checkpoints found in the project.

.. code-block:: bash

    $ great_expectations checkpoint list
    Found 1 checkpoint.
    - my_checkpoint


``great_expectations checkpoint run <CHECKPOINT>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Run an existing checkpoint.

.. tip::
    This is an ideal way to embed Great Expectations into your pipeline or use it adjacent to your pipeline if you have shell access in your pipeline.
    If you do not have shell access or prefer a python script you can use the ``checkpoint script`` command to generate a python file to run a checkpoint.


This command will return posix status codes and print messages as follows:

+-------------------------------+-----------------+-----------------------+
| **Situation**                 | **Return code** | **Message**           |
+-------------------------------+-----------------+-----------------------+
| all validations passed        | 0               | Validation succeeded! |
+-------------------------------+-----------------+-----------------------+
| one or more validation failed | 1               | Validation failed!    |
+-------------------------------+-----------------+-----------------------+

If there is a checkpoint named ``source_tables`` in your project you can run it as follows:

.. code-block:: bash

    $ great_expectations checkpoint run source_tables
    Validation succeeded!

``great_expectations checkpoint script <CHECKPOINT>``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create an executable script for deploying a checkpoint via python.

This is provided as a convenience for teams whose pipeline deployments may not have shell access or may wish to have more flexibility when running a checkpoint.

To generate a script, you must first have an existing checkpoint.
To make a new checkpoint use ``great_expectations checkpoint new``.

.. code-block:: bash

    $ great_expectations checkpoint script cost_model_protection
    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    A python script was created that runs the checkpoint named: `cost_model_protection`
      - The script is located in `great_expectations/uncommitted/run_cost_model_protection.py`
      - The script can be run with `python great_expectations/uncommitted/run_cost_model_protection.py`

The generated script looks like this:

.. code-block:: python

    """
    This is a basic generated Great Expectations script that runs a checkpoint.

    A checkpoint is a list of one or more batches paired with one or more
    Expectation Suites and a configurable Validation Operator.

    Checkpoints can be run directly without this script using the
    `great_expectations checkpoint run` command. This script is provided for those
    who wish to run checkpoints via python.

    Data that is validated is controlled by BatchKwargs, which can be adjusted in
    the checkpoint file: great_expectations/checkpoints/cost_model_protection.yml.

    Data are validated by use of the `ActionListValidationOperator` which is
    configured by default. The default configuration of this Validation Operator
    saves validation results to your results store and then updates Data Docs.

    This makes viewing validation results easy for you and your team.

    Usage:
    - Run this file: `python great_expectations/uncommitted/run_cost_model_protection.py`.
    - This can be run manually or via a scheduler such as cron.
    - If your pipeline runner supports python snippets you can paste this into your
    pipeline.
    """
    import sys

    from great_expectations import DataContext

    # checkpoint configuration
    context = DataContext("/Users/taylor/Desktop/demo/great_expectations")
    checkpoint = context.get_checkpoint("cost_model_protection")

    # load batches of data
    batches_to_validate = []
    for batch in checkpoint["batches"]:
        batch_kwargs = batch["batch_kwargs"]
        for suite_name in batch["expectation_suite_names"]:
            suite = context.get_expectation_suite(suite_name)
            batch = context.get_batch(batch_kwargs, suite)
            batches_to_validate.append(batch)

    # run the validation operator
    results = context.run_validation_operator(
        checkpoint["validation_operator_name"],
        assets_to_validate=batches_to_validate,
    )

    # take action based on results
    if not results["success"]:
        print("Validation failed!")
        sys.exit(1)

    print("Validation succeeded!")
    sys.exit(0)

This script can be run by invoking it with:

.. code-block:: bash

    $ python great_expectations/uncommitted/run_cost_model_protection.py
    Validation Succeeded!

Just like the built in command ``great_expectations checkpoint run``, this posix-compatible script exits with a status of ``0`` if validation is successful and a status of ``1`` if validation failed.

A failure will look like:

.. code-block:: bash

    $ python great_expectations/uncommitted/run_cost_model_protection.py
    Validation failed!

Shell autocompletion for the CLI
=================================

If you want to enable autocompletion for the Great Expectations CLI, you can execute following commands in your shell (or add them to your .bashrc/.zshrc or ~/.config/fish/completions/):

.. code-block:: bash

   $ eval "$(_GREAT_EXPECTATIONS_COMPLETE=source_bash great_expectations)"

for bash

.. code-block:: zsh

   $ eval "$(_GREAT_EXPECTATIONS_COMPLETE=source_zsh great_expectations)"

for zsh, and

.. code-block:: fish

   $ eval (env _GREAT_EXPECTATIONS_COMPLETE=source_fish great_expectations)

for fish (you'll have to create a ~/.config/fish/completions/great_expectations.fish file).

Alternatively, if you don't want the eval command to slow down your shell startup time, you can instead add the commands as a script to your shell profile. For more info, see the official `Click documentation`_.

.. _Click documentation: https://click.palletsprojects.com/en/7.x/bashcomplete/


Miscellaneous
======================

* ``great_expectations project check-config`` checks your ``great_expectations/great_expectations.yml`` for validity. This is handy for occasional Great Expectations version migrations.

Acknowledgements
======================

This article was heavily inspired by the phenomenal Rails Command Line Guide https://guides.rubyonrails.org/command_line.html.
