.. _command_line:

###################################
The Great Expectations Command Line
###################################

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
This is a list of the most common commands you'll use in order of how much you'll probably use them:

* ``great_expectations suite edit``
* ``great_expectations suite new``
* ``great_expectations suite list``
* ``great_expectations docs build``
* ``great_expectations tap new``
* ``great_expectations datasource list``
* ``great_expectations datasource new``
* ``great_expectations datasource profile``
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

      In addition, the CLI supports the following special commands:

      - great_expectations init : same as `project new`
      - great_expectations datasource profile : profile a  datasource
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

Create a new expectation suite.
Just as writing SQL queries is far better with access to data, so are writing expectations.
These are best written interactively against some data.

To this end, this command interactively helps you choose some data, creates the new suite, adds sample expectations to it, and opens up Data Docs.

.. important::

    The sample suites generated **are not meant to be production suites** - they are examples only.

    Great Expectations will choose a couple of columns and generate expectations about them to demonstrate some examples of assertions you can make about your data.

.. code-block:: bash

    $ great_expectations suite new
    Enter the path (relative or absolute) of a data file
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
    Enter the path (relative or absolute) of a data file
    : data/npi.csv
    ... (same as above)


``great_expectations suite new --empty``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you prefer to skip the example expectations and start writing expectations in a new empty suite directly in a jupyter notebook, add the ``--empty`` flag.

.. code-block:: bash

    $ great_expectations suite new --empty
    Enter the path (relative or absolute) of a data file
    : data/npi.csv

    Name the new expectation suite [npi.warning]: npi.warning
    A new Expectation suite 'npi.warning' was added to your project
    Because you requested an empty suite, we\'ll open a notebook for you now to edit it!
    If you wish to avoid this you can add the `--no-jupyter` flag.

    [I 14:55:15.992 NotebookApp] Serving notebooks from local directory: /Users/dickens/Desktop/great_expectations/uncommitted
    ... (jupyter opens)


``great_expectations suite new --empty --no-jupyter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you prefer to disable Great Expectations from automatically opening the generated jupyter notebook, add the ``--no-jupyter`` flag.

.. code-block:: bash

    $ great_expectations suite new --empty --no-jupyter

    Enter the path (relative or absolute) of a data file
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


great_expectations datasource
==============================

All command line operations for working with :ref:`datasources <datasource>` are here.
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


``great_expectations datasource profile``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For details on profiling, see this :ref:`reference document<profiling_reference>`

.. caution:: Profiling is a beta feature and is not guaranteed to be stable. YMMV

great_expectations tap
=======================

All command line operations for working with taps are here.
A tap is an executable python file that runs validations that you can create to aid deployment of validations.

``great_expectations tap new``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creating a tap requires a valid suite name and tap filename.
This is the name of a python file that this command will write to.


.. note::
    Taps are a beta feature to speed up deployment.
    Please
    `open a new issue <https://github.com/great-expectations/great_expectations/issues/new>`__
    if you discover a use case that does not yet work
    or have ideas how to make this feature better!

.. code-block:: bash

    $ great_expectations tap new npi.warning npi.warning.py
    This is a BETA feature which may change.

    Enter the path (relative or absolute) of a data file
    : data/npi.csv
    A new tap has been generated!
    To run this tap, run: python npi.warning.py
    You can edit this script or place this code snippet in your pipeline.

You will now see a new tap file on your filesystem.

This can be run by invoking it with:

.. code-block:: bash

    $ python  npi.warning.py
    Validation Suceeded!
    $ echo $?
    0

This posix-compatible exits with a status of ``0`` if validation is successful and a status of ``1`` if validation failed.

A failure will look like:

.. code-block:: bash

    $ python  npi.warning.py
    Validation Failed!
    $ echo $?
    1

The :ref:`Typical Workflow <Typical Workflow>` document shows you how taps can be embedded in your existing pipeline or used adjacent to a pipeline.

If you are using a SQL datasource you will be guided through a series of prompts that helps you choose a table or write a SQL query.

.. tip::

	 A custom SQL query can be very handy if for example you wanted to validate all records in a table with timestamps.

For example, imagine you have a machine learning model that looks at the last 14 days of customer events to predict churn.
If you have built a suite called ``churn_model_assumptions`` and a postgres database with a ``user_events`` table with an ``event_timestamp`` column and you wanted to validate all events that occurred in the last 14 days you might do something like:

.. code-block:: bash

    $ great_expectations tap new churn_model_assumptions churn_model_assumptions.py
    This is a BETA feature which may change.

    Which table would you like to use? (Choose one)
    1. user_events (table)
    Don't see the table in the list above? Just type the SQL query
    : SELECT * FROM user_events WHERE event_timestamp > now() - interval '14 day';
    A new tap has been generated!
    To run this tap, run: python churn_model_assumptions.py
    You can edit this script or place this code snippet in your pipeline.

This tap can then be run nightly before your model makes churn predictions!

Miscellaneous
======================

* ``great_expectations project check`` checks your ``great_expectations/great_expectations.yml`` for validity. This is handy for occasional Great Expectations version migrations.

Acknowledgements
======================

This article was heavily inspired by the phenomenal Rails Command Line Guide https://guides.rubyonrails.org/command_line.html.
