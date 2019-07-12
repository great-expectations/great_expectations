.. _tutorial_init:

Step 1: Running ``great_expectations init``
===============================================

.. toctree::
   :maxdepth: 2


Video
------

.. <<< NEEDS UTM>>>

Watch `the video on YouTube <https://youtu.be/TlTxVyyDunQ>`_.


Mildly opinionated scaffolding
----------------------------------------

Great Expectations provides a mildly opinionated deployment framework that simplifies operations such as connecting to data sources; fetching, profiling and validating batches of data; and compiling to human-readable documentation.

This tutorial uses a toy project called ``example_dickens_data_project``, but the same methods should work for most data projects. If you want to follow along with this exact example, start with:

.. code-block::

    git clone https://github.com/superconductive/example-dickens-data-project.git
    cd example-dickens-data-project

By default, everything in the Great Expectations deployment framework will be expressed in a directory structure within a ``great_expectations/`` folder within your version control system. To create this folder, navigate to the root of your project directory in a terminal and run:

.. code-block::

    great_expectations init

The command line interface (CLI) will scaffold and populate the configuration and other artifacts necessary to get started with Great Expectations.

.. code-block::

    $ great_expectations init
      _____                _   
     / ____|              | |  
    | |  __ _ __ ___  __ _| |_ 
    | | |_ | '__/ _ \/ _` | __|
    | |__| | | |  __/ (_| | |_ 
     \_____|_|  \___|\__,_|\__|
                               
                               
     ______                      _        _   _                 
    |  ____|                    | |      | | (_)                
    | |__  __  ___ __   ___  ___| |_ __ _| |_ _  ___  _ __  ___ 
    |  __| \ \/ / '_ \ / _ \/ __| __/ _` | __| |/ _ \| '_ \/ __|
    | |____ >  <| |_) |  __/ (__| || (_| | |_| | (_) | | | \__ \
    |______/_/\_\ .__/ \___|\___|\__\__,_|\__|_|\___/|_| |_|___/
                | |                                             
                |_|                                             
    


    Always know what to expect from your data.

    If you're new to Great Expectations, this tutorial is a good place to start:

        https://docs.greatexpectations.io/en/latest/getting_started/cli_init.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop


    Let's add Great Expectations to your project, by scaffolding a new great_expectations directory:

        great_expectations
            ├── great_expectations.yml
            ├── datasources
            ├── expectations
            ├── fixtures
            ├── notebooks
            ├── plugins
            ├── uncommitted
            │   ├── validations
            │   ├── credentials
            │   ├── documentation
            │   └── samples
            └── .gitignore

    OK to proceed?
    [Y/n]: Y

    Done.


If you inspect the ``great_expectations/`` directory at this point, it should contain:

.. code-block::

    great_expectations/
    ├── datasources
    ├── expectations
    ├── fixtures
    ├── great_expectations.yml
    ├── notebooks
    │   ├── create_expectations.ipynb
    │   └── integrate_validation_into_pipeline.ipynb
    ├── plugins
    ├── .gitignore
    └── uncommitted
        ├── credentials
        ├── documentation
        ├── samples
        └── validations

    10 directories, 3 files


Adding Datasources
----------------------------------------

Next, the CLI will ask you if you want to configure a Datasource.

Datasources allow you to configure connections to data to evaluate Expectations. Great Expectations currently supports native evaluation of Expectations in three compute environments:

1. Pandas DataFrames
2. Relational databases via SQL Alchemy
3. Spark DataFrames

Therefore, a Datasource could be a local pandas environment with some configuration to parse CSV files from a directory; a connection to postgresql instance; a spark cluster connected to an S3 bucket; etc. In the future, we plan to add support for other compute environments, such as dask and BigQuery. (If you'd like to use or contribute to those environments, please chime in on `GitHub issues <https://github.com/great-expectations/great_expectations/issues>`_.)

Our example project has a ``data/`` folder containing several CSVs. Within the CLI, we can configure a Pandas DataFrame Datasource like so:

.. code-block::

    ========== Datasources ==========

    See https://docs.greatexpectations.io/en/latest/core_concepts/datasource.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop for more information about datasources.


    Configure a datasource:
        1. Pandas DataFrame
        2. Relational database (SQL)
        3. Spark DataFrame
        4. Skip datasource configuration
    : 1
    1

    Enter the path of the root directory where the data files are stored.
    (The path may be either absolute or relative to current directory.)
    : data

    Give your new data source a short name.
    [data__dir]: 



This step adds a new block for Datasource configuration to ``great_expectations/great_expectations.yml``. Don't worry about these details yet. For now, it's enough to know that we've configured a Datasource and the configuration information is stored in this file.

.. code-block::

    datasources:
        data__dir:
            type: pandas
            generators:
                default:
                    type: subdir_reader
                    base_directory: ../data
                    reader_options:
                        sep:
                        engine: python


For a SQL data source, configuration would look like this instead:

.. code-block::

    ========== Datasources ==========

    See https://docs.greatexpectations.io/en/latest/core_concepts/datasource.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop for more information about datasources.


    Configure a datasource:
        1. Pandas DataFrame
        2. Relational database (SQL)
        3. Spark DataFrame
        4. Skip datasource configuration
    : 2
    2

    Give your new data source a short name.
    [mydb]: my_db

    Great Expectations relies on sqlalchemy to connect to relational databases.
    Please make sure that you have it installed.

    Next, we will configure database credentials and store them in the "my_db" section
    of this config file: great_expectations/uncommitted/credentials/profiles.yml:

    What is the driver for the sqlalchemy connection? [postgres]: postgres
    What is the host for the sqlalchemy connection? [localhost]: my_db_host.internal.priv
    What is the port for the sqlalchemy connection? [5432]:  
    What is the username for the sqlalchemy connection? [postgres]: user
    What is the password for the sqlalchemy connection?: 
    What is the database name for the sqlalchemy connection? [postgres]: 
    Creating new profiles store at /home/user/my_project/great_expectations/uncommitted/credentials/profiles.yml


The corresponding config would be:

.. code-block::

    datasources:
        my_db:
            type: sqlalchemy
            generators:
                default:
                    type: queries
            profile: my_db

Note: the CLI will also create a ``uncommitted/credentials/profiles.yml`` files to contain SQL credentials. Note that this file goes in the ``uncommitted/`` directory, which should *NOT* be committed to source control.

Strictly speaking, a Great Expectations Datasource is not the data itself, but part of a *pointer* to a data compute environment where Expectations can be evaluated, called a `DataAsset.` Fully describing the pointer requires a 3-ple:

1. ``datasource_name`` (`my_postgresql_db`)
2. ``generator_name`` (`queries`)
3. ``generator_asset`` (`user_events_table`)

In addition, for some operations you will need to specify:

* ``batch_id`` (`SELECT * FROM user_events_table WHERE created_at>2018-01-01`), and/or
* ``expectation_suite_name`` (`BasicDatasetProfiler`).

Together, these five elements completely allot you to reference all of the main entities within the DataContext.

You can get started in Great Expectations without learning all the details of the DataContext. To start, you'll mainly use elements 1 and 3: ``datasource_names``, like `my_postgresql_db` and ``generator_assets``, like `user_events_table`. For most users, these names are already familiar and intuitive. From there, Great Expectations' defaults can usually fill in the gaps.


Profiling data
----------------------------------------

Now that we've configured a DataSource, the next step is to profile it. Profiling will generate a first set of candidate Expectations for your data. By default, they will cover a wide range of statistics and other characteristics of the Dataset that could be useful for future validation.

Profiling will also evaluate these candidate Expectations against your actual data, producing a set of Expectation Validation Results (EVRs), which will contain observed values and other context derived from the data itself.

Together, profiled Expectations and EVRs provide a lot of useful information for creating the Expectations you will use in production. They also provide the raw materials for first-pass data documentation. For more details on profiling, please see :ref:`profiling`.

Within the CLI, it's easy to profile our data.

Warning: For large data sets, the current default profiler may run slowly and impose significant I/O and compute load. Be cautious when executing against shared databases.

.. code-block::

    ========== Profiling ==========

    Would you like to profile 'data__dir' to create candidate expectations and documentation?

    Please note: Profiling is still a beta feature in Great Expectations.  The current profiler will evaluate the entire 
    data source (without sampling), which may be very time consuming. 
    As a rule of thumb, we recommend starting with data smaller than 100MB.

    To learn more about profiling, visit https://docs.greatexpectations.io/en/latest/guides/profiling.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0.
            
    Proceed? [Y/n]: Y
    Profiling 'data__dir' with 'BasicDatasetProfiler'
    Found 1 data assets using generator 'default'
    Profiling all 1.
        Profiling 'notable_works_by_charles_dickens'...
        Profiled 3 columns using 38 rows from notable_works_by_charles_dickens (0.132 sec)

    Profiled 1 of 1 named data assets, with 38 total rows and 3 columns in 0.13 seconds.
    Generated, evaluated, and stored 27 candidate Expectations.
    Note: You will need to review and revise Expectations before using them in production.

    Done.

    Profiling results are saved here:
    /home/user/example-dickens-data-project/great_expectations/uncommitted/validations/2019-07-12T085507.080557Z/data__dir/default/notable_works_by_charles_dickens/BasicDatasetProfiler.json

The default profiler (``BasicDatasetProfiler``) will add two JSON files in your ``great_expectations/`` directory. They will be placed in subdirectories that following our namespacing conventions. Great Expectations' DataContexts can fetch these objects by name, so you won't usually need to access these files directly. Still, it's useful to see how they're stored, to get a sense for how namespaces work.

.. code-block::

    great_expectations/
    ├── datasources
    ├── expectations
    │   └── data__dir
    │       └── default
    │           └── notable_works_by_charles_dickens
    │               └── BasicDatasetProfiler.json
    ├── fixtures
    ├── great_expectations.yml
    ├── notebooks
    │   ├── create_expectations.ipynb
    │   └── integrate_validation_into_pipeline.ipynb
    ├── plugins
    └── uncommitted
        ├── credentials
        ├── documentation
        ├── samples
        └── validations
            └── 2019-07-12T090442.066278Z
                └── data__dir
                    └── default
                        └── notable_works_by_charles_dickens
                            └── BasicDatasetProfiler.json

    17 directories, 5 files


We won't go into full detail on the contents of Expectation and EVR objects here. But as a quick illustration, Expectation Suite JSON objects consist mainly of Expectations like:

.. code-block::

    {
      "expectation_type": "expect_column_values_to_be_in_set",
      "kwargs": {
        "column": "Type",
        "value_set": [],
        "result_format": "SUMMARY"
      },
      "meta": {
        "BasicDatasetProfiler": {
          "confidence": "very low"
        }
      }
    }

Expectation Suites created by the BasicDatasetProfiler are very loose and unopinionated. (Hence, the empty ``value_set`` parameter.) They are more like placeholders for Expectations than actual Expectations. (A tighter Expectation might include something like ``value_set=["Novel", "Short Story", "Novella"]``.) That said, even these loose Expectations can be evaluated against data to produce EVRs.

EVRs contain Expectations, *plus* validation results from a evaluation against a specific batch of data.

.. code-block::

    {
      "success": false,
      "result": {
        "element_count": 38,
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_count": 38,
        "unexpected_percent": 1.0,
        "unexpected_percent_nonmissing": 1.0,
        "partial_unexpected_list": [
          "Short Stories",
          "Novel",
          "Short Stories",
          ...
        ],
        "partial_unexpected_index_list": [
          0,
          1,
          ...
          19
        ],
        "partial_unexpected_counts": [
          {
            "value": "Novel",
            "count": 14
          },
          {
            "value": "Short Story",
            "count": 9
          },
          {
            "value": "Novella",
            "count": 5
          },
          ...
        ]
      },
      "exception_info": {
        "raised_exception": false,
        "exception_message": null,
        "exception_traceback": null
      },
      "expectation_config": {
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
          "column": "Type",
          "value_set": [],
          "result_format": "SUMMARY"
        },
        "meta": {
          "BasicDatasetProfiler": {
            "confidence": "very low"
          }
        }
      }
    }

The full Expectation Suite and EVR are JSON objects that also contain additional metadata, which we won't go into here. For more information about these objects please see :ref:`validation_result`.

Data documentation
----------------------------------------------------------

Expectation Suites and EVR's contain a huge amount of useful information about your data, but they aren't very easy to consume as JSON objects. To make them more accessible, Great Expectations provides tools to render Expectation Suites and EVRs to documentation.

We call this feature "Compile to Docs."  This approach to documentation has two significant advantages.

First, for engineers, Compile to Docs makes it possible to automatically keep your documentation in sync with your tests. This prevents documentation rot and can save a huge amount of time on otherwise unrewarding document maintenance.

Second, the ability to translate Expectations back and forth betwen human- and machine-readable formats opens up many opportunities for domain experts and stakeholders who aren't engineers to collaborate more closely with engineers on data applications.

Within the CLI, we compile to documentation as follows:

.. code-block::

    ========== Data Documentation ==========

    To generate documentation from the data you just profiled, the profiling results should be moved from 
    great_expectations/uncommitted (ignored by git) to great_expectations/fixtures.

    Before committing, please make sure that this data does not contain sensitive information!

    To learn more: https://docs.greatexpectations.io/en/latest/guides/data_documentation.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop

    Move the profiled data and build HTML documentation? [Y/n]: Y

    Moving files...

    Done.

    Building documentation...

    To view the generated data documentation, open this file in a web browser:
        great_expectations/uncommitted/documentation/index.html


    To create expectations for your data, start Jupyter and open a tutorial notebook:

    To launch with jupyter notebooks:
        jupyter notebook great_expectations/notebooks/create_expectations.ipynb

    To launch with jupyter lab:
        jupyter lab great_expectations/notebooks/create_expectations.ipynb

Opening `great_expectations/uncommitted/documentation/index.html` in a browser will give you a page like:

.. image:: ../index_render.png

Clicking through to the first link will show you prescriptive data documentation. This renders the Expectation Suite itself.

.. image:: ../prescriptive_render.png

Clicking through to the second link will show you descriptive data documentation. This renders the full content of validation results, not just the Expectations themselves.

.. image:: ../descriptive_render.png


Note that the CLI moved our EVRs from

.. code-block::

    uncommitted/validations/2019-07-12T090442.066278Z/data__dir/default/notable_works_by_charles_dickens/

to

.. code-block::
    
    fixtures/validations/2019-07-12T090442.066278Z/data__dir/default/notable_works_by_charles_dickens/

This is because this data documentation is intended to act as the source of truth for Expectations within this project: all users at the same point within the version control system (e.g. the same git hash) should be able to render exactly the same documentation from shared assets within version control.

Note also that the default ``great_expectations/`` setup does NOT commit compiled docs themselves within version control. Instead, they live in ``uncommitted/documentation/``, with a subdirectory structure that mirrors the project namespace.

.. code-block::

    great_expectations/
    ├── datasources
    ├── expectations
    │   └── data__dir
    │       └── default
    │           └── notable_works_by_charles_dickens
    │               └── BasicDatasetProfiler.json
    ├── fixtures
    │   └── validations
    │       └── data__dir
    │           └── default
    │               └── notable_works_by_charles_dickens
    │                   └── BasicDatasetProfiler.json
    ├── great_expectations.yml
    ├── notebooks
    │   ├── create_expectations.ipynb
    │   └── integrate_validation_into_pipeline.ipynb
    ├── plugins
    └── uncommitted
        ├── credentials
        ├── documentation
        │   ├── data__dir
        │   │   └── default
        │   │       └── notable_works_by_charles_dickens
        │   │           └── BasicDatasetProfiler.html
        │   └── index.html
        ├── samples
        └── validations
            └── 2019-07-12T090442.066278Z
                └── data__dir
                    └── default
                        └── notable_works_by_charles_dickens

    24 directories, 7 files


