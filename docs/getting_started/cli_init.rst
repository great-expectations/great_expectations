.. _tutorial_init:

Run ``great_expectations init``
===============================================

Video
------

..

Watch `the video on YouTube <https://greatexpectations.io/videos/getting_started/cli_init>`_.


Default Project Structure
----------------------------------------

Great Expectations provides a default project framework that simplifies operations such as connecting to data sources;
fetching, profiling and validating batches of data; and compiling to human-readable documentation.

This tutorial uses example data from the United States Centers for Medicare and Medicaid Services `National Provider
Identifier Standard <https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/DataDissemination.html>`_
(NPI). If you want to follow along with this exact example, start with:

.. code-block:: bash

    git clone https://github.com/superconductive/ge_example_project.git
    cd example-dickens-ge_example_project-project

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
    │   ├── pandas
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   ├── spark
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   └── sql
    │       ├── create_expectations.ipynb
    │       └── validations_playground.ipynb
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   └── local_site
        ├── samples
        └── validations


Adding Datasources
----------------------------------------

Next, the CLI will ask you if you want to configure a Datasource.

Datasources allow you to configure connections to data to evaluate Expectations. Great Expectations currently supports
native evaluation of Expectations in three compute environments:

1. Pandas DataFrames
2. Relational databases via SQL Alchemy
3. Spark DataFrames

A Datasource could be a local pandas environment with some configuration to parse CSV files from a directory; a
connection to postgresql instance; a Spark cluster connected to an S3 bucket; etc. In the future, we plan to add
support for other compute environments, such as dask. (If you'd like to use or contribute to those environments,
please chime in on `GitHub issues <https://github.com/great-expectations/great_expectations/issues>`_.)

Our example project has a ``data/`` folder containing several CSVs. Within the CLI, we can configure a Pandas DataFrame
Datasource like so:

.. code-block:: bash

    ========== Datasources ==========

    See https://docs.greatexpectations.io/en/latest/features/datasource.html for more information about datasources.


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



This step adds a new block for Datasource configuration to ``great_expectations/great_expectations.yml``. Don't worry
about these details yet. For now, it's enough to know that we've configured a Datasource and the configuration
information is stored in this file.

.. code-block:: bash

    datasources:
      data__dir:
        class_name: PandasDatasource
        data_asset_type:
          class_name: PandasDataset
        generators:
          default:
            class_name: SubdirReaderGenerator
            base_directory: ../data
            reader_options:
              sep:
              engine: python

For a SQL data source, configuration would look like this instead:

.. code-block:: bash

    ========== Datasources ==========

    See https://docs.greatexpectations.io/en/latest/features/datasource.html for more information about datasources.


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


The corresponding config would be:

.. code-block:: bash

    datasources:
      my_db:
        class_name: SqlAlchemyDatasource
        credentials: ${my_db}
        data_asset_type:
          class_name: SqlAlchemyDataset
        generators:
          default:
            class_name: TableGenerator

Note: the SQL credentials you entered are stored in the ``uncommitted/config_variables.yml`` file.
Note that this file goes in the ``uncommitted/`` directory, which should *NOT* be committed to source control.
The ${my_db} variable is substituted with the credentials at runtime.

A Great Expectations Datasource brings the worlds of data and expectations together. Datasources produce
Great Expectations DataAssets, which support the core GE api, including validation.
Fully describing a DataAsset's "name" requires three parts:

1. ``datasource`` (`my_postgresql_db`)
2. ``generator`` (`queries`)
3. ``generator_asset`` (`user_events_table`)

In addition, to work with a specific batch of data and validate it against a particular set of expectations, you will
need to specify:

* ``batch_kwargs`` (`SELECT * FROM user_events_table WHERE created_at>2018-01-01`), and/or
* ``expectation_suite_name`` (`BasicDatasetProfiler`).

Together, these five elements completely allow you to reference all of the main entities within the DataContext.

You can get started in Great Expectations without learning all the details of the DataContext. To start, you'll mainly
use elements 1 and 3: ``datasource``s, (with names such as  `my_postgresql_db`) and ``generator_asset``s, which may
conceptually be similar to a `user_events_table`, for example.

Configuring Slack Notifications
----------------------------------------

Great Expectations can post messages to a Slack channel each time a dataset is validated. This helps teams to monitor
data quality in their pipeline in real time. Here is what these messages look like:

.. image:: ../images/validation_result_slack_message_example.png
    :width: 400px

The ``great_expectations init`` command prompts you to enter a Slack webhook URL to enable this functionality.

Obtaining this URL is easy. This article walks you through the steps:
`Incoming Webhooks For Slack <https://slack.com/help/articles/115005265063-incoming-webhooks-for-slack>`_

Since Slack webhook URLs are security credentials, we store them in the ``uncommitted/config_variables.yml`` file that
will not be checked in into your source control. The config property name is `validation_notification_slack_webhook`

If you don't have a Slack webhook URL right now, you can decline the ``init`` command's prompt and configure this
feature later.

Profiling data
----------------------------------------

Now that we've configured a DataSource, the next step is to profile it. Profiling will generate a very loose set of
Expectations for your data. By default, they will cover a wide range of statistics and other characteristics
of the Dataset that could be useful for future validation and data exploration.

Profiling will also evaluate those Expectations against your actual data, producing a set of Expectation
Validation Results (EVRs), which will contain observed values and other context derived from the data itself.

Profiling results can provide a lot of useful information for creating the Expectations you will
use later. They also provide the raw materials for first-pass data documentation. For more details on profiling,
please see :ref:`profiling`.

Within the CLI, it's easy to profile our data.

Warning: For large data sets, the current default profiler may run slowly and impose significant I/O and compute load.
Be cautious when executing against shared databases.

.. code-block:: bash

    ========== Profiling ==========

    Profiling 'data__dir' will create expectations and documentation.

    Please note: Profiling is still a beta feature in Great Expectations.  The current profiler will evaluate the entire 
    data source (without sampling), which may be very time consuming. 
    As a rule of thumb, we recommend starting with data smaller than 100MB.

    To learn more about profiling, visit https://docs.greatexpectations.io/en/latest/reference/profiling.html

    Found 1 data assets from generator default

    Would you like to profile 'data__dir'?
     [Y/n]:
    Profiling 'data__dir' with 'BasicDatasetProfiler'
    Profiling all 1 data assets from generator default
        Profiling 'npidata'...
                Preparing column 1 of 329: NPI
                Preparing column 2 of 329: Entity Type Code
    ...
    ...
                Preparing column 329 of 329: Healthcare Provider Taxonomy Group_15
        2039 expectation(s) included in expectation_suite.
        Profiled 329 columns using 18877 rows from npidata (17.647 sec)

    Profiled 1 of 1 named data assets, with 18877 total rows and 329 columns in 17.65 seconds.
    Generated, evaluated, and stored 2039 Expectations. Please review results using data-docs.

The default profiler (``BasicDatasetProfiler``) will add two JSON files in your ``great_expectations/`` directory.
They will be placed in subdirectories that include the three components of names described above. Great
Expectations' DataContexts can fetch these objects by name, so you won't usually need to access these files directly.
Still, it's useful to see how they're stored, to get a sense for how namespaces work.

.. code-block:: bash

    great_expectations
    ├── .gitignore
    ├── datasources
    ├── expectations
    │   └── data__dir
    │       └── default
    │           └── npidata
    │               └── BasicDatasetProfiler.json
    ├── fixtures
    ├── great_expectations.yml
    ├── notebooks
    │   ├── pandas
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   ├── spark
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   └── sql
    │       ├── create_expectations.ipynb
    │       └── validations_playground.ipynb
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   ├── local_site
        │   └── team_site
        ├── samples
        └── validations
            └── profiling
                └── data__dir
                    └── default
                        └── npidata
                            └── BasicDatasetProfiler.json


We won't go into full detail on the contents of Expectation and EVR objects here. But as a quick illustration,
Expectation Suite JSON objects consist mainly of Expectations like:

.. code-block:: json

    {
      "expectation_type": "expect_column_distinct_values_to_be_in_set",
      "kwargs": {
        "column": "Entity Type Code",
        "value_set": null,
        "result_format": "SUMMARY"
      },
      "meta": {
        "BasicDatasetProfiler": {
          "confidence": "very low"
        }
      }
    }

Expectation Suites created by the BasicDatasetProfiler are very loose and unopinionated. (Hence, the null
``value_set`` parameter.) They are more like placeholders for Expectations than actual Expectations.
(A tighter Expectation might include something like ``value_set=[1, 2]``.) That said, even these loose
Expectations can be evaluated against data to produce EVRs.

EVRs contain Expectations, *plus* validation results from a evaluation against a specific batch of data.

.. code-block:: bash

    {
        "success": true,
        "result": {
            "observed_value": [
                1.0,
                2.0
            ],
            "element_count": 18877,
            "missing_count": 382,
            "missing_percent": 2.023626635588282,
            "details": {
                "value_counts": [
                    {
                        "value": 1.0,
                        "count": 15689
                    },
                    {
                        "value": 2.0,
                        "count": 2806
                    }
                ]
            }
        },
        "expectation_config": {
            "expectation_type": "expect_column_distinct_values_to_be_in_set",
            "kwargs": {
                "column": "Entity Type Code",
                "value_set": null,
                "result_format": "SUMMARY"
            },
            "meta": {
                "BasicDatasetProfiler": {
                    "confidence": "very low"
                }
            }
        },
        "exception_info": {
            "raised_exception": false,
            "exception_message": null,
            "exception_traceback": null
        }
    }

The full Expectation Suite and EVR are JSON objects that also contain additional metadata, which we won't go into here.
For more information about these objects please see :ref:`validation_result`.

Data Docs
----------------------------------------------------------

Expectation Suites and EVR's contain a huge amount of useful information about your data, but they aren't very easy to
consume as JSON objects. To make them more accessible, Great Expectations provides tools to render Expectation Suites
and EVRs to documentation.

We call this feature "Compile to Docs."  This approach to documentation has two significant advantages.

First, for engineers, Compile to Docs makes it possible to automatically keep your documentation in sync with your
tests. This prevents documentation rot and can save a huge amount of time on otherwise unrewarding document maintenance.

Second, the ability to translate Expectations back and forth betwen human- and machine-readable formats opens up
many opportunities for domain experts and stakeholders who aren't engineers to collaborate more closely with
engineers on data applications.

Within the CLI, we compile to documentation as follows:

.. code-block:: bash

    ========== Data Docs ==========

    Great Expectations can create data documentation from the data you just profiled.

    To learn more: https://docs.greatexpectations.io/en/latest/features/data_docs.html

    Build HTML Data Docs? [Y/n]:

    Building Data Docs...
        ...

    The following data documentation HTML sites were generated:

    local_site:
       great_expectations/uncommitted/data_docs/local_site/index.html


Opening `great_expectations/uncommitted/data_docs/local_site/index.html` in a browser will give you a page like:

.. image:: ../images/index_render.png

Clicking through to the profiling results will present an overview of the data, built from expectations and validated
using the batch that was just profiled.

.. image:: ../images/profiling_render.png

Clicking through to the second link will show you descriptive data documentation. This renders the full content of validation results, not just the Expectations themselves.

.. image:: ../images/prescriptive_render.png


Note also that the default ``great_expectations/`` setup stores compiled documentation in the ``uncommitted/data_docs/``
directory, with a subdirectory structure that mirrors the project namespace.

After the init command completes, you should see the following directory structure :

.. code-block:: bash

    great_expectations
    ├── .gitignore
    ├── datasources
    ├── expectations
    │   └── data__dir
    │       └── default
    │           └── npidata
    │               └── BasicDatasetProfiler.json
    ├── fixtures
    ├── great_expectations.yml
    ├── notebooks
    │   ├── pandas
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   ├── spark
    │   │   ├── create_expectations.ipynb
    │   │   └── validations_playground.ipynb
    │   └── sql
    │       ├── create_expectations.ipynb
    │       └── validations_playground.ipynb
    ├── plugins
    └── uncommitted
        ├── config_variables.yml
        ├── documentation
        │   └── local_site
        │       ├── expectations
        │       │   └── data__dir
        │       │       └── default
        │       │           ├── npidata
        │       │           │   └── BasicDatasetProfiler.html
        │       ├── index.html
        │       └── validations
        │           └── profiling
        │               └── data__dir
        │                   └── default
        │                       └── npidata
        │                           └── BasicDatasetProfiler.html
        └── validations
            └── profiling
                └── data__dir
                    └── default
                        └── npidata
                            └── BasicDatasetProfiler.json


Next Steps
-----------

Once you have opened datadocs, a prompt will suggest possible next steps, such as to :ref:`tutorial_create_expectations` or
:ref:`tutorial_validate_data`.
