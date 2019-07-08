.. _tutorial_init:

Step 1: Running `great_expectations init`
===============================================

.. toctree::
   :maxdepth: 2


Video
------

https://www.loom.com/share/8bbe5d958ca348a9b072a1c63f3f127e


Opinionated scaffolding for projects
----------------------------------------



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

        https://docs.greatexpectations.io/en/latest/getting_started.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop


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


Adding DataSources
----------------------------------------

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


Profiling data
----------------------------------------

.. code-block::

    ========== Profiling ==========

    Would you like to profile 'data__dir' to create candidate expectations and documentation?

    Please note: As of v0.7.0, profiling is still a beta feature in Great Expectations.  
    This generation of profilers will evaluate the entire data source (without sampling) and may be very time consuming. 
    As a rule of thumb, we recommend starting with data smaller than 100MB.

    To learn more about profiling, visit https://docs.greatexpectations.io/en/latest/guides/profiling.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop.
                
    Proceed? [Y/n]: Y
    Profiling 'data__dir' with 'BasicDatasetProfiler'
    Found 1 data assets using generator 'default'
    Profiling all 1.
        Profiled 38 rows from notable_works_by_charles_dickens (0.191 sec)

    Profiled 1 of 1 named data assets, with 38 total rows and 3 columns in 0.19 seconds.
    Generated, evaluated, and stored 27 candidate Expectations.
    Note: You will need to review and revise Expectations before using them in production.

    Done.

    Profiling results are saved here:
    /Users/abe/Desktop/example-dickens-data-project/great_expectations/uncommitted/validations/2019-07-08T11:09:34.803735/data__dir/default/notable_works_by_charles_dickens/BasicDatasetProfiler.json


How Great Expectations interacts with source control
----------------------------------------------------------


.. code-block::

    ========== Data Documentation ==========

    To generate documentation from the data you just profiled, the profiling results should be moved from 
    great_expectations/uncommitted (ignored by git) to great_expectations/fixtures.

    Before committing, please make sure that this data does not contain sensitive information!

    To learn more: https://docs.greatexpectations.io/en/latest/guides/data_documentation.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop

    Move the profiled data? [Y/n]: Y

    Moving files...

    Done.



Data documentation
----------------------------------------------------------

.. code-block::

    Build documentation using the profiled data? [Y/n]: Y

    Building documentation...

    To view the generated data documentation, open this file in a web browser:
        great_expectations/data_documentation/index.html


    To create expectations for your data, start Jupyter and open a tutorial notebook:

    To launch with jupyter notebooks:
        jupyter notebook great_expectations/notebooks/create_expectations.ipynb

    To launch with jupyter lab:
        jupyter lab great_expectations/notebooks/create_expectations.ipynb