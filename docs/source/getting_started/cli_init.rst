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

Great Expectations provides a mildly opinionated deployment framework that simplifies core operations, such as connecting to data sources; fetching, profiling and validating batches of data; and compiling to human-readable documentation.

By default, everything in this framework will be expressed in a directory structure within a `great_expectations/` folder within your version control system. To create this folder, navigate the the root of your project directory in a terminal and run `great_expectations init`. This command line interface (CLI) will scaffold and populate the configuration and other artifacts necessary to get started with Great Expectations.


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

Next, the CLI will ask you if you want to configure a data source.

DataSources allow you to configure connections to data to evaluate Expectations. Great Expectations currently supports native evaluation of Expectations in three compute environments: pandas dataframes, relational databases via SQL Alchemy, and Spark dataframes. Therefore, a DataSource could be a directory containing CSVs with some configuration to parse those files in pandas; a connection to postgresql instance; an S3 bucket that is read and processed by Spark; etc. (In the future, we expect to extend to other compute environments, such as dask and BigQuery.)

Suppose your project contains a `data/` folder. In that case, you can configure a Pandas DataFrame directory like so.

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

For a SQL data source, configuration would look like this instead.

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

Strictly speaking, a Great Expectations DataSource is not the data itself, but part of a _pointer_ to a data compute environment where Expectations can be evaluated, called a `DataAsset.` Fully describing the pointer requires a 5-ple:

1. datasource_name (e.g. `my_postgresql_db`)
2. generator_name (`split_by_date`)
3. data_asset_name (`user_events_table`)
4. batch_id (`2018-01-01`)
5. expectation_suite_id (`BasicDatasetProfiler`).

Together, these elements define the DataAsset namespace <<<link>>>.

You can get started in Great Expectations without learning all the details of the namespace. To start, you'll mainly use elements 1 and 3: datasource_names, like "my_postgresql_db" and data_asset_names, like "user_events_table". For most users, these names are already familiar and intuitive. From there, Great Expectations' namespace defaults <<<link>>> can usually fill in the gaps.


Profiling data
----------------------------------------

Now that we've configured a DataSource, the next step is to _profile_ it. Profiling will generate a first set of candidate Expectations for your data. By default, they will cover a wide range of statistics and other characteristics of the Dataset that could be useful for future validation.

Profiling also evaluates these candidate Expectations against your actual data, producing a set of ExpectationValidationResults (EVRs), which contain observed values and other context derived from the data itself.

Together, profiled Expectations and EVRs provide a lot of useful information for dialing in the Expectations you will use in production. They will also provide the raw materials for first-pass data documentation.


<<< UPDATE CODE BLOCK >>>
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

    ========== Data Documentation ==========

    To generate documentation from the data you just profiled, the profiling results should be moved from 
    great_expectations/uncommitted (ignored by git) to great_expectations/fixtures.

    Before committing, please make sure that this data does not contain sensitive information!

    To learn more: https://docs.greatexpectations.io/en/latest/guides/data_documentation.html?utm_source=cli&utm_medium=init&utm_campaign=0_7_0__develop

    Move the profiled data? [Y/n]: Y

    Moving files...

    Done.

Note: before committing profiled Expectations to source control, we STRONGLY recommend reviewing them, for two reasons. 

First, Expectations can contain sensitive data that should not be committed to source code. For example, if you apply `expect_column_values_to_be_in_set` to a column containing social security numbers, the EVRs will contain actual SSNs. Most likely, you would want to replace real SSNs with placeholders before committing to source control.

Second, since profiled Expectations are only based on raw data, they almost always require an additional layer of domain knowledge and judgement. This is doubly true when the data itself isn't fully explored or trusted.

<<SHOW WHERE EXPECTATIONS AND EVRS ARE STORED>>>

We've created the great expectations directory that like we talked about, and now inside the, uh, expectations sub directory we have as a part of a namespace. So the namespace consisting of the data source name that we configured, our generator, the data asset specific data asset name, and then the name. In this case of the profiler that we used, we've got a first baseline suite, a suite of expectations. Now in our uncommitted directory, we have a single validation run. So that run corresponding to the batch that we just did as part of profiling where we have the exact same hierarchical namespace. And the validation result. So if we take a look really briefly at what's in the, uh, expectations, we'll see that we have, um, uh, the expectations that were created by profiling are generally quite loose. Um, so, you know, we'll see, uh, for example, the unique value count between Nolan and no meaning that we don't have an expectation of what it is, but we would like grade expectations to compute that statistic and make it available as a part of the profiling. 

<<DESCRIBE EXPECTATION JSON objects>>>

What that means is that if we switch over and look at the result of the actual validation run, uh, we'll see the, uh, evaluation of each of those expectations that have now produced, uh, the specific statistics relevant for evaluating that expectations. So, uh, in this case, that unique value count, uh, was observed to be 38 in that particular column. As addition, in addition, as a part of the uh, validation result, what we see are some, uh, information about the overall data asset that we just looked at. So again, we've got that hierarchical name that we've described that the data context we'll use to identify the data source generator. And to data asset as well as the name of the specific expectations suite that was used to validate, uh, the, the data asset and create this validation report as well as the specific information about this particular batch.


Data documentation
----------------------------------------------------------

So the file that, uh, formed the basis of it as well as when we grab that file. So in other words, comfortable that there's no sensitive information in that validation result, we can go ahead and agree to moving it into our fixtures folder and building documentation on the basis of that.

Now that that documentation is built, uh, we can just take a look at it in, uh, our web browser. So, uh, in this new, uh, newly rendered document, we have just a single data asset and we can see an overview of what's there. So for each of the columns, the expectations have formed the basis for, uh, describing the number of distinct values, quintiles, variety of different statistics, example, values, histogram, and other things that are relevant. Now, the key thing is that each of those pieces of information is now backed by an expectation.


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