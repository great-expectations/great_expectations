.. _typical_workflow:

Typical Workflow
===============================================

This article describes a typical Great Expectations workflow when used by a data team.

Its objective is to gain control and confidence in your data delivery workflows and to address the challenges of validating and monitoring the quality and accuracy of your data.

Data team members capture and document their shared understanding of their data as expectations. As new data arrives in the pipeline, it is evaluated against these expectations. If the observed properties of the data are found to be different from the expected ones, the team responds by rejecting (or fixing) the data, updating the expectations, or both.

The article focuses on the "What" and the "Why" of each step in this workflow, and touches on the "How" only briefly. The exact details of configuring and executing these steps are intentionally left out - they can be found in the tutorials and reference linked from each section.


Setting up a project
----------------------------------------

To use Great Expectations in a new data project, a Data Context must be initialized. You will see references to Data Context throughout the documentation. Data Context simply represents a Great Expectations project.

The command line interface (CLI) command ``init`` does the initialization. Run this command in the terminal in the root of your project's repo:

.. code-block:: bash

    great_expectations init

This command has to be run only once per project.

The command creates ``great_expectations`` subdirectory in the current directory. The team member who runs it, commits the generated directory into the version control. The contents of ``great_expectations`` look like this:

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
        │   └── local_site
        └── validations

* The ``great_expectations/great_expectations.yml`` configuration file defines how to access the project's data, expectations, validation results, etc.
* The ``expectations`` directory is where the expectations are stored as JSON files.
* The ``uncommitted`` directory is the home for files that should not make it into the version control - it is configured to be excluded in the ``.gitignore`` file. Each team member will have their own content of this directory. In Great Expectations, files should not go into the version control for two main reasons:

  * They contain sensitive information. For example, to allow the ``great_expectations/great_expectations.yml`` configuration file, it must not contain any database credentials and other secrets. These secrets are stored in the ``uncommitted/config_variables.yml`` that is not checked in.

  * They are not a "primary source of truth" and can be regenerated. For example, ``uncommitted/documentation`` contains generated data documentation (this article will cover data documentation in a later section).



Adding Datasources
----------------------------------------

Evaluating an expectation against a batch of data is the fundamental operation in Great Expectations.

For example, imagine tht we have a movie ratings table in the database. This expectation says that we expect that column "rating" takes only 1, 2, 3, 4 and 5:

.. code-block:: json

    {
      "kwargs": {
        "column": "rating",
        "value_set": [1, 2, 3, 4, 5]
      },
      "expectation_type": "expect_column_distinct_values_to_be_in_set"
    }

When Great Expectations evaluates this expectation against a dataset that has a column named "rating", it returns a validation result saying whether the data meets the expectation.

This operation can be executed in one of the following compute environments (engines): Pandas, PySpark and an SQL database. This means that the evaluated dataset (or batch of data) can be a Pandas DataFrame, a PySpark DataFrame or a query result set.

A Datasource is an object Great Expectations uses to connect to a compute environment (e.g., a Postgres database on a particular host).

Each You can have multiple Datasources in a project (Data Context). This is useful if the team's pipeline consists of, for example, both a Spark cluster and a Redshift database.

All the Datasources that your project uses are configured in the ``great_expectations/great_expectations.yml`` configuration file of the Data Context:


.. code-block::

    datasources:

      our_product_postgres_database:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
        credentials: ${prod_db_credentials}

      our_redshift_warehouse:
        class_name: SqlAlchemyDatasource
        data_asset_type:
          class_name: SqlAlchemyDataset
        credentials: ${warehouse_credentials}



You can add Datasources by editing the configuration file, but the preferred way is to use the CLI convenience command:

.. code-block:: bash

    great_expectations datasource new


The command prompts for the required connection attributes and tests the connection to the new Datasource.

A Datasource object knows how to load data into the computation environment. For example, you can call a PySpark Datasource object to load data into a DataFrame from a directory on S3. This is beyond the scope of this section, but will be useful a but later.


After a team member adds a new Datasource to the Data Context, they commit the updated configuration file into the version control in order to make the change available to the rest of the team.

Since ``great_expectations/great_expectations.yml`` is committed into the version control, the CLI command makes sure not to store the credentials (database user and password in the file). Instead it saves them in a separate filedatasources can take their credentials - ``uncommitted/config_variables.yml`` - that is not committed into the version control.

This means that that when another team member checks out the updated configuration file with the newly added Datasource, they must set the credentials in their ``uncommitted/config_variables.yml`` or in environment variables.

Setting up Data Docs
----------------------------------------------------------

Data Docs is a feature of Great Expectations that creates data documentation by compiling expectations and validation results into HTML.

Data Docs produces a visual description of what you expect from your data, and how the observed properties of your data differ from your expectations. It helps to keep your entire team on the same page as data evolves.

Here is what the ``expect_column_distinct_values_to_be_in_set`` expectation about the `rating` column of the movie ratings table from the earlier example looks like in Data Docs.
.. image:: ../images/exp_ratings_col_dist_val_set.png

This approach to data documentation has two significant advantages.

First, for engineers, Data Docs makes it possible to automatically keep your data documentation in sync with your tests. This prevents documentation rot and can save a huge amount of time on otherwise unrewarding document maintenance.

Second, the ability to translate expectations back and forth between human- and machine-readable formats opens up
many opportunities for domain experts and stakeholders who aren't engineers to collaborate more closely with
engineers on data applications.

To set up Data Docs for a project, a “data documentation site” (a static HTML website) must be defined in the Data Context's configuration file.

Multiple sites can be configured inside a project, each suitable for a particular data documentation use case. For example, some data teams use one site that has expectations and validation results from all the runs of their data pipeline for monitoring the pipeline's health, and another site that has only the expectations for communicating with their client (similar to API documentation in software development).

By default Data Docs sites' files are published to the local filesystem in `great_expectations/uncommitted/data_docs/` directory. To make the site available to the team, a team member can be configure it to publish to a shared location, such as a S3 or GCS.

All the Data Docs sites that your project has are defined in the ``great_expectations/great_expectations.yml`` configuration file. The site's configuration defines what they should display and where they are hosted. Data Docs is very customizable, but the details are beyond this article's scope.


Authoring expectation suites
----------------------------------------------------------

Earlier in this article we said that capturing and documenting the team's shared understanding of its data as expectations is the core part of this typical workflow.

Expectation Suites combine multiple expectations into an overall description of a dataset. For example, a team can group all the expectations about its ``rating`` table in the movie ratings database from our previous example into an Expectation Suite and call it "movie_ratings_database.rating.expectations".

Each Expectation Suite is saved as a JSON file in the ``great_expectations/expectations`` subdirectory of the Data Context. Users check these files into the version control each time they are updated, same way they treat their source files.

The lifecycle of an Expectation Suite starts with creating it. Then it goes through a loop of Review and Edit as the team's understanding of the data described by the suite evolves.

We will describe the Create, Review and Edit steps in brief:

Create
********************************************


Expectation Suites are saved as JSON files, so you can create a new suite by writing a file directly. However, just like with other features the preferred way is to let CLI save you time and typos. Run this command in the root directory of your project (where the init command created the ``great_expectations`` subdirectory:


.. code-block:: bash

    great_expectations suite new


This command prompts you to name your new Expectation Suite and to select a sample batch of the dataset the suite will describe. Then it profiles the selected sample and adds some initial expectations to the suite. The purpose of these is expectations is to provide examples of what properties of data can be described using Great Expectations. They are only a starting point that the user builds on.

The command concludes by saving the newly generated Expectation Suite as a JSON file and rendering the expectation suite into an HTML page in the Data Docs website of the Data Context.


Review
********************************************

Reviewing expectations is best done in Data Docs:

.. image:: ../images/sample_e_s_view.png

Edit
********************************************

The best interface for editing an Expectation Suite is a Jupyter notebook.

Editing an Expectation Suite means adding expectations, removing expectations, and modifying the arguments of existing expectations.

For every expectation type there is a Python method that sets its arguments, evaluates this expectation against a sample batch of data and adds it to the Expectation Suite.

Take a look at the screenshot below. It shows the HTML view and the Python method for the same expectation (``expect_column_distinct_values_to_be_in_set``) side by side:

.. image:: ../images/exp_html_python_side_by_side .png

The CLI provides a command that, given an Expectation Suite, generates a Jupyter notebook to edit it. It takes care of generating a cell for every expectation in the suite and of getting a sample batch of data. The HTML page for each Expectation Suite has the CLI command syntax in order to make it easier for users.

.. image:: ../images/edit_e_s_popup.png

The generated Jupyter notebook can be discarded, since it is auto-generated.



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