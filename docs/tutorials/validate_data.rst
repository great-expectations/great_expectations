.. _tutorial_validate_data:



Validate Data
==============

Expectations describe Data Assets. Data Assets are composed of Batches. Validation checks Expectations against a Batch of data. Expectation Suites combine multiple Expectations into an overall description of a Batch.

Validation = checking if a Batch of data from a Data Asset X conforms to all Expectations in Expectation Suite Y. Expectation Suite Y is a collection of Expectations that you created that specify what a valid Batch of Data Asset X should look like.

To run Validation you need a **Batch** of data. To get a **Batch** of data you need:

* to provide `batch_kwargs` to a :ref:`Data Context<data_context>`
* to specify an **Expectation Suite** to validate against

This tutorial will explain each of these objects, show how to obtain them, execute validation and view its result.

0. Open Jupyter Notebook
------------------------

This tutorial assumes that:

* you ran ``great_expectations init``
* your current directory is the root of the project where you ran ``great_expectations init``

You can either follow the tutorial with the sample National Provider Identifier (NPI) dataset (processed with Pandas) referenced in the :ref:`great_expectations init<tutorial_init>` tutorial, or you can execute the same steps on your project with your own data.

If you get stuck, find a bug or want to ask a question, go to `our Slack <https://greatexpectations.io/slack>`_ - this is the best way to get help from the contributors and other users.

Validation is typically invoked inside the code of a data pipeline (e.g., an Airflow operator). This tutorial uses a Jupyter notebook as a validation playground.

The ``great_expectations init`` command created a ``great_expectations/notebooks/`` folder in your project. The folder contains example notebooks for pandas, Spark and SQL datasources.

If you are following this tutorial using the NPI dataset, open the pandas notebook. If you are working with your dataset, see the instructions for your datasource:

.. content-tabs::

    .. tab-container:: tab0
        :title: pandas

        .. code-block:: bash

            jupyter notebook great_expectations/notebooks/pandas/validation_playground.ipynb

    .. tab-container:: tab1
        :title: pyspark

        .. code-block:: bash

            jupyter notebook great_expectations/notebooks/spark/validation_playground.ipynb

    .. tab-container:: tab2
        :title: SQLAlchemy

        .. code-block:: bash

            jupyter notebook great_expectations/notebooks/sql/validation_playground.ipynb




1. Get a DataContext Object
---------------------------

A DataContext represents a Great Expectations project. It organizes Datasources, notification settings, data documentation sites, and storage and access for Expectation Suites and Validation Results.
The DataContext is configured via a yml file stored in a directory called great_expectations;
the configuration file as well as managed Expectation Suites should be stored in version control.

Instantiating a DataContext loads your project configuration and all its resources.

::

    context = ge.data_context.DataContext()

To read more about DataContexts, see: :ref:`data_context`


2. Choose an Expectation Suite
-------------------------------------------

The ``context`` instantiated in the previous section has a convenience method that lists all Expectation Suites created in a project:

.. code-block:: python

  for expectation_suite_id in context.list_expectation_suites():
      print(expectation_suite_id.expectation_suite_name)

Choose the Expectation Suite you will use to validate a Batch of data:

.. code-block:: python

    expectation_suite_name = "warning"


3. Load a batch of data you want to validate
---------------------------------------------

Expectations describe Batches of data - Expectation Suites combine multiple Expectations into an overall description of a Batch. Validation checks a Batch against an Expectation Suite.

For example, a Batch could be the most recent day of log data. For a database table, a Batch could be the data in that table at a particular time.

In order to validate a Batch of data, you will load it as a Great Expectations :class:`Dataset <great_expectations.dataset.dataset.Dataset>`.

Batches are obtained by using a Data Context's ``get_batch`` method, which accepts ``batch_kwargs`` and ``expectation_suite_name`` as arguments.

Calling this method asks the Context to get a Batch of data using the provided ``batch_kwargs`` and attach the Expectation Suite ``expectation_suite_name`` to it.

The ``batch_kwargs`` argument is a dictionary that specifies a batch of data - it contains all the information necessary for a Data Context to obtain a batch of data from a :ref:`Datasource<datasource>`. The keys of a ``batch_kwargs``
dictionary will vary depending on the type of Datasource and how it generates Batches, but will always have a ``datasource`` key with the name of a Datasource. To list the Datasources configured in a project, you may use a Data Context's ``list_datasources`` method.

.. content-tabs::

    .. tab-container:: tab0
        :title: pandas

        A Pandas Datasource generates Batches from Pandas DataFrames or CSV files. A Pandas Datasource can accept ``batch_kwargs`` that describe either a path to a file or an existing DataFrame:

        .. code-block:: python

            # list datasources of the type PandasDatasource in your project
            [datasource['name'] for datasource in context.list_datasources() if datasource['class_name'] == 'PandasDatasource']
            datasource_name = # TODO: set to a datasource name from above

            # If you would like to validate a file on a filesystem:
            batch_kwargs = {'path': "YOUR_FILE_PATH", 'datasource': datasource_name}

            # If you already loaded the data into a Pandas Data Frame:
            batch_kwargs = {'dataset': "YOUR_DATAFRAME", 'datasource': datasource_name}

            batch = context.get_batch(batch_kwargs, expectation_suite_name)
            batch.head()

    .. tab-container:: tab1
        :title: pyspark

        A Spark Datasource generates Batches from Spark DataFrames or CSV files. A Spark Datasource can accept ``batch_kwargs`` that describe either a path to a file or an existing DataFrame:

        .. code-block:: python

            # list datasources of the type SparkDFDatasource in your project
            [datasource['name'] for datasource in context.list_datasources() if datasource['class_name'] == 'SparkDFDatasource']
            datasource_name = # TODO: set to a datasource name from above

            # If you would like to validate a file on a filesystem:
            batch_kwargs = {'path': "YOUR_FILE_PATH", 'datasource': datasource_name}
            # To customize how Spark reads the file, you can add options under reader_options key in batch_kwargs (e.g., header='true')

            # If you already loaded the data into a PySpark Data Frame:
            batch_kwargs = {'dataset': "YOUR_DATAFRAME", 'datasource': datasource_name}


            batch = context.get_batch(batch_kwargs, expectation_suite_name)
            batch.head()

    .. tab-container:: tab2
        :title: SQLAlchemy

        A SQLAlchemy Datasource generates Batches from tables, views and query results. A SQLAlchemy Datasource can accept ``batch_kwargs`` that instruct it load a batch from a table, a view, or a result set of a query:

        .. code-block:: python

            # list datasources of the type SqlAlchemyDatasource in your project
            [datasource['name'] for datasource in context.list_datasources() if datasource['class_name'] == 'SqlAlchemyDatasource']
            datasource_name = # TODO: set to a datasource name from above

            # If you would like to validate an entire table or view in your database's default schema:
            batch_kwargs = {'table': "YOUR_TABLE", 'datasource': datasource_name}

            # If you would like to validate an entire table or view from a non-default schema in your database:
            batch_kwargs = {'table': "YOUR_TABLE", "schema": "YOUR_SCHEMA", 'datasource': datasource_name}

            # If you would like to validate the result set of a query:
            # batch_kwargs = {'query': 'SELECT YOUR_ROWS FROM YOUR_TABLE', 'datasource': datasource_name}

            batch = context.get_batch(batch_kwargs, expectation_suite_name)
            batch.head()

    The examples of ``batch_kwargs`` above can also be the outputs of "Generators" used by Great Expectations. You
can read about the default Generators' behavior and how to implement additional Generators in this article:
:ref:`batch_kwargs_generator`.

4. Validate the batch
-----------------------

When Great Expectations is integrated into a data pipeline, the pipeline calls GE to validate a specific batch (an input to a pipeline's step or its output).

Validation evaluates the Expectations of an Expectation Suite against the given Batch and produces a report that describes observed values and
any places where Expectations are not met. To validate the Batch of data call the :meth:`~great_expectations.\
data_asset.data_asset.DataAsset.validate` method on the batch:

.. code-block:: python

  validation_result = batch.validate()

The ``validation_result`` object has detailed information about every Expectation in the Expectation Suite that was used to validate the Batch: whether the Batch met the Expectation and even more details if it did not. You can read more about the result object's structure here: :ref:`validation_result`.

You can print this object out:

.. code-block:: python

    print(json.dumps(validation_result, indent=4))


Here is what a part of this object looks like:

.. image:: ../images/validation_playground_result_json.png
    :width: 500px

Don't panic! This blob of JSON is meant for machines. :ref:`data_docs` are an compiled HTML view of both expectation suites and validation results that is far more suitable for humans. You will see how easy it is to build them in the next sections.

5. Validation Operators
-----------------------

The ``validate()`` method evaluates one Batch of data against one Expectation Suite and returns a dictionary of Validation Results. This is sufficient when you explore your data and get to know Great Expectations.

When deploying Great Expectations in a real data pipeline, you will typically discover these additional needs:

* Validating a group of Batches that are logically related (e.g. Did all my Salesforce integrations work last night?).
* Validating a Batch against several Expectation Suites (e.g. Did my nightly clickstream event job have any **critical** failures I need to deal with ASAP or **warnings** I should investigate later?).
* Doing something with the Validation Results (e.g., saving them for a later review, sending notifications in case of failures, etc.).

Validation Operators provide a convenient abstraction for both bundling the validation of multiple Expectation Suites and the actions that should be taken after the validation. See the
:ref:`validation_operators_and_actions` for more information.

An instance of ``action_list_operator`` operator is configured in the default ``great_expectations.yml`` configuration file. ``ActionListValidationOperator`` validates each Batch in the list that is passed as ``assets_to_validate`` argument to its ``run`` method against the Expectation Suite included within that Batch and then invokes a list of configured actions on every Validation Result.

Below is the operator's configuration snippet in the ``great_expectations.yml`` file:

.. code-block:: bash

  action_list_operator:
    class_name: ActionListValidationOperator
    action_list:
      - name: store_validation_result
        action:
          class_name: StoreValidationResultAction
      - name: store_evaluation_params
        action:
          class_name: StoreEvaluationParametersAction
      - name: update_data_docs
        action:
          class_name: UpdateDataDocsAction
      - name: send_slack_notification_on_validation_result
        action:
          class_name: SlackNotificationAction
          # put the actual webhook URL in the uncommitted/config_variables.yml file
          slack_webhook: ${validation_notification_slack_webhook}
          notify_on: all # possible values: "all", "failure", "success"
          renderer:
            module_name: great_expectations.render.renderer.slack_renderer
            class_name: SlackRenderer

We will show how to use the two most commonly used actions that are available to this operator:

Save Validation Results
~~~~~~~~~~~~~~~~~~~~~~~

The DataContext object provides a configurable ``validations_store`` where GE can store validation_result objects for
subsequent evaluation and review. By default, the DataContext stores results in the
``great_expectations/uncommitted/validations`` directory. To specify a different directory or use a remote store such
as ``s3`` or ``gcs``, edit the stores section of the DataContext configuration object:

.. code-block:: bash

    stores:
      validations_store:
        class_name: ValidationsStore
        store_backend:
          class_name: TupleS3Backend
          bucket: my_bucket
          prefix: my_prefix

Removing the store_validation_result action from the ``action_list_operator`` configuration will disable automatically storing ``validation_result`` objects.

Send a Slack Notification
~~~~~~~~~~~~~~~~~~~~~~~~~

The last action in the action list of the Validation Operator above sends notifications using a user-provided callback
function based on the validation result.

.. code-block:: bash

  - name: send_slack_notification_on_validation_result
    action:
      class_name: SlackNotificationAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      slack_webhook: ${validation_notification_slack_webhook}
      notify_on: all # possible values: "all", "failure", "success"
      renderer:
        module_name: great_expectations.render.renderer.slack_renderer
        class_name: SlackRenderer

GE includes a slack-based notification in the base package. To enable a slack notification for results, simply specify
the slack webhook URL in the uncommitted/config_variables.yml file:

.. code-block:: bash

  validation_notification_slack_webhook: https://slack.com/your_webhook_url

Running the Validation Operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before running the Validation Operator, create a ``run_id``. A ``run_id`` links together validations of different data assets, making it possible to track "runs" of a pipeline and
follow data assets as they are transformed, joined, annotated, enriched, or evaluated. The run id can be any string;
by default, Great Expectations will use an ISO 8601-formatted UTC datetime string.

The default ``run_id`` generated by Great Expectations is built using the following code:

.. code-block:: python

    run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S.%fZ")

When you integrate validation in your pipeline, your pipeline runner probably has a run id that can be inserted here to make smoother integration.

Finally, run the Validation Operator:

.. code-block:: python

  results = context.run_validation_operator(
      "action_list_operator",
      assets_to_validate=[batch],
      run_id=run_id)


6. View the Validation Results in Data Docs
-------------------------------------------

Data Docs compiles raw Great Expectations objects including Expectations and Validations into structured documents such as HTML documentation. By default the HTML website is hosted on your local filesystem. When you are working in a team, the website can be hosted in the cloud (e.g., on S3) and serve as the shared source of truth for the team working on the data pipeline.

Read more about the capabilities and configuration of Data Docs here: :ref:`data_docs`.

One of the actions executed by the validation operator in the previous section rendered the validation result as HTML and added this page to the Data Docs site.

You can open the page programmatically and examine the result:

.. code-block:: python

    context.open_data_docs()


Congratulations!
----------------

Now you you know how to validate a Batch of data.

What is next? This is a collection of tutorials that walk you through a variety of useful Great Expectations workflows: :ref:`tutorials`.


