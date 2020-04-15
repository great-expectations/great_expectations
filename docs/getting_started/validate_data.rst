.. _getting_started__validate_data:

Validating Data using Great Expectations
=========================================================

So far, your team members used Great Expectations to capture and document their expectations about your data.

It is time for your team to benefit from Great Expectations' automated testing that systematically surfaces errors, discrepancies and surprises lurking in your data, allowing you and your team to be more proactive when data changes.

We typically see two main deployment patterns that we will explore in depth below.

1. Great Expectations is **deployed adjacent to your existing data pipeline**.
2. Great Expectations is **embedded into your existing data pipeline**.

Deploying automated testing adjacent to a data pipeline
*******************************************************

You might find yourself in a situation where you do not have the engineering resources, skills, desire, or permissions to embed Great Expectations into your pipeline.
As long as your data is accessible you can still reap the benefits of automated data testing.

.. note:: This is a fast and convenient way to get the benefits of automated data testing without requiring engineering efforts to build Great Expectations into your pipelines.

A tap is an executable python file that runs validates a batch of data against an expectation suite.
Taps are a convenient way to generate a data validation script that can be run manually or via a scheduler.

Let's make a new tap using the ``tap new`` command.

To do this we\'ll specify the name of the suite and the name of the new python file we want to create.
For this example, let\'s say we want to validate a batch of data against the ``movieratings.ratings`` expectation suite, and we want to make a new file called ``movieratings.ratings_tap.py``

.. code-block:: bash

    $ great_expectations tap new movieratings.ratings movieratings.ratings_tap.py
    This is a BETA feature which may change.

    Which table would you like to use? (Choose one)
        1. ratings (table)
        Don\'t see the table in the list above? Just type the SQL query
    : 1
    A new tap has been generated!
    To run this tap, run: python movieratings.ratings_tap.py
    You can edit this script or place this code snippet in your pipeline.

If you open the generated tap file you'll see it's only a few lines of code to get validations running!
It will look like this:

.. code-block:: python

    """
    A basic generated Great Expectations tap that validates a single batch of data.

    Data that is validated is controlled by BatchKwargs, which can be adjusted in
    this script.

    Data are validated by use of the `ActionListValidationOperator` which is
    configured by default. The default configuration of this Validation Operator
    saves validation results to your results store and then updates Data Docs.

    This makes viewing validation results easy for you and your team.

    Usage:
    - Run this file: `python movieratings.ratings_tap.py`.
    - This can be run manually or via a scheduler such as cron.
    - If your pipeline runner supports python snippets you can paste this into your
    pipeline.
    """
    import sys
    import great_expectations as ge

    # tap configuration
    context = ge.DataContext()
    suite = context.get_expectation_suite("movieratings.ratings_tap")
    # You can modify your BatchKwargs to select different data
    batch_kwargs = {
        "table": "ratings",
        "schema": "movieratings",
        "datasource": "movieratings",
    }

    # tap validation process
    batch = context.get_batch(batch_kwargs, suite)
    results = context.run_validation_operator("action_list_operator", [batch])

    if not results["success"]:
        print("Validation Failed!")
        sys.exit(1)

    print("Validation Succeeded!")
    sys.exit(0)

To run this and validate a batch of data, run:

.. code-block:: bash

    $ python movieratings.ratings_tap.py
    Validation Succeeded!

This can easily be run manually anytime you want to check your data.
It can also easily be run on a schedule basis with a scheduler such as cron.

You'll want to view the detailed data quality reports in Data Docs by running ``great_expectations docs build``.

For example, if you wanted to run this script nightly at 04:00, you'd add something like this to your crontab.

.. code-block:: bash

    $ crontab -e
    0 4 * * * /full/path/to/python /full/path/to/movieratings.ratings_tap.py

If you don't have access to a scheduler, you can always make checking your data part of your daily routine.
Once you experience how much time and pain this saves you, we recommend geting engineering resources to embed Great Expectations validations into your pipeline.

Embedding automated testing into a data pipeline
************************************************

.. note:: This is an ideal way to deploy automated data testing if you want to take automated interventions based on the results of data validation.
  For example, you may want your pipeline to quarantine data that does not meet your expectations.


A data engineer can add a :ref:`Validation Operator<validation_operators_and_actions>` to your pipeline and configure it.
These :ref:`Validation Operators<validation_operators_and_actions>` evaluate the new batches of data that flow through your pipeline against the expectations your team defined in the previous sections.

While data pipelines can be implemented with various technologies, at their core they are all DAGs (directed acyclic graphs) of computations and transformations over data.

This drawing shows an example of a node in a pipeline that loads data from a CSV file into a database table.

- Two expectation suites are deployed to monitor data quality in this pipeline.
- The first suite validates the pipeline's input - the CSV file - before the pipeline executes.
- The second suite validates the pipeline's output - the data loaded into the table.

.. image:: ../images/pipeline_diagram_two_nodes.png

To implement this validation logic, a data engineer inserts a Python code snippet into the pipeline - before and after the node. The code snippet prepares the data for the GE Validation Operator and calls the operator to perform the validation.

The exact mechanism of deploying this code snippet depends on the technology used for the pipeline.

If Airflow drives the pipeline, the engineer adds a new node in the Airflow DAG. This node will run a PythonOperator that executes this snippet. If the data is invalid, the Airflow PythonOperator will raise an error which will stop the rest of the execution.

If the pipeline uses something other than Airflow for orchestration, as long as it is possible to add a Python code snippet before and/or after a node, this will work.

Below is an example of this code snippet, with comments that explain what each line does.

.. code-block:: python

    # Data Context is a GE object that represents your project.
    # Your project's great_expectations.yml contains all the config
    # options for the project's GE Data Context.
    context = ge.data_context.DataContext()

    datasource_name = "my_production_postgres" # a datasource configured in your great_expectations.yml

    # Tell GE how to fetch the batch of data that should be validated...

    # ... from the result set of a SQL query:
    batch_kwargs = {"query": "your SQL query", "datasource": datasource_name}

    # ... or from a database table:
    # batch_kwargs = {"table": "name of your db table", "datasource": datasource_name}

    # ... or from a file:
    # batch_kwargs = {"path": "path to your data file", "datasource": datasource_name}

    # ... or from a Pandas or PySpark DataFrame
    # batch_kwargs = {"dataset": "your Pandas or PySpark DataFrame", "datasource": datasource_name}

    # Get the batch of data you want to validate.
    # Specify the name of the expectation suite that holds the expectations.
    expectation_suite_name = "movieratings.ratings" # this is an example of
                                                        # a suite that you created
    batch = context.get_batch(batch_kwargs, expectation_suite_name)

    # Call a validation operator to validate the batch.
    # The operator will evaluate the data against the expectations
    # and perform a list of actions, such as saving the validation
    # result, updating Data Docs, and firing a notification (e.g., Slack).
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=run_id) # e.g., Airflow run id or some run identifier that your pipeline uses.

    if not results["success"]:
        # Decide what your pipeline should do in case the data does not
        # meet your expectations.


Responding to Validation Results
----------------------------------------

A :ref:`Validation Operator<validation_operators_and_actions>` is deployed at a particular point in your data pipeline.

A new batch of data arrives and the operator validates it against an expectation suite (see the previous step).

The :ref:`actions<actions>` of the operator store the validation result, add an HTML view of the result to the Data Docs website, and fire a configurable notification (by default, Slack).

If the data meets all the expectations in the suite, no action is required. This is the beauty of automated testing. No team members have to be interrupted.

In case the data violates some expectations, team members must get involved.

In the world of software testing, if a program does not pass a test, it usually means that the program is wrong and must be fixed.

In pipeline and data testing, if data does not meet expectations, the response to a failing test is triaged into 3 categories:

1. **The data is fine, and the validation result revealed a characteristic that the team was not aware of.**
  The team's data scientists or domain experts update the expectations to reflect this new discovery.
  They use the process described above in the Review and Edit sections to update the expectations while testing them against the data batch that failed validation.
2. **The data is "broken"**, and **can be recovered.**
  For example, the users table could have dates in an incorrect format.
  Data engineers update the pipeline code to deal with this brokenness and fix it on the fly.
3. **The data is "broken beyond repair".**
  The owners of the pipeline go upstream to the team (or external partner) who produced the data and address it with them.
  For example, columns in the users table could be missing entirely.
  The validation results in Data Docs makes it easy to communicate exactly what is broken, since it shows the expectation that was not met and observed examples of non-conforming data.
