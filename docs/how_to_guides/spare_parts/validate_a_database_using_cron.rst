.. _how_to_guides__salck_validationaction

.. warning:: This doc is spare parts: leftover pieces of old documentation.
  It's potentially helpful, but may be incomplete, incorrect, or confusing.


How to validate tables in a database using cron
=================================================================

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
