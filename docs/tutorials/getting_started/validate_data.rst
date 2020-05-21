.. _getting_started__validate_data:

Validate some data
=========================================================

Validation is the core operation of Great Expectations: “Validate X data against Y Expectations.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Let’s set up our first Checkpoint by continuing with the CLI:

.. code-block:: bash

    Would you like to configure a Checkpoint for data validation? [Y/n]

    Heads up! This feature is Experimental. It may change. Please give us your feedback!

    Select a datasource
        1. files_datasource
        2. repeated-phrases-gop__dir
    : 1

    Enter the path (relative or absolute) of a data file
    : data/religion-survey/religion-survey-results.csv

    A checkpoint named `my_checkpoint` was added to your project:

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /Users/me/pipeline/source_files/npi.csv
          datasource: files_datasource
          reader_method: read_csv
        expectation_suite_names: # one or more suites may validate against a single batch
          - npi.warning


Let’s pause there before continuing.

What just happened?
-------------------

Checkpoints bring :ref:`Batches` of data together with corresponding :ref:`Expectation Suites`. The actual computation is carried out by a :ref:`ValidationOperator`. After executing validation, the ValidationOperator can kick off additional workflows through :ref:`ValidationActions`.

We just configured a Checkpoint so that it’s able to load ``npi.csv`` as a Batch, pair it with the ``npi.warning`` Expectation Suite, and execute them both using a pre-configured Validation Operator called ``action_list_operator``.

We’ll look more closely at the Validation Operator in a moment.

First, let’s test it out.

Test out your Checkpoint in a notebook
--------------------------------------

Checkpoints can be run like applications from the command line or cron. They can also be run from within data orchestration tools like airflow, prefect, kedro, etc.

The simplest way to test out a Checkpoint is in a disposable notebook. This is often a handy tool for testing and debugging new configuration.

.. code-block:: bash

    Would you like to test this checkpoint by running it from a disposable notebook? [Y/n]


Click yes, and the CLI will start up a notebook that looks like this.

<<< Screen shot >>>

Aside from imports, this should be a one-liner.

.. code-block:: python

    data_context.run_checkpoint(run_id)