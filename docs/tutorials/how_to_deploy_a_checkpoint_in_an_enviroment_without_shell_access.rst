
#################################################################
How to deploy a checkpoint in an environment without shell access
#################################################################

In this howto guide you'll learn how to deploy a **TODO link** `checkpoint`
in an environment without shell access such as `databricks <https://databricks.com>`_.


``great_expectations checkpoint script <CHECKPOINT>``

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create an executable script for deploying a checkpoint via python.

This is provided as a convenience for teams whose pipeline deployments may not have shell access or may wish to have more flexibility when running a checkpoint.

To generate a script, you must first have an existing checkpoint.
To make a new checkpoint use ``great_expectations checkpoint new``.

.. code-block:: bash

    $ great_expectations checkpoint script cost_model_protection
    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    A python script was created that runs the checkpoint named: `cost_model_protection`
      - The script is located in `great_expectations/uncommitted/run_cost_model_protection.py`
      - The script can be run with `python great_expectations/uncommitted/run_cost_model_protection.py`

The generated script looks like this:

.. code-block:: python

    """
    This is a basic generated Great Expectations script that runs a checkpoint.

    A checkpoint is a list of one or more batches paired with one or more
    Expectation Suites and a configurable Validation Operator.

    Checkpoints can be run directly without this script using the
    `great_expectations checkpoint run` command. This script is provided for those
    who wish to run checkpoints via python.

    Data that is validated is controlled by BatchKwargs, which can be adjusted in
    the checkpoint file: great_expectations/checkpoints/cost_model_protection.yml.

    Data are validated by use of the `ActionListValidationOperator` which is
    configured by default. The default configuration of this Validation Operator
    saves validation results to your results store and then updates Data Docs.

    This makes viewing validation results easy for you and your team.

    Usage:
    - Run this file: `python great_expectations/uncommitted/run_cost_model_protection.py`.
    - This can be run manually or via a scheduler such as cron.
    - If your pipeline runner supports python snippets you can paste this into your
    pipeline.
    """
    import sys

    from great_expectations import DataContext

    # checkpoint configuration
    context = DataContext("/Users/taylor/Desktop/demo/great_expectations")
    checkpoint = context.get_checkpoint("cost_model_protection")

    # load batches of data
    batches_to_validate = []
    for batch in checkpoint["batches"]:
        batch_kwargs = batch["batch_kwargs"]
        for suite_name in batch["expectation_suite_names"]:
            suite = context.get_expectation_suite(suite_name)
            batch = context.get_batch(batch_kwargs, suite)
            batches_to_validate.append(batch)

    # run the validation operator
    results = context.run_validation_operator(
        checkpoint["validation_operator_name"],
        assets_to_validate=batches_to_validate,
    )

    # take action based on results
    if not results["success"]:
        print("Validation Failed!")
        sys.exit(1)

    print("Validation Succeeded!")
    sys.exit(0)

This script can be run by invoking it with:

.. code-block:: bash

    $ python great_expectations/uncommitted/run_cost_model_protection.py
    Validation Suceeded!

Just like the built in command ``great_expectations checkpoint run``, this posix-compatible script exits with a status of ``0`` if validation is successful and a status of ``1`` if validation failed.

A failure will look like:

.. code-block:: bash

    $ python great_expectations/uncommitted/run_cost_model_protection.py
    Validation Failed!
