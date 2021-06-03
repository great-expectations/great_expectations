.. _how_to_guides__validation__how_to_run_a_checkpoint_in_python:

How to run a Checkpoint in python
=================================

This guide will help you run a Checkpoint in python.
This is useful if your pipeline environment or orchestration engine does not have shell access.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - You have :ref:`created a Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`

Steps
-----

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        1. First, generate the python with the command:

        .. code-block:: bash

            great_expectations checkpoint script my_checkpoint

        2. Next, you will see a message about where the python script was created like:

        .. code-block:: bash

            A python script was created that runs the checkpoint named: `my_checkpoint`
              - The script is located in `great_expectations/uncommitted/run_my_checkpoint.py`
              - The script can be run with `python great_expectations/uncommitted/run_my_checkpoint.py`

        3. Next, open the script which should look like this:

        .. code-block:: python

            """
            This is a basic generated Great Expectations script that runs a checkpoint.

            A checkpoint is a list of one or more batches paired with one or more
            Expectation Suites and a configurable Validation Operator.

            Checkpoints can be run directly without this script using the
            `great_expectations checkpoint run` command. This script is provided for those
            who wish to run checkpoints via python.

            Data that is validated is controlled by BatchKwargs, which can be adjusted in
            the checkpoint file: great_expectations/checkpoints/my_checkpoint.yml.

            Data are validated by use of the `ActionListValidationOperator` which is
            configured by default. The default configuration of this Validation Operator
            saves validation results to your results store and then updates Data Docs.

            This makes viewing validation results easy for you and your team.

            Usage:
            - Run this file: `python great_expectations/uncommitted/run_my_checkpoint.py`.
            - This can be run manually or via a scheduler such as cron.
            - If your pipeline runner supports python snippets you can paste this into your
            pipeline.
            """
            import sys

            from great_expectations import DataContext

            # checkpoint configuration
            context = DataContext("/home/ubuntu/my_project/great_expectations")
            checkpoint = context.get_checkpoint("my_checkpoint")

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
              # TODO prepare for new RunID - checkpoint name and timestamp
              # run_id=RunID(checkpoint)
            )

            # take action based on results
            if not results["success"]:
              print("Validation failed!")
              sys.exit(1)

            print("Validation succeeded!")
            sys.exit(0)


        4. This python can then be invoked directly using python `python great_expectations/uncommitted/run_my_checkpoint.py`
        or the python code can be embedded in your pipeline.

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        1. First, generate the python with the command:

        .. code-block:: bash

            great_expectations --v3-api checkpoint script my_checkpoint

        2. Next, you will see a message about where the python script was created like:

        .. code-block:: bash

            A python script was created that runs the checkpoint named: `my_checkpoint`
              - The script is located in `great_expectations/uncommitted/run_my_checkpoint.py`
              - The script can be run with `python great_expectations/uncommitted/run_my_checkpoint.py`

        3. Next, open the script which should look like this:

        .. code-block:: python

            """
            This is a basic generated Great Expectations script that runs a Checkpoint.

            Checkpoints are the primary method for validating batches of data in production and triggering any followup actions.

            A Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a
            notification to team members about validation results, or storing a result in a shared cloud storage.

            See also <cyan>https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html</cyan> for more information about the Checkpoints and how to configure them in your Great Expectations environment.

            Checkpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script
            is provided for those who wish to run Checkpoints in python.

            Usage:
            - Run this file: `python great_expectations/uncommitted/run_chk.py`.
            - This can be run manually or via a scheduler such, as cron.
            - If your pipeline runner supports python snippets, then you can paste this into your pipeline.
            """
            import sys

            from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
            from great_expectations.data_context import DataContext

            data_context: DataContext = DataContext(
                context_root_dir="/Users/talgluck/Documents/ge_main/quagga/UAT/DataContexts/cli_testing/ge_suite/v3_many_suites_pandas_filesystem_v3_config/great_expectations"
            )

            result: CheckpointResult = data_context.run_checkpoint(
                checkpoint_name="chk",
                batch_request=None,
                run_name=None,
            )

            if not result["success"]:
                print("Validation failed!")
                sys.exit(1)

            print("Validation succeeded!")
            sys.exit(0)


        4. This python can then be invoked directly using python `python great_expectations/uncommitted/run_my_checkpoint.py`
        or the python code can be embedded in your pipeline.

            Other arguments to the `DataContext.run_checkpoint()` method may be required, depending on the amount and specifics of the Checkpoint configuration previously saved in the configuration file of the Checkpoint with the corresponding `name`.  The dynamically specified Checkpoint configuration, provided to the runtime as arguments to `DataContext.run_checkpoint()` must complement the settings in the Checkpoint configuration file so as to comprise a properly and sufficiently configured Checkpoint with the given `name`.

        Please see :ref:`How to configure a New Checkpoint using "test_yaml_config" <how_to_guides_how_to_configure_a_new_checkpoint_using_test_yaml_config>` for additional Checkpoint configuration and `DataContext.run_checkpoint()` examples.


.. discourse::
    :topic_identifier: 225
