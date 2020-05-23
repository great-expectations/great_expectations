.. _getting_started__validate_data:

Set up your first Checkpoint
============================

As we said earlier, validation the core operation of Great Expectations: “Validate X data against Y Expectations.”

In normal usage, the best way to validate data is with a :ref:`Checkpoint`. Checkpoints bring :ref:`Batches` of data together with corresponding :ref:`Expectation Suites` for validation. Configuring Checkpoints simplifies deployment, by pre-specifying the "X"s and "Y"s that you want to validate at any given point in your data infrastructure.

Let’s set up our first Checkpoint by running another CLI command:

.. code-block:: bash

  great_expectations checkpoint new my_checkpoint npidata_pfile_20200511-20200517.warning.json

``my_checkpoint`` will be the name of your new Checkpoint. It will use ``npidata_pfile_20200511-20200517.warning`` as its primary :ref:`Expectation Suite`. (You can add other Expectation Suites later.)

From there, you can configure the Checkpoint using the CLI:

.. code-block:: bash

    Heads up! This feature is Experimental. It may change. Please give us your feedback!
    
    Would you like to:
        1. choose from a list of data assets in this datasource
        2. enter the path of a data file
    : 1
    
    Which data would you like to use?
        1. npidata_pfile_20200511-20200517 (file)
        Don't see the name of the data asset in the list above? Just type it
    : 1
    A checkpoint named `my_checkpoint` was added to your project!
      - To edit this checkpoint edit the checkpoint file: /home/ubuntu/example_project/great_expectations/checkpoints/my_checkpoint.yml
      - To run this checkpoint run `great_expectations checkpoint run my_checkpoint`
    
Let’s pause there before continuing.

How Checkpoints work
--------------------

Your new checkpoint file is in ``my_checkpoint.yml``. With comments removed, it looks like this:

.. code-block:: yaml

    validation_operator_name: action_list_operator
    batches:
      - batch_kwargs:
          path: /home/ubuntu/example_project/great_expectations/../my_data/npidata_pfile_20200511-20200517.csv
          datasource: my_data__dir
          data_asset_name: npidata_pfile_20200511-20200517
        expectation_suite_names: # one or more suites may validate against a single batch
          - npidata_pfile_20200511-20200517.warning


Our newly configured Checkpoint knows how to load ``npidata_pfile_20200511-20200517.csv`` as a Batch, pair it with the ``npidata_pfile_20200511-20200517.warning`` Expectation Suite, and execute them both using a pre-configured :ref:`Validation Operator <Validation Operators>` called ``action_list_operator``.

You don't need to worry much about the details of Validation Operators for now. They orchestrate the actual work of validating data and processing the results. After executing validation, the Validation Operator can kick off additional workflows through :ref:`Validation Actions`.

You can see the configuration for ``action_list_operator`` in your ``great_expectations.yml`` file. With comments removed, it looks like this:

.. code-block:: yaml

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
      # - name: send_slack_notification_on_validation_result
      #   action:
      #     class_name: SlackNotificationAction
      #     # put the actual webhook URL in the uncommitted/config_variables.yml file
      #     slack_webhook: ${validation_notification_slack_webhook}
      #     notify_on: all # possible values: "all", "failure", "success"
      #     renderer:
      #       module_name: great_expectations.render.renderer.slack_renderer
      #       class_name: SlackRenderer

You can see that the ``action_list`` for your validation Operator contains three Validation Actions. After each run using this operator...

1. ``store_validation_result`` : store the :ref:`Validation Results`.
2. ``store_evaluation_params`` : store :ref:`Evaluation Parameters`.
3. ``update_data_docs`` : update your :ref:`Data Docs`.

A fourth action, ``send_slack_notification_on_validation_result``, will trigger a notification in slack.

Checkpoints can be run like applications from the command line or cron:

.. code-block:: bash

    great_expectations checkpoint run my_checkpoint

You can also generate Checkpoint scripts that you can edit and run using python, or within data orchestration tools like airflow, prefect, kedro, dagster, flyte, etc.

.. code-block:: bash

    great_expectations checkpoint script my_checkpoint

Now that you know how to configure Checkpoints, let's proceed to the last step of the tutorial: :ref:`Customize your deployment`.