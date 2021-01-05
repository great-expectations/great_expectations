.. _how_to_guides_how_to_configure_a_new_checkpoint_using_test_yaml_config:

How to configure a New Checkpoint using ``test_yaml_config``
==================================================================

``test_yaml_config`` is a convenience method for configuring the moving parts of a Great Expectations deployment. It allows you to quickly test out configs for Datasources, Stores, and Checkpoints. For many deployments of Great Expectations, these components (plus Expectations) are the only ones you'll need.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`

``test_yaml_config`` is primarily intended for use within a notebook, where you can iterate through an edit-run-check loop in seconds.

Steps
-----

#. **Instantiate a DataContext**

    Create a new Jupyter Notebook and instantiate a DataContext by running the following lines:

    .. code-block:: python

        import great_expectations as ge
        context = ge.get_context()

#. **Create or copy a yaml config**

    You can create your own, or copy an example. For this example, we'll demonstrate using a basic Checkpoint configuration.

    .. code-block:: python

        config = """
        name: my_fancy_checkpoint
        config_version: 1
        class_name: Checkpoint
        # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
        # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
        # run_name_template: %Y-%M-foo-bar-template-"$VAR"
        validations:
          - batch_request:
              datasource_name: my_datasource
              data_connector_name: my_special_data_connector
              data_asset_name: users
              partition_request:
                index: -1
            expectation_suite_name: users.delivery
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
            evaluation_parameters:
              # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
              # param1: "$MY_PARAM"
              # param2: 1 + "$OLD_PARAM"
              param1: 1
              param2: 2
            runtime_configuration:
              result_format:
                result_format: BASIC
                partial_unexpected_count: 20
        """

#. **Run context.test_yaml_config.**

    .. code-block:: python

        context.test_yaml_config(
            name="my_fancy_checkpoint",
            yaml_config=config,
        )

    When executed, ``test_yaml_config`` will instantiate the component and run through a ``self_check`` procedure to verify that the component works as expected.

    In the case of a Checkpoint, this means

        1. validating the `yaml` configuration,
        2. verifying that the Checkpoint class with the given configuration, if valid, can be instantiated, and
        3. printing warnings in case certain parts of the configuration, while valid, may be incomplete and need to be better specified for a successful Checkpoint operation.

    The output will look something like this:

    .. code-block:: bash

        Attempting to instantiate class from config...
        Instantiating as a Checkpoint, since class_name is Checkpoint

        Successfully instantiated Checkpoint

        Checkpoint class name: Checkpoint

    If something about your configuration wasn't set up correctly, ``test_yaml_config`` will raise an error.  Whenever possible, test_yaml_config provides helpful warnings and error messages. It can't solve every problem, but it can solve many.

    The following example of a warning illustrates the point about ``test_yaml_config`` making a best effort to be helpful:

    .. code-block:: bash
        Attempting to instantiate class from config...
        Successfully instantiated Checkpoint

        Checkpoint class name: Checkpoint
        WARNING  great_expectations.checkpoint.checkpoint:checkpoint.py:320 Your current Checkpoint configuration has an empty or missing "validations" attribute.  This means
        you must either update your checkpoint configuration or provide an appropriate validations list programmatically (i.e., when your Checkpoint is run).

    The next two examples demonstrate what happens in case of a Checkpoint configuration error:

    .. code-block:: bash
        KeyError: "Neither config : ordereddict([('config_version', 1)]) nor config_defaults : {} contains a module_name key."

    .. code-block:: bash
        great_expectations.exceptions.exceptions.InvalidConfigError: Your current Checkpoint configuration is incomplete.  Please update your checkpoint configuration to continue.


#. **Iterate as necessary.**

    From here, iterate by editing your config and re-running ``test_yaml_config``, adding config blocks for additional validations, action_list constituent actions, batch_request variations, etc. Please see <doc> for options and ideas.

#. **(Optional:) Test running the new Checkpoint.**

    Note that when ``test_yaml_config`` runs successfully, it saves the specified Checkpoint configuration to the Store Backend configured for the Checkpoint Configuration store of your DataContext. This means that you can also test ``context.run_checkpoint``, right within your notebook:

    .. code-block:: python

        validation_results: List[ValidationOperatorResult] = context.run_checkpoint(
            checkpoint_name="my_fancy_checkpoint",
        )

   Before running a Checkpoint, make sure that all classes referred to in the configuration exist.  The same applies to the expectation suites.

   When `run_checkpoint` returns, the elements of the `validation_results` list can then be checked for the value of the `success` field and other information associated with running the specified actions.

#. **Check your stored Checkpoint config.**
    If the Store Backend of your Checkpoint Store is on the local filesystem, you can navigate to the `base_directory` for (configured in `great_expectations.yml`) and find the configuration files corresponding to the Checkpoints you created.

#. **Additional Checkpoint configration examples.**

    In this example, the Checkpoint configuration uses the nesting of `batch_request` sections inside the `validations` block so as to use the defaults defined at the top level.

    .. code-block:: python

        config = """
        name: my_fancy_checkpoint
        config_version: 1
        class_name: Checkpoint
        # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
        # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
        # run_name_template: %Y-%M-foo-bar-template-"$VAR"
        validations:
          - batch_request:
              datasource_name: my_datasource
              data_connector_name: my_special_data_connector
              data_asset_name: users
              partition_request:
                index: -1
          - batch_request:
              datasource_name: my_datasource
              data_connector_name: my_other_data_connector
              data_asset_name: users
              partition_request:
                index: -2
        expectation_suite_name: users.delivery
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
        evaluation_parameters:
          # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
          # param1: "$MY_PARAM"
          # param2: 1 + "$OLD_PARAM"
          param1: 1
          param2: 2
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
        """


    The following Checkpoint configuration runs the top-level `action_list` against the top-level `batch_request` as well as the locally-specified `action_list` against the top-level `batch_request`.

    .. code-block:: python

        config = """
        name: airflow_users_node_3
        config_version: 1
        class_name: Checkpoint
        batch_request:
            datasource_name: my_datasource
            data_connector_name: my_special_data_connector
            data_asset_name: users
            partition_request:
                index: -1
        validations:
          - expectation_suite_name: users.warning  # runs the top-level action list against the top-level batch_request
          - expectation_suite_name: users.error  # runs the locally-specified_action_list (?UNION THE TOP LEVEL?) against the top-level batch_request
            action_list:
            - name: quarantine_failed_data
              action:
                  class_name: CreateQuarantineData
            - name: advance_passed_data
              action:
                  class_name: CreatePassedData
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
        evaluation_parameters:
            environment: $GE_ENVIRONMENT
            tolerance: 0.01
        runtime_configuration:
            result_format:
              result_format: BASIC
              partial_unexpected_count: 20
        """


    The Checkpoint mechanism also offers the convenience of templates.  The first Checkpoint configuration is that of a valid Checkpoint in the sense that it can be run as long as all the parameters not present in the configuration are specified in the `run_checkpoint` API call.

    .. code-block:: python

        config = """
        name: my_base_checkpoint
        config_version: 1
        class_name: Checkpoint
        # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
        # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
        # run_name_template: %Y-%M-foo-bar-template-"$VAR"
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
        evaluation_parameters:
          # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
          # param1: "$MY_PARAM"
          # param2: 1 + "$OLD_PARAM"
          param1: 1
          param2: 2
        runtime_configuration:
            result_format:
              result_format: BASIC
              partial_unexpected_count: 20
        """

   .. code-block:: python

        validation_results: List[ValidationOperatorResult]

        validation_results = data_context.run_checkpoint(
            checkpoint_name="my_base_checkpoint",
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_special_data_connector",
                        "data_asset_name": "users",
                        "partition_request": {
                            "index": -1,
                        },
                    },
                    "expectation_suite_name": "users.delivery",
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector",
                        "data_asset_name": "users",
                        "partition_request": {
                            "index": -2,
                        },
                    },
                    "expectation_suite_name": "users.delivery",
                },
            ],
        )

    However, the `run_checkpoint` method can be simplified by configuring a separate Checkpoint that uses the above Checkpoint as a template and includes the settings previously specified in the `run_checkpoint` method:

    .. code-block:: python

        config = """
        name: my_fancy_checkpoint
        config_version: 1
        class_name: Checkpoint
        template_name: my_base_checkpoint
        validations:
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_special_data_connector
            data_asset_name: users
            partition_request:
              index: -1
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_other_data_connector
            data_asset_name: users
            partition_request:
              index: -2
        expectation_suite_name: users.delivery
        """

    Now the `run_checkpoint` method is as simple as in the previous examples:

    .. code-block:: python

        validation_results = context.run_checkpoint(
            checkpoint_name="my_fancy_checkpoint",
        )

    The `validation_results` in both cases (the parameterized `run_checkpoint` method and the configuration that incorporates another configuration as a template) are the same.


    The final example presents a Checkpoint configuration that is suitable for the use in a pipeline managed by Airflow.

    .. code-block:: python

        config = """
        name: airflow_checkpoint
        config_version: 1
        class_name: Checkpoint
        validations:
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_runtime_data_connector
            data_asset_name: IN_MEMORY_DATA_ASSET
        expectation_suite_name: users.delivery
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
        """


   To run this Checkpoint, the `batch_request` with the `batch_data` attribute needs to be specified explicitly as part of the `run_checkpoint()` API call, because the the data to be validated is accessible only dynamically during the execution of the pipeline.

   .. code-block:: python

        validation_results: List[ValidationOperatorResult] = data_context.run_checkpoint(
            checkpoint_name=checkpoint.config.name,
            batch_request={
                "batch_data": my_data_frame,
                "partition_request": {
                    "partition_identifiers": {
                        "run_id": airflow_run_id,
                    }
                },
            },
        )



Additional Resources
--------------------


.. discourse::
   :topic_identifier: <TBD>
