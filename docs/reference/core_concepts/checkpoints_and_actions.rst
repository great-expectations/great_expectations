.. _checkpoints_and_actions:

#############################################
Checkpoints And Actions Introduction
#############################################

.. attention::

  As part of the new modular expectations API in Great Expectations, Validation Operators are evolving into Checkpoints.
  At some point in the future Validation Operators will be fully deprecated.

The `validate` method evaluates one Batch of data against one Expectation Suite and returns a dictionary of validation results. This is sufficient when you explore your data and get to know Great Expectations.
When deploying Great Expectations in a real data pipeline, you will typically discover additional needs:

* Validating a group of Batches that are logically related (for example, a Checkpoint for all staging tables).
* Validating a Batch against several Expectation Suites (for example, run three suites to protect a machine learning model `churn.critical`, `churn.warning`, `churn.drift`).
* Doing something with the validation results (for example, saving them for later review, sending notifications in case of failures, etc.).

Checkpoints provide a convenient abstraction for bundling the validation of a Batch (or Batches) of data against an Expectation Suite (or several), as well as the actions that should be taken after the validation. Like Expectation Suites and Validation Results, Checkpoints are managed using a Data Context, and have their own Store which is used to persist their configurations to yaml files. These configurations can be committed to version control and shared with your team.

The classes that implement Checkpoints are in the :py:mod:`great_expectations.checkpoint` module.

.. _validation_actions:
***************************************************
Validation Actions
***************************************************

Actions are Python classes with a `run` method that takes a result of validating a Batch against an Expectation Suite and does something with it (e.g., save validation results to the disk or send a Slack notification).
Classes that implement this API can be configured to be added to the list of actions used by a particular Checkpoint.

Classes that implement Actions can be found in the :py:mod:`great_expectations.checkpoint.actions` module.

***************************************************
Checkpoint Configuration
***************************************************

A Checkpoint uses its configuration to determine what data to validate against which Expectation Suite(s), and what actions to perform on the validation results - these validations and actions are executed by calling a Checkpoint's ``run`` method (analogous to calling ``validate`` with a single Batch). Checkpoint configurations are very flexible. At one end of the spectrum, you can specify a complete configuration in a Checkpoint's yaml file, and simply call ``my_checkpoint.run()``. At the other end, you can specify a minimal configuration in the yaml file and provide missing keys as kwargs when calling ``run``.

At runtime, a Checkpoint configuration has three required and three optional keys, and is built using a combination of the yaml configuration and any kwargs passed in at runtime:

**Required**
  #. ``name``: user-selected Checkpoint name (e.g. "staging_tables")
  #. ``config_version``: version number of the Checkpoint configuration
  #. ``validations``: a list of dictionaries that describe each validation that is to be executed, including any actions. Each validation dictionary has three required and three optional keys:

    *Required*
      #. ``batch_request``: a dictionary describing the batch of data to validate (learn more about specifying batches here: :ref:`Batches<specifying_batches>`)
      #. ``expectation_suite_name``: the name of the Expectation Suite to validate the batch of data against
      #. ``action_list``: a list of actions to perform after each batch is validated
    *Optional*
      #. ``name``: providing a name will allow referencing the validation inside the run by name (e.g. "user_table_validation")
      #. ``evaluation_parameters``: used to define named parameters using Great Expectations :ref:`evaluation parameter syntax<evaluation_parameters>`
      #. ``runtime_configuration``: provided to the Validator's ``runtime_configuration`` (e.g. ``result_format``)

**Optional**
  #. ``class_name``: the class of the Checkpoint to be instantiated, defaults to ``Checkpoint``
  #. ``template_name``: the name of another Checkpoint to use as a base template
  #. ``run_name_template``: a template to create run names, using environment variables and datetime-template syntax (e.g. "%Y-%M-staging-$MY_ENV_VAR")

**Configuration Defaults and Parameter Override Behavior**

Checkpoint configurations follow a nested pattern, where more general keys provide defaults for more specific ones. For instance, any required validation dictionary keys (e.g. ``expectation_suite_name``) can be specified at the top-level (i.e. peers with the validations list), serving as runtime defaults. Starting at the earliest reference template, if a configuration key is re-specified, its value can be appended, updated, replaced, or cause an error when redefined.

  *Replaced*
    * ``name``
    * ``module_name``
    * ``class_name``
    * ``run_name_template``
    * ``expectation_suite_name``

  *Updated*
    * ``batch_request``: at runtime, if a key is re-defined, an error will be thrown
    * ``action_list``: actions that share the same user-defined name will be updated, otherwise a new action will be appended
    * ``evaluation_parameters``
    * ``runtime_configuration``

  *Appended*
    * ``action_list``: actions that share the same user-defined name will be updated, otherwise a new action will be appended
    * ``validations``

Configuration Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. content-tabs::

  .. tab-container:: tab0
    :title: No nesting, all options specified

    **yaml**:

    .. code-block:: yaml

      name: my_fancy_checkpoint
      config_version: 1
      class_name: Checkpoint
      run_name_template: %Y-%M-foo-bar-template-$VAR
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
            param1: $MY_PARAM
            param2: 1 + $OLD_PARAM
          runtime_configuration:
              result_format:
                result_format: BASIC
                partial_unexpected_count: 20

    **runtime**:

    .. code-block:: python

      context.run_checkpoint(checkpoint_name="my_fancy_checkpoint")

  .. tab-container:: tab1
    :title: Nesting with defaults for most keys

    Two validations will occur when this Checkpoint is run.

    **yaml**:

    .. code-block:: yaml

      name: my_fancy_checkpoint
      config_version: 1
      class_name: Checkpoint
      run_name_template: %Y-%M-foo-bar-template-$VAR
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
          param1: $MY_PARAM
          param2: 1 + $OLD_PARAM
      runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20

    **runtime**:

    .. code-block:: python

      context.run_checkpoint(checkpoint_name="my_fancy_checkpoint")

  .. tab-container:: tab2
    :title: Keys provided at runtime

    **yaml**:

    .. code-block:: yaml

      name: my_base_checkpoint
      config_version: 1
      class_name: Checkpoint
      run_name_template: %Y-%M-foo-bar-template-$VAR
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
          param1: $MY_PARAM
          param2: 1 + $OLD_PARAM
      runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20

    **runtime**:

    .. code-block:: python

      context.run_checkpoint(
          "my_base_checkpoint",
          validations=[{
              "batch_request": {
                  "datasource_name": "my_datasource"
                  "data_connector_name": "my_special_data_connector"
                  "data_asset_name": "users"
                  "partition_request": {
                      "index": -1
                  }
              },
              expectation_suite_name": "users.delivery"
          },
          {
              "batch_request": {
                  "datasource_name": "my_datasource"
                  "data_connector_name": "my_other_data_connector"
                  "data_asset_name": "users"
                  "partition_request": {
                      "index": -2
                  }
              },
              expectation_suite_name": "users.delivery"
          }]
      )

  .. tab-container:: tab3
    :title: Template used to slim down runtime code

    This configuration references the Checkpoint detailed in the previous example ("Keys provided at runtime"), allowing the runtime call to ``run_checkpoint`` to be much slimmer.

    **yaml**:

    .. code-block:: yaml

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

    **runtime**:

    .. code-block:: python

      # Same as the parameterized run of previous "my_base_checkpoint" example
      context.run_checkpoint(
          "my_fancy_checkpoint"
      )

SimpleCheckpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~

