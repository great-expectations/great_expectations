.. _checkpoints_and_actions:

#############################################
Checkpoints and Actions Introduction
#############################################

.. attention::

  As part of the new modular expectations API in Great Expectations, Validation Operators are evolving into Checkpoints.
  At some point in the future Validation Operators will be fully deprecated.

The ``batch.validate()`` method evaluates one Batch of data against one Expectation Suite and returns a dictionary of validation results. This is sufficient when you explore your data and get to know Great Expectations.
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

Actions are Python classes with a `run` method that takes the result of validating a Batch against an Expectation Suite and does something with it (e.g., save validation results to disk, or send a Slack notification).
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

Checkpoint configurations follow a nested pattern, where more general keys provide defaults for more specific ones. For instance, any required validation dictionary keys (e.g. ``expectation_suite_name``) can be specified at the top-level (i.e. at the same level as the validations list), serving as runtime defaults. Starting at the earliest reference template, if a configuration key is re-specified, its value can be appended, updated, replaced, or cause an error when redefined.

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

**************************
SimpleCheckpoint Class
**************************

For many use cases, the SimpleCheckpoint class can be used to simplify the process of specifying a Checkpoint configuration. SimpleCheckpoint provides a basic set of actions - store validation result, store evaluation parameters, update data docs, and optionally, send a Slack notification - allowing you to omit an ``action_list`` from your configuration and at runtime.

Configurations using the SimpleCheckpoint class can optionally specify four additional top-level keys that customize and extend the basic set of default actions:

  * ``site_names``: a list of Data Docs site names to update as part of the update data docs action - defaults to "all"
  * ``slack_webhook``: if provided, an action will be added that sends a Slack notification to the provided webhook
  * ``notify_on``: used to define when a notification is fired, according to validation result outcome - ``all``, ``failure``, or ``success``. Defaults to ``all``.
  * ``notify_with``: a list of Data Docs site names for which to include a URL in any notifications - defaults to ``all``

*************************
CheckpointResult
*************************

The return object of a Checkpoint run is a CheckpointResult object. The ``run_results`` attribute forms the backbone of this type and defines the basic contract for what a Checkpoint's ``run`` method returns. It is a dictionary where the top-level keys are the ValidationResultIdentifiers of the validation results generated in the run. Each value is a dictionary having at minimum, a ``validation_result`` key containing an ExpectationSuiteValidationResult and an ``actions_results`` key containing a dictionary where the top-level keys are names of actions performed after that particular validation, with values containing any relevant outputs of that action (at minimum and in many cases, this would just be a dictionary with the action's ``class_name``).

The ``run_results`` dictionary can contain other keys that are relevant for a specific checkpoint implementation. For example, the ``run_results`` dictionary from a WarningAndFailureExpectationSuiteCheckpoint might have an extra key named "expectation_suite_severity_level" to indicate if the suite is at either a "warning" or "failure" level.

CheckpointResult objects include many convenience methods (e.g. ``list_data_asset_names``) that make working with Checkpoint results easier. You can learn more about these methods in the documentation for :py:class:`~great_expectations.checkpoint.types.checkpoint_result.CheckpointResult`.

Example CheckpointResult:

.. code-block:: python

    {
        "run_id": run_identifier_object,
        "run_results": {
            validation_result_identifier_object: {
                "validation_result": expectation_suite_validation_result_object,
                "actions_results": {
                    "my_action_name_that_stores_validation_results": {
                        "class": "StoreValidationResultAction"
                    }
                }
            }
        }
        "checkpoint_config": my_checkpoint_config_object,
        "success": True
    }

**************************************
Checkpoint Configuration Examples
**************************************

.. content-tabs::

  .. tab-container:: tab0
    :title: No nesting

    This configuration specifies full validation dictionaries - no nesting (defaults) are used. When run, this Checkpoint will perform one validation of a single batch of data, against a single Expectation Suite ("users.delivery").

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
            data_connector_query:
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
    :title: Nesting with defaults

    This configuration specifies four top-level keys (``expectation_suite_name``, ``action_list``, ``evaluation_parameters``, and ``runtime_configuration``) that can serve as defaults for each validation, allowing the keys to be omitted from the validation dictionaries. When run, this Checkpoint will perform two validations of two different batches of data, both against the same Expectation Suite ("users.delivery"). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

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
            data_connector_query:
              index: -1
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_other_data_connector
            data_asset_name: users
            data_connector_query:
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
    :title: Keys passed at runtime

    This configuration omits the ``validations`` key from the yaml, which means a ``validations`` list must be provided when the Checkpoint is run. Because ``action_list``, ``evaluation_parameters``, and ``runtime_configuration`` appear as top-level keys in the yaml configuration, these keys may be omitted from the validation dictionaries, unless a non-default value is desired. When run, this Checkpoint will perform two validations of two different batches of data, with each batch of data validated against a different Expectation Suite ("users.delivery" and "users.diagnostic", respectively). Each validation will trigger the same set of actions and use the same evaluation parameters and runtime configuration.

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
          checkpoint_name="my_base_checkpoint",
          validations=[
              {
                  "batch_request": {
                      "datasource_name": "my_datasource"
                      "data_connector_name": "my_special_data_connector"
                      "data_asset_name": "users"
                      "data_connector_query": {
                          "index": -1
                      }
                  },
                  "expectation_suite_name": "users.delivery"
              },
              {
                  "batch_request": {
                      "datasource_name": "my_datasource"
                      "data_connector_name": "my_other_data_connector"
                      "data_asset_name": "users"
                      "data_connector_query": {
                          "index": -2
                      }
                  },
                  "expectation_suite_name": "users.diagnostic"
              }
          ]
      )

  .. tab-container:: tab3
    :title: Using template

    This configuration references the Checkpoint detailed in the previous example ("Keys passed at runtime"), allowing the runtime call to ``run_checkpoint`` to be much slimmer.

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
            data_connector_query:
              index: -1
          expectation_suite_name: users.delivery
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_other_data_connector
            data_asset_name: users
            data_connector_query:
              index: -2
          expectation_suite_name: users.diagnostic

    **runtime**:

    .. code-block:: python

      # Same as the parameterized run of previous "my_base_checkpoint" example
      context.run_checkpoint(checkpoint_name="my_fancy_checkpoint")

  .. tab-container:: tab4
    :title: SimpleCheckpoint

    This configuration specifies the SimpleCheckpoint class under the ``class_name`` key, allowing for a much slimmer configuration.

    **yaml, using SimpleCheckpoint**:

    .. code-block:: yaml

      name: my_checkpoint
      config_version: 1
      class_name: SimpleCheckpoint
      validations:
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_data_connector
            data_asset_name: MyDataAsset
            data_connector_query:
              index: -1
          expectation_suite_name: my_suite
      site_names:
        - my_diagnostic_data_docs_site
      slack_webhook: http://my_slack_webhook.com
      notify_on: failure
      notify_with:
        - my_diagnostic_data_docs_site

    **Equivalent yaml, using Checkpoint**:

    .. code-block:: yaml

      name: my_checkpoint
      config_version: 1
      class_name: Checkpoint
      validations:
        - batch_request:
            datasource_name: my_datasource
            data_connector_name: my_data_connector
            data_asset_name: MyDataAsset
            data_connector_query:
              index: -1
          expectation_suite_name: my_suite
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
            site_names:
              - my_diagnostic_data_docs_site
        - name: send_slack_notification
          action:
            class_name: SlackNotificationAction
            slack_webhook: http://my_slack_webhook.com
            notify_on: failure
            notify_with:
              - my_diagnostic_data_docs_site
            renderer:
              module_name: great_expectations.render.renderer.slack_renderer
              class_name: SlackRenderer

    **runtime**:

    .. code-block:: python

      context.run_checkpoint(checkpoint_name="my_checkpoint")
