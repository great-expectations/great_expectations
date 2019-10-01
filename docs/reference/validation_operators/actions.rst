.. _actions:

================================================================================
Actions
================================================================================

An action is a way to take an arbitrary method and make it configurable and runnable within a data context.

The only requirement from an action is for it to have a take_action method.


SlackNotificationAction
------------------------

SlackNotificationAction is a namespeace-aware validation action that sends a Slack notification to a given webhook

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

    - name: send_slack_notification_on_validation_result
    action:
      class_name: SlackNotificationAction
      # put the actual webhook URL in the uncommitted/config_variables.yml file
      slack_webhook: ${validation_notification_slack_webhook}
     notify_on: all # possible values: "all", "failure", "success"
      renderer:
        module_name: great_expectations.render.renderer.slack_renderer
        class_name: SlackRenderer


StoreAction
-----------

StoreAction is a namespeace-aware validation action that stores a validation result
in the store.

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml

    - name: store_validation_result
    action:
      class_name: StoreAction
      # name of the store where the actions will store validation results
      # the name must refer to a store that is configured in the great_expectations.yml file
      target_store_name: local_validation_result_store


ExtractAndStoreEvaluationParamsAction
-------------------------------------

ExtractAndStoreEvaluationParamsAction is a namespeace-aware validation action that
extracts evaluation parameters from a validation result and stores them in the store
configured for this action.

Evaluation parameters allow expectations to refer to statistics/metrics computed
in the process of validating other prior expectations.

Configuration
~~~~~~~~~~~~~

    - name: store_evaluation_params
    action:
      class_name: ExtractAndStoreEvaluationParamsAction
      # name of the store where the action will store the parameters
      # the name must refer to a store that is configured in the great_expectations.yml file
      target_store_name: evaluation_parameter_store
