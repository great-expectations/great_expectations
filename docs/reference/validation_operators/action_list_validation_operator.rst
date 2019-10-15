.. _action_list_validation_operator:

================================================================================
ActionListValidationOperator
================================================================================


ActionListValidationOperator validates each batch in its `run` method's `assets_to_validate` argument against the expectation suite included within that batch.

Then it invokes a list of configured actions on every validation result.

Each action in the list must be an instance of NamespacedValidationAction
class (or its descendants). Read more about actions here: :ref:`actions`.

The init command includes this operator in the default configuration file.


Configuration
--------------

An instance of ActionListValidationOperator is included in the default configuration file `great_expectations.yml` that `great_expectations init` command creates.

.. code-block:: yaml

    perform_action_list_operator: # this is the name you will use when you invoke the operator
        class_name: ActionListValidationOperator

        # the operator will call the following actions on each validation result
        # you can remove or add actions to this list. See the details in the actions
        # reference
        action_list:
          - name: store_validation_result
            action:
              class_name: StoreAction
              target_store_name: validations_store
          - name: send_slack_notification_on_validation_result
            action:
              class_name: SlackNotificationAction
              # put the actual webhook URL in the uncommitted/config_variables.yml file
              slack_webhook: ${validation_notification_slack_webhook}
             notify_on: all # possible values: "all", "failure", "success"
              renderer:
                module_name: great_expectations.render.renderer.slack_renderer
                class_name: SlackRenderer
          - name: update_data_docs
            action:
              class_name: UpdateDataDocsAction


Invocation
-----------

This is an example of invoking an instance of a Validation Operator from Python:

::

    results = context.run_validation_operator(
        assets_to_validate=[batch0, batch1, ...],
        run_id="some_string_that_uniquely_identifies_this_run",
        validation_operator_name="perform_action_list_operator",
    )

* `assets_to_validate` - an iterable that specifies the data assets that the operator will validate. The members of the list can be either batches or triples that will allow the operator to fetch the batch: (data_asset_name, expectation_suite_name, batch_kwargs) using this method: :py:meth:`~great_expectations.data_context.ConfigOnlyDataContext.get_batch`
* run_id - pipeline run id, a timestamp or any other string that is meaningful to you and will help you refer to the result of this operation later
* validation_operator_name you can instances of a class that implements a Validation Operator

The `run` method returns an object that looks like this:

::

    {
        'success: True/False, (True if all validations are successful)
        'details': {
            great_expectations.data_context.types.ExpectationSuiteIdentifier: 
                {
                'validation_result': :ref:validation_result
                'actions_results':
                    {
                    'action_0_name': action result object (defined by the action),
                    'action_1_name': action result object (defined by the action),
                    ...
                    'action_n_name`: action result object (defined by the action)
                    }
                }
        }
    }