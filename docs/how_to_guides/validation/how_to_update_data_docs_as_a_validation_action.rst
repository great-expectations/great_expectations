.. _how_to_guides__validation__how_to_update_data_docs_as_a_validation_action:

How to update Data Docs as a Validation Action
=================================================

This guide will explain how to use a Validation Action to update Data Docs sites with new validation results from Validation Operator runs.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Set up a working deployment of Great Expectations <getting_started>`
    - :ref:`Set up an ActionListValidationOperator <how_to_guides__validation__how_to_configure_an_actionlistvalidationoperator>` **or**
    - :ref:`Set up a WarningAndFailureExpectationSuitesValidationOperator <how_to_guides__validation__how_to_configure_a_warningandfailureexpectationsuitesvalidationoperator>`

Steps
------

1. Add the ``UpdateDataDocsAction`` action to the ``action_list`` key of the ``ActionListValidationOperator`` or ``WarningAndFailureExpectationSuitesValidationOperator`` config in your ``great_expectations.yml``. This action will update all configured Data Docs sites with the new validation from the Validation Operator run. 

  .. admonition:: Note:

    The ``StoreValidationResultAction`` action must appear before this action, since Data Docs are rendered from validation results from the store.

  .. code-block:: yaml
  
    validation_operators:
      action_list_operator: # this is a user-selected name
        class_name: ActionListValidationOperator
        action_list:
        - name: store_validation_result # this is a user-selected name
          action:
            class_name: StoreValidationResultAction
        - name: update_data_docs # this is a user-selected name
          action:
            class_name: UpdateDataDocsAction

2. If you only want to update certain configured Data Docs sites, add a ``site_names`` key to the ``UpdateDataDocsAction`` config.

  .. code-block:: yaml
  
    validation_operators:
      action_list_operator: # this is a user-selected name
        class_name: ActionListValidationOperator
        action_list:
        - name: store_validation_result # this is a user-selected name
          action:
            class_name: StoreValidationResultAction
        - name: update_data_docs # this is a user-selected name
          action:
            class_name: UpdateDataDocsAction
            site_names:
              - team_site

Additional resources
--------------------

- :ref:`how_to_guides__validation__how_to_store_validation_results_as_a_validation_action`
- :ref:`validation_operators_and_actions`

Comments
--------

.. discourse::
    :topic_identifier: 223
