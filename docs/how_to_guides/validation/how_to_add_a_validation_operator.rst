.. _how_to_guides__validation__how_to_add_a_validation_operator:

How to add a Validation Operator
======================================

This guide will help you add a new instance of a :ref:`Validation Operator <validation_operators_and_actions>`. Validation Operators give you the ability to validate multiple batches
of data together, and kick off Validation Actions based on the results of validation. Additionally, each Validation Operator encodes a set of business rules around validation. When you add an instance of a Validation Operator,
you can use its business rules in your project.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`.
  - Created at least one Expectation Suite.
  - Created at least one :ref:`Checkpoint <how_to_guides__validation__how_to_create_a_new_checkpoint>`. You will need it in order to test that your new Validation Operator is working.

Steps
-----

The snippet below shows a portion of your ``great_expectations.yml`` configuration after you perform the following steps. The steps will explain each line in this snippet.

.. code-block:: yaml
   :linenos:

   validation_operators:
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
     # Next Validation Operator was added manually
     my_second_validation_operator:
       class_name: ActionListValidationOperator
       action_list:
       - name: store_validation_result
         action:
           class_name: StoreValidationResultAction

1. Open your project's ``great_expectations.yml`` configuration file and navigate to the ``validation_operators`` section (line 1 in the snippet). This section contains the Validation Operator instance ``action_list_operator`` that was automatically created by the ``great_expectations init`` CLI command.
2. Add a new block after the existing Validation Operator instances. The name of the block is the name you are giving to the new Validation Operator instance (line 15 in the snippet). These names must be unique within a project.
3. Add a ``class_name`` attribute in the new block you added in the previous step (line 16 in the snippet). The value is the name of the class that implements the Validation Operator that you are adding. This can be one of the classes that are included in Great Expectations or a class that you implemented. This example adds another instance of `ActionListValidationOperator`.

  .. admonition:: Note:

    - If you are adding a custom Validation Operator, you will have to add a `module_name` attribute in addition to `class_name`. You will find more details about custom Validation Operators in this :ref:`guide <how_to_guides__validation__how_to_implement_a_custom_validation_operator>`.

4. Consult the reference documentation of the class that implements the Validation Operator you are adding for additional properties (required or optional) that are specific to that class. The snippet above configured one such property specific to :ref:`ActionListValidationOperator<how_to_guides__validation__how_to_configure_an_actionlistvalidationoperator>`.
5. Test that your new Validation Operator is configured correctly:

   1. Open the configuration file of a Checkpoint you created earlier and replace the value of `validation_operator_name` with the value from Step 2 above. The details of Checkpoint configuration can be found in this :ref:`guide<how_to_guides__validation__how_to_add_validations_data_or_suites_to_a_checkpoint>`.
   2. Run the Checkpoint and verify that no errors are thrown. You can run the Checkpoint from the CLI as explained :ref:`here<how_to_guides__validation__how_to_run_a_checkpoint_in_terminal>` or from Python, as explained :ref:`here<how_to_guides__validation__how_to_run_a_checkpoint_in_python>`.


Additional notes
----------------


Additional resources
--------------------

.. discourse::
    :topic_identifier: 217
