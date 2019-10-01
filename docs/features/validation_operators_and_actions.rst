.. _validation_operators_and_actions:


##############################################
Validation Operators And Actions Introduction
##############################################

The `validate` method evaluates one batch of data against one expectation suite and returns a dictionary of validation results. This is sufficient when you explore your data and get to know Great Expectations.
When deploying Great Expectations in a real data pipeline, you will typically discover additional needs:

* validating a group of batches that are logically related
* validating a batch against several expectation suites
* doing something with the validation results (e.g., saving them for a later review, sending notifications in case of failures, etc.).

Validation Operators provide a convenient abstraction for both bundling the validation of multiple expectation suites and the actions that should be taken after the validation.

*************************************************************
How do you find a Validation Operator that is right for you?
*************************************************************

Each Validation Operator encodes a particular set of business rules around validation. Currently, two Validation Operator implementations are currently included in the Great Expectations distribution.

The classes that implement them are in :py:mod:`great_expectations.validation_operators` module.


PerformActionListValidationOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PerformActionListValidationOperator validates each batch in the list that is passed as `assets_to_validate` argument to its `run` method against the expectation suite included within that batch and then invokes a list of configured actions on every validation result.

Actions are Python classes with `run` method that takes a result of validating a batch against an expectation suite and do something with it (e.g., save validation results to the disk or send a Slack notification). Classes that implement this API can be configured to be added to the list of actions for this operator (and other operators that use actions). Read more about actions here: :ref:`actions`.

An instance of this operator is included in the default configuration file `great_expectations.yml` that `great_expectations init` command creates.

A user can choose the actions to perform in their instance of the operator.

Read more about PerformActionListValidationOperator here: :ref:`perform_action_list_validation_operator`.

RunWarningAndFailureExpectationSuitesValidationOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

RunWarningAndFailureExpectationSuitesValidationOperator implements a business logic pattern that many data practitioners consider useful.

Group the expectations that you create about a data asset into two expectation suites. The critical expectations that you are confident that the data should meet go into the expectation suite that this operator calls "failure" (meaning, not meeting these expectations should be considered a failure in the pipeline). The rest of expectations go into the "warning" expectation suite.

The failure Expectation Suite contains expectations that are considered important enough to justify stopping the pipeline when they are violated.

RunWarningAndFailureExpectationSuitesValidationOperator retrieves two expectation suites for every data asset in the `assets_to_validate` argument of its `run` method.

After completing all the validations, it sends a Slack notification with the success status.

Read more about PerformActionListValidationOperator here: :ref:`run_warning_and_failure_expectation_suites_validation_operator`

************************************************
Can you implement your own Validation Operator?
************************************************

If you wish to implement some validation handling logic that is not supported by the operators included in Great Expectations, follow these steps:

* Extend the :py:class:`great_expectations.validation_operators.ValidationOperator` base class
* Implement the `run` method in your new class
* Put your class in the plugins directory of your project (see `plugins_directory` property in the `great_expectations.yml configuration file`)

Once these steps are complete, your new Validation Operator can be configured and used.

If you think that the business logic of your operator can be useful to other data practitioners, please consider contributing it to Great Expectations.

**********************************************
How do you start using a Validation Operator?
**********************************************

If you are using a Validation Operator that came with GE or was contributed by another developer,
you can get to a rich set of useful behaviors with very little coding. This is done by editing the operator's configuration in the GE configuration file and by extending the operator in case you want to add new behavior.

To use a Validation Operator (one that is included in Great Expectations or one that you implemented in your project's plugins directory), you need to configure an instance of the operator in your `great_expectations.yml` file and then invoke this instance from your Python code.


Configuring an operator
~~~~~~~~~~~~~~~~~~~~~~~

All Validation Operators configuration blocks should appear under `validation_operators` key in `great_expectations.yml`

To configure an instance of an operator in your Great Expectations context, create a key under `validation_operators`. This is the name of you will use to invoke this operator.

.. code-block:: python

    my_operator:
        class_name: TheClassThatImplementsMyOperator
        foo: bar

In the example of an operator config block above:

* the `class_name` value is the name of the class that implements this operator. The key `module_name` must also be specified if the class is not in the default module.
* the `foo` key specifies the value of the `foo` argument of this class' constructor. Since every operator class might define its own constructor, the keys will vary.


Invoking an operator
~~~~~~~~~~~~~~~~~~~~

This is an example of invoking an instance of a Validation Operator from Python:

.. code-block:: python

    results = context.run_validation_operator(
        assets_to_validate=[batch0, batch1, ...],
        run_identifier="some_string_that_uniquely_identifies_this_run",
        validation_operator_name="perform_action_list_operator",
    )

* `assets_to_validate` - a list that specifies the data assets that the operator will validate. The members of the list can be either batches (which means that have data asset identifier, batch kwargs and expectation suite identifier) or a triple that will allow the operator to fetch the batch: (data asset identifier, batch kwargs, expectation suite identifier) using this method: :py:meth:`great_expectations.data_context.ConfigOnlyDataContext.get_batch`
* run_identifier - pipeline run id, a timestamp or any other string that is meaningful to you and will help you refer to the result of this operation later
* validation_operator_name you can instances of a class that implements a Validation Operator

Each operator class is free to define its own object that the `run` method returns. Consult the reference of the specific Validation Operator.


********
Actions
********

A class What happens inside an operator is completely up to the implementor.

In practice a typical operator will invoke actions.

An action is a way to take an arbitrary method and make it configurable and runnable within a data context.

The only requirement from an action is for it to have a take_action method.

GE comes with a list of actions that we consider useful and you can reuse in your pipelines. Most of them take in validation results and do something with it.








