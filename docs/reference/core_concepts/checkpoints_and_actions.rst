.. _checkpoints_and_actions:

#############################################
Checkpoints And Actions Introduction
#############################################

.. attention::

  As part of the new modular expectations API in Great Expectations, Validation Operators are evolving into Checkpoints.
  At some point in the future Validation Operators will be fully deprecated.

The `validate` method evaluates one batch of data against one expectation suite and returns a dictionary of validation results. This is sufficient when you explore your data and get to know Great Expectations.
When deploying Great Expectations in a real data pipeline, you will typically discover additional needs:

* Validating a group of batches that are logically related (for example, a Checkpoint for all staging tables).
* Validating a batch against several expectation suites (for example, run three suites to protect a machine learning model `churn.critical`, `churn.warning`, `churn.drift`).
* Doing something with the validation results (for example, saving them for later review, sending notifications in case of failures, etc.).

Checkpoints provide a convenient abstraction for both bundling the validation of multiple expectation suites and the actions that should be taken after the validation.

***************************************************
Finding a Checkpoint that is right for you
***************************************************

Each Checkpoint encodes a particular set of business rules around validation.
Currently, two Checkpoint implementations are included in the Great Expectations distribution.

The classes that implement them are in :py:mod:`great_expectations.checkpoint` module.

***TODO this link probably needs to change***
.. _action_list_validation_operator:

***TODO this section is very rough and needs work***
SimpleCheckpoint
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SimpleCheckpoint validates each batch in the list that is passed as `assets_to_validate` argument to its `run` method against the expectation suite included within that batch and then invokes a list of configured actions on every validation result.

Actions are Python classes with a `run` method that takes a result of validating a batch against an expectation suite and does something with it (e.g., save validation results to the disk or send a Slack notification).
Classes that implement this API can be configured to be added to the list of actions for this operator (and other operators that use actions).
Read more about actions here: :py:class:`great_expectations.validation_operators.actions.ValidationAction`.

A user can choose the actions to perform in their instance of the operator.

***TODO this link probably needs to change***
Read more about SimpleCheckpoint here: :py:class:`great_expectations.validation_operators.ActionListValidationOperator`.

.. _validation_actions:

*****************
Checkpoint
*****************

***TODO write this***