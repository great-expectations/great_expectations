---
title: Validation Operator
id: validation_operator
hoverText: An internal object that can trigger Validation Actions after Validation completes.
---

A Validation Operator is an internal object that can trigger Validation Actions after Validation completes.


NOTES: TEMPORARY
-------------
(Only used internally in V3) Executes Actions that are configured to trigger when Validation completes. There are only two types of validation operators: ActionListValidationOperator that contains a list of Actions, and WarningAndFailureExpectationSuitesValidationOperator that allows you to specify suites for warnings and failures. I think this has been replaced with Checkpoint (that likewise references a list of Actions) but the configuration and use of Validation Operator is still well documented in many How To guides.