---
title: Action 
id: action 
hoverText: A Python class with a run method that takes the result of validating a Batch against an Expectation Suite and does something with it
---

A Validation Action is a Python class with a run method that takes the result of validating a Batch against an Expectation Suite and does something with it.




## NOTES (Temporary)

## Validation Actions

Actions are Python classes with a `run` method that takes the result of validating a Batch against an Expectation Suite
and does something with it (e.g., save Validation Results to disk, or send a Slack notification). Classes that implement
this API can be configured to be added to the list of actions used by a particular Checkpoint.

Classes that implement Actions can be found in the `great_expectations.checkpoint.actions` module.

