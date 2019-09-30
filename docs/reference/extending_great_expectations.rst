.. _extending_great_expectations:

##############################
Extending Great Expectations
##############################

It is possible to extend behavior in great expectations in several ways, described below.

********************
Additional backends
********************

Expectations require backend implementations to translate the semantically-meaningful expectation to the actual
metrics and compute tasks required to evaluate it.

Inheriting Documentation
=========================

Expectations should maintain the same API and behavior independently of the backend that implements them.
When implementing an expectation defined in the base `Dataset` for a new backend, we recommend that you add the
`@DocInherit` decorator first to use the default dataset documentation for the expectation. That can help users of
your dataset see consistent documentation no matter which backend is implementing the great_expectations API.

`@DocInherit` overrides your function's __get__ method with one that will replace the local docstring with the
docstring from its parent. It is defined in `Dataset.util`.
