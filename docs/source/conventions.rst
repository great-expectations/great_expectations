.. _conventions:

================================================================================
Naming conventions
================================================================================

`expect_*_to_*`


================================================================================
Extending Great Expectations
================================================================================

When implementing an expectation defined in the base DataSet for a new backend, add the ``@DocInherit` decorator first to use the default DataSet documentation for the expectation. That can help users of your DataSet see consistent documentation no matter which backend is implementing the great_expectations API.

``@DocInherit` overrides your function's __get__ method with one that will replace the local docstring with the docstring from its parent. It is defined in `dataset.util`.
