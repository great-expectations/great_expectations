.. _encoding_expectations:


Encoding Expectations
=======================

Generating expectations is one of the most important parts of using Great Expectations effectively, and there are
a variety of methods for generating and encoding expectations. When expectations are encoded in the GE format, they
become shareable and persistent sources of truth about how data was expected to behave-and how it actually did.

There are several paths to generating expectations:

1. Automated inspection of datasets. Currently, the profiler mechanism in GE produces expectation suites that can be
   used for validation. In some cases, the goal is :ref:`profiling` your data, and in other cases automated inspection
   can produce expectations that will be used in validating future batches of data.

2. Expertise. Rich experience from Subject Matter Experts, Analysts, and data owners is often a critical source of
   expectations. Interviewing experts and encoding their tacit knowledge of common distributions, values, or failure
   conditions can be can excellent way to generate expectations.

3. Exploratory Analysis. Using GE in an exploratory analysis workflow such as enabled by the ``create_expectations``
   notebook is an important way to develop experience with both raw and derived datasets and generate useful and
   testable expectations about characteristics that may be important for the data's eventual purpose, whether
   reporting or feeding another downstream model or data system.
