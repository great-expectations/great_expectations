.. _profilers:

#########
Profilers
#########

Great Expectations provides a mechanism to automatically generate expectations, using a feature called a `Profiler`. A
Profiler builds an Expectation Suite from one or more Data Assets. It usually also validates the data against the
newly-generated Expectation Suite to return a Validation Result. There are several Profilers included with Great
Expectations.

A Profiler makes it possible to quickly create a starting point for generating expectations about a Dataset. For
example, during the ``init`` flow, Great Expectations uses the ``UserConfigurableProfiler`` to demonstrate
important features of Expectations by creating and validating an Expectation Suite that has several different kinds of
expectations built from a small sample of data. A Profiler is also critical to generating the Expectation Suites used
during :ref:`profiling`.

You can also extend Profilers to capture organizational knowledge about your data. For example, a team might have a
convention that all columns **named** "id" are primary keys, whereas all columns ending with the
**suffix** "_id" are foreign keys. In that case, when the team using Great Expectations first encounters a new
dataset that followed the convention, a Profiler could use that knowledge to add an expect_column_values_to_be_unique
Expectation to the "id" column (but not, for example an "address_id" column).
