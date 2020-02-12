.. _profiler:

##############
Profiler
##############

A Profiler builds an Expectation Suite from one or more Data Assets. It usually also validates the data against the
newly-generated Expectation Suite to return a Validation Result.

A Profiler makes it possible to quickly create a starting point for generating expectations about a Dataset. For
example, during the `init` flow, Great Expectations uses the `SampleExpectationsDatasetProfiler` to demonstrate
important features of Expectations by creating and validating an Expectation Suite that has several different kinds of
expectations built from a small sample of data.

You can also extend Profilers to capture organizational knowledge about your data. For example, a team might have a
convention that all columns named "id" are a non-compound primary key, and all columns named "XXXX_id" are foreign
keys. In that case, when the team using Great Expectations first encounters a new dataset that followed the
convention, a profiler that knows that could know to add the expect_column_values_to_be_unique expectation on the
"id" column (but not on the "XXXX_id" column).

*Last updated:* |lastupdate|
