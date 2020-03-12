.. _evaluation_parameters:

######################
Evaluation Parameters
######################

Several cases where we want to define expectations using either dynamic values or values only available at validation
time. For example:

1. We expect that the number of rows in a computed aggregate table (or DAG node) to equal the number of unique values
in an event table / upstream DAG node.
2. We expect that the number of rows in one table should be within 10% of the number of rows in another table.
3. We expect that the values in a column should be taken from a set stored in a database updated by a different system.



Proposed spec:

1. Exists today (see below):
>>> providers_by_state.expect_table_row_count_to_equal(value={"$PARAMETER":
"urn:great_expectations:validations:state_data:expect_column_proportion_of_unique_values_to_be_between.result
.observed_value:column=name"})

2. Proposed API:
>>> providers_by_state.expect_table_row_count_to_be_between(min_value={"$PARAMETER_EXPRESSION":
"0.9 * urn:great_expectations:validations:state_data:expect_column_proportion_of_unique_values_to_be_between.result
.observed_value:column=name"},
max_value={"$PARAMETER_EXPRESSION":
"1.1 * urn:great_expectations:validations:state_data:expect_column_proportion_of_unique_values_to_be_between.result
.observed_value:column=name"})


3. Proposed API:
>>> states.expect_column_distinct_values_to_equal_set(value_set={"$PARAMETER_EXPRESSION":
"urn:great_expectations:stores:remote_metric_store:states:column=name"})

WHERE
-> "states" is the name of a KEY/METRIC exposed by the remote_metric_store (see `metric_store.py`)
-> "column=name" are the metric kwargs in string form

Then, the "remote_metric_store" provides a `set_remote_metric` or similar method that allows one to specify a method
for the store to retrieve a metric (method TBD).





------------------------
OLD LANGUAGE
------------------------

Often, the specific parameters associated with an expectation will be derived from upstream steps in a processing
pipeline. For example, we may want to `expect_table_row_count_to_equal` a value stored in a previous step.

Great Expectations makes it possible to use "Evaluation Parameters" to accomplish that goal. We declare Expectations
using parameters that need to be provided at validation time; during interactive development, we can even provide a
temporary value that should be used during the initial evaluation of the expectation.

>>> my_df.expect_table_row_count_to_equal(
...    value={"$PARAMETER": "upstream_row_count", "$PARAMETER.upstream_row_count": 10},
...    result_format={'result_format': 'BOOLEAN_ONLY'})
{
  'success': True
}

You can also store parameter values in a special dictionary called evaluation_parameters that is stored in the \
expectation_suite to be available to multiple expectations or while declaring additional expectations.

>>> my_df.set_evaluation_parameter("upstream_row_count", 10)
>>> my_df.get_evaluation_parameter("upstream_row_count")

If a parameter has been stored, then it does not need to be provided for a new expectation to be declared:

>>> my_df.set_evaluation_parameter("upstream_row_count", 10)
>>> my_df.expect_table_row_count_to_be_between(max_value={"$PARAMETER": "upstream_row_count"})

When validating expectations, you can provide evaluation parameters based on upstream results:

>>> my_df.validate(expectation_suite=my_dag_step_config, evaluation_parameters={"upstream_row_count":upstream_row_count})

.. _data_context_evaluation_parameter_store:

***************************************
DataContext Evaluation Parameter Store
***************************************

When a DataContext has a configured evaluation parameter store, it can automatically identify and store evaluation
parameters that are referenced in other expectation suites. The evaluation parameter store uses a URN schema for
identifying dependencies between expectation suites.

The DataContext-recognized URN must begin with the string ``urn:great_expectations:validations``. Valid URNs must have
one of the following structures to be recognized by the Great Expectations DataContext:

::

  urn:great_expectations:validations:<expectation_suite_name>:<metric_name>
  urn:great_expectations:validations:<expectation_suite_name>:<metric_name>:<metric_kwargs_id>

Replace names in ``<>`` with the desired name. For example:

::

  urn:great_expectations:validations:dickens_data:expect_column_proportion_of_unique_values_to_be_between.result.observed_value:column=Title

*last updated*: |lastupdate|
