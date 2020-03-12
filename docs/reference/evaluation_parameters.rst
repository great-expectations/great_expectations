.. _evaluation_parameters:

######################
Evaluation Parameters
######################

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
