.. _evaluation_parameters:

================================================================================
Evaluation Parameters
================================================================================

Often, the specific parameters associated with an expectation will be derived from upstream steps in a processing \
pipeline. For example, we may want to `expect_table_row_count_to_equal` a value stored in a previous step, but we \
may still want to ensure that we can use the same expectation configuration object.

Great Expectations makes working with parameters of that kind easy! When declaring an expectation, you can specify that \
a particular argument is an evaluation parameter that should be substituted at evaluation time, and provide a temporary \
value that should be used during the initial evaluation of the expectation.

.. code-block:: python

    >> my_df.expect_table_row_count_to_equal(
        value: {"$PARAMETER": "upstream_row_count",
                "$PARAMETER.upstream_row_count": 10}
        result_format={'result_format': 'BOOLEAN_ONLY'}
    )
    {
        'success': True
    }

You can also store parameter values in a special dictionary called evaluation_parameters that is stored in the \
expectation_suite to be available to multiple expectations or while declaring additional expectations.

.. code-block:: python

    >> my_df.set_evaluation_parameter("upstream_row_count", 10)
    >> my_df.get_evaluation_parameter("upstream_row_count)

If a parameter has been stored, then it does not need to be provided for a new expectation to be declared:

.. code-block:: python

    >> my_df.set_evaluation_parameter("upstream_row_count", 10)
    >> my_df.expect_table_row_count_to_be_between(max_value={"$PARAMETER": "upstream_row_count"})

When validating expectations, you can provide evaluation parameters based on upstream results:

.. code-block:: python

    >> my_df.validate(expectation_suite=my_dag_step_config, evaluation_parameters={"upstream_row_count": upstream_row_count})

Finally, the command-line tool also allows you to provide a JSON file that contains parameters to use during evaluation:

.. code-block:: bash

    >> cat my_parameters_file.json
    {
        "upstream_row_count": 10
    }
    >> great_expectations validate --evaluation_parameters=my_parameters_file.json dataset_file.csv expectation_suite.json


.. _data_context_evaluation_parameter_store:

DataContext Evaluation Parameter Store
=======================================

When a DataContext has a configured evaluation parameter store, it can automatically identify and store evaluation
parameters that are referenced in other expectation suites. The evaluation parameter store uses a URN schema for 
identifying dependencies between expectation suites.

The DataContext-recognized URN must begin with the string ``urn:great_expectations:validations``. Valid URNs must have
one of the following structures to be recognized by the Great Expectations DataContext:

::

  urn:great_expectations:validations:<data_asset_name>:expectations:<expectation_name>:columns:<column_name>:result:<result_key>
  urn:great_expectations:validations:<data_asset_name>:expectations:<expectation_name>:columns:<column_name>:details:<details_key>
  urn:great_expectations:validations:<data_asset_name>:expectations:<expectation_name>:result:<result_key>
  urn:great_expectations:validations:<data_asset_name>:expectations:<expectation_name>:details:<details_key>

Replace names in ``<>`` with the desired name. For example:

::

  urn:great_expectations:validations:my_source/default/notable_works_by_charles_dickens:expectations:expect_column_proportion_of_unique_values_to_be_between:columns:Title:result:observed_value
