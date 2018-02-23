.. _custom_expectations:

==============================================================================
Custom expectations
==============================================================================

It's common to want to extend Great Expectations with application- or domain-specific Expectations. For example:

.. code-block:: bash

    expect_column_text_to_be_in_english
    expect_column_value_to_be_valid_medical_diagnosis_code
    expect_column_value_to_be_be_unicode_encodable

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Side note: in future versions, Great Expectations will probably grow to include additional Expectations. If you have an Expectation that could be universally useful, please make the case on the `Great Expectations issue tracker on github <https://github.com/great-expectations/great_expectations/issues>`_.

The easy way
--------------------------------------------------------------------------------

1. Create a subclass from the DataSet class of your choice
2. Define custom functions containing your business logic
3. Use the `@column_map_expectation` and `@column_aggregate_expectation` decorators to turn them into full Expectations

Note: following Great Expectations :ref:`naming_conventions` is highly recommended, but not strictly required. If you want to confuse yourself with bad names, the package won't stop you.

.. code-block:: bash

    from great_expectations.dataset import PandasDataSet, MetaPandasDataSet

    class CustomPandasDataSet(PandasDataSet):

        @MetaPandasDataSet.column_map_expectation
        def expect_column_values_to_equal_2(self, series):
            return series.map(lambda x: x==2)

        @MetaPandasDataSet.column_aggregate_expectation
        def expect_column_mode_to_equal_0(self, series):
            mode = series.mode[0]
            return {
                "success" : mode == 0,
                "true_value" : mode,
                "summary_obj" : {}
            }

`@column_map_expectation` decorates a custom function, wrapping it with all the business logic required to turn it into a fully-fledged Expectation. This spares you the hassle of defining logic to handle required arguments like `mostly` and `output_format`. Your custom function can focus exclusively on the business logic of passing or failing the expectation.

To work with these decorators, your custom function must accept two arguments: `self` and `series`. When your function is called, `series` will contain all the non-null values in the given column. Your function must return a series of boolean values in the same order, with the same index.

`@column_aggregate_expectation` accepts `self` and `series`. It must return a dictionary containing a boolean `success` value, and a `true_value` argument.


The hard way
--------------------------------------------------------------------------------

1. Create a subclass from the DataSet class of your choice
2. Write the whole expectation yourself
3. Decorate it with the `@expectation` decorator

This is more complicated, since you have to handle all the logic of additional parameters and output formats. Pay special attention to proper formatting of :ref:`output_format`. Malformed result objects can break Great Expectations in subtle and unanticipated ways.

.. code-block:: bash

    from great_expectations.dataset import PandasDataSet, expectation

    class CustomPandasDataSet(ge.dataset.PandasDataSet):

        @expectation
        def expect_column_values_to_equal_1(self, column, mostly=None, suppress_expectations=False):
            notnull = self[column].notnull()
            
            result = self[column][notnull] == 1
            exceptions = list(self[column][notnull][result==False])
            
            if mostly:
                #Prevent division-by-zero errors
                if len(not_null_values) == 0:
                    return {
                        'success':True,
                        'exception_list':exceptions
                    }

                percent_properly_formatted = float(sum(properly_formatted))/len(not_null_values)
                return {
                    "success" : percent_properly_formatted >= mostly,
                    "exception_list" : exceptions
                }
            else:
                return {
                    "success" : len(exceptions) == 0,
                    "exception_list" : exceptions
                }

The quick way
--------------------------------------------------------------------------------

For rapid prototyping, you can use the following syntax to quickly iterate on the logic for expectations.

.. code-block:: bash

    >> dataset.test_expectation_function(my_func)
    
    >> dataset.test_column_map_expectation_function(my_map_func, column='my_column')
    
    >> dataset.test_column_aggregate_expectation_function(my_agg_func, column='my_column')

These functions will return output just like regular expectations. However, they will NOT save a copy of the expectation to the config.


Using custom expectations
--------------------------------------------------------------------------------

Let's suppose you've defined `CustomPandasDataSet` in a module called `custom_dataset.py`. You can instantiate a DataSet with your custom expectations simply by adding `dataset_class=CustomPandasDataSet` in `ge.read_csv`.

Once you do this, all the functionality of your new expectations will be available for uses.

.. code-block:: bash

    >> import great_expectations as ge
    >> from custom_dataset import CustomPandasDataSet

    >> my_df = ge.read_csv("my_data_file.csv", dataset_class=CustomPandasDataSet)

    >> my_df.expect_column_values_to_equal_1("all_twos")
    {
        "success": False,
        "exception_list": [2,2,2,2,2,2,2,2]
    }

A similar approach works for the command-line tool.

.. code-block:: bash

    >> great_expectations validate \
        my_data_file.csv \
        my_expectations.json \
        dataset_class=custom_dataset.CustomPandasDataSet



