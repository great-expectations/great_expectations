.. _output_format:

================================================================================
Expectation output formats
================================================================================

All Expectations accept an `output_format` parameter. Great Expectations defins four values for `output_format`: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, and `SUMMARY`. The API also allows you to define new formats that mix, match, extend this initial set.

.. code-block:: bash

    >> print list(my_df.my_var)
    ['A', 'B', 'B', 'C', 'C', 'C', 'D', 'D', 'D', 'D', 'E', 'E', 'E', 'E', 'E', 'F', 'F', 'F', 'F', 'F', 'F', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'H', 'H', 'H', 'H', 'H', 'H', 'H', 'H']

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        output_format="BOOLEAN_ONLY"
    )
    False

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        output_format="BASIC"
    )
    {
        'success': False,
        'summary_obj': {
            'exception_count': 6,
            'exception_percent': 0.16666666666666666,
            'exception_percent_nonmissing': 0.16666666666666666,
            'partial_exception_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        output_format="COMPLETE"
    )
    {
        'exception_index_list': [0, 10, 11, 12, 13, 14],
        'exception_list': ['A', 'E', 'E', 'E', 'E', 'E'],
        'success': False
    }

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        output_format="SUMMARY"
    )
    {
        'success': False,
        'summary_obj': {
            'element_count': 36,
            'exception_count': 6,
            'exception_percent': 0.16666666666666666,
            'exception_percent_nonmissing': 0.16666666666666666,
            'missing_count': 0,
            'missing_percent': 0.0,
            'partial_exception_counts': {'A': 1, 'E': 5},
            'partial_exception_index_list': [0, 10, 11, 12, 13, 14],
            'partial_exception_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }

The out-of-the-box default is `output_format=BASIC`.

Note: accepting a single parameter for `output_format` should make the library of formats relatively easy to extend in the future.


Behavior for `BOOLEAN_ONLY` result objects
------------------------------------------------------------------------------
...is simple: if the expectation is satisfied, it returns True. Otherwise it returns False.

.. code-block:: bash

    >> my_df.expect_column_values_to_be_in_set(
        "possible_benefactors",
        ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers"]
        output_format="BOOLEAN_ONLY"
    )
    False

    >> my_df.expect_column_values_to_be_in_set(
        "possible_benefactors",
        ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers", "Mr. Magwitch"]
        output_format="BOOLEAN_ONLY"
    )
    False

Behavior for `BASIC` result objects
------------------------------------------------------------------------------
...depends on the expectation. Great Expectations has native support for three types of Expectations: `column_map_expectation`, `column_aggregate_expectation`, and a base type `expectation`.

`column_map_expectations` apply a boolean test function to each element within a column. The basic format is:

.. code-block:: bash

    {
        "success" : Boolean,
        "summary_obj" : {
            "partial_exception_list" : [A list of up to 20 values that violate the expectation]
            "partial_exception_index_list" : [A list of the indexes of those values]
            "exception_count" : The total count of exceptions in the column
            "exception_percent" : The overall percent of exceptions
            "exception_percent_nonmissing" : The percent of exceptions, excluding mising values from the denominator
        }
    }


Note: when exception values are duplicated, `exception_list` will contain multiple copies of the value.

.. code-block:: bash

    [1,2,2,3,3,3,None,None,None,None]

    expect_column_values_to_be_unique

    {
        "success" : Boolean,
        "summary_obj" : {
            "exception_list" : [2,2,3,3,3]
            "exception_index_list" : [1,2,3,4,5]
            "exception_count" : 5,
            "exception_percent" : 0.5,
            "exception_percent_nonmissing" : 0.8333333,
        }
    }


`column_aggregate_expectations` compute a single value for the column.

Format:

.. code-block:: bash

    {
        "success" : Boolean,
        "true_value" : Depends
    }
    

For example:

.. code-block:: bash

    expect_table_row_count_to_be_between

    {
        "success" : true,
        "true_value" : 7
    }


    expect_column_stdev_to_be_between
    {
        "success" : false
        "true_value" : 3.04
    }

    expect_column_most_common_value_to_be
    {
        "success" : ...
        "true_value" : ...
    }


Behavior for `SUMMARY` result objects
------------------------------------------------------------------------------

`SUMMARY` provides a `summary_obj` with values usef of common exception values. For `column_map_expectations`, the standard format is:

.. code-block:: bash

    {
        'success': False,
        'summary_obj': {
            'element_count': 36,
            'exception_count': 6,
            'exception_percent': 0.16666666666666666,
            'exception_percent_nonmissing': 0.16666666666666666,
            'missing_count': 0,
            'missing_percent': 0.0,
            'partial_exception_counts': {'A': 1, 'E': 5},
            'partial_exception_index_list': [0, 10, 11, 12, 13, 14],
            'partial_exception_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }



For `column_aggregate_expectations`, `SUMMARY` output is the same as `BASIC` output, plus a `summary_obj`.

.. code-block:: bash

    {
        'success': False,
        'true_value': 3.04,
        'summary_obj': {
            'element_count': 77,
            'missing_count': 7,
            'missing_percent': 0.1,
        }
    }


