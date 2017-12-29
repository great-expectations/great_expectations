.. _output_format:

================================================================================
Expectation result object formats
================================================================================

All Expectations accept a `result_obj_format` parameter, which controls what information is returned from the \
evaluation of an expectation. The value of `result_obj_format` does not affect the other configurations affecting \
return values such as `include_config` and `catch_excpetions`.

Great Expectations defines four values for `result_obj_format`: `NONE`, `BASIC`, `SUMMARY`, and `COMPLETE`. \
Each value supports a different use case for working with Great Expectations, including interactive exploratory work \
and automatic validation.

+---------------------------------------+--------------------------------------------------------------+
| `result_obj_format` Setting           | Primary use case                                             |
+=======================================+==============================================================+
|    NONE                               | Automatic validation. No result_obj is returned.             |
+---------------------------------------+--------------------------------------------------------------+
|    BASIC                              | Exploratory analysis in a notebook.                          |
+---------------------------------------+--------------------------------------------------------------+
|    SUMMARY                            | Detailed exploratory work with follow-on investigation.      |
+---------------------------------------+--------------------------------------------------------------+
|    COMPLETE                           | Debugging pipelines or developing detailed regression tests. |
+---------------------------------------+--------------------------------------------------------------+


result_obj_format examples
------------------------------------------------------------------------------

.. code-block:: bash

    >> print list(my_df.my_var)
    ['A', 'B', 'B', 'C', 'C', 'C', 'D', 'D', 'D', 'D', 'E', 'E', 'E', 'E', 'E', 'F', 'F', 'F', 'F', 'F', 'F', 'G', 'G', 'G', 'G', 'G', 'G', 'G', 'H', 'H', 'H', 'H', 'H', 'H', 'H', 'H']

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        result_obj_format="NONE"
    )
    {
        'success': False
    }

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        result_obj_format="BASIC"
    )
    {
        'success': False,
        'result_obj': {
            'unexpected_count': 6,
            'unexpected_percent': 0.16666666666666666,
            'unexpected_percent_nonmissing': 0.16666666666666666,
            'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }

    >> expect_column_values_to_match_regex(
        "my_column",
        "[A-Z][a-z]+",
        result_obj_format="SUMMARY"
    )
    {
        'success': False,
        'result_obj': {
            'element_count': 36,
            'unexpected_count': 6,
            'unexpected_percent': 0.16666666666666666,
            'unexpected_percent_nonmissing': 0.16666666666666666,
            'missing_count': 0,
            'missing_percent': 0.0,
            'partial_unexpected_counts': {'A': 1, 'E': 5},
            'partial_unexpected_index_list': [0, 10, 11, 12, 13, 14],
            'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }

    >> my_df.expect_column_values_to_be_in_set(
        "my_var",
        ["B", "C", "D", "F", "G", "H"],
        result_obj_format="COMPLETE"
    )
    {
        'success': False,
        'result_obj': {
            'unexpected_index_list': [0, 10, 11, 12, 13, 14],
            'unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }



The out-of-the-box default is `result_obj_format=BASIC`.


Behavior for `NONE`
------------------------------------------------------------------------------
When the `result_obj_format` is `NONE`, no `result_obj` is returned. The result of evaluating the expectation is \
exclusively returned via the value of the `success` parameter.

For example:

.. code-block:: bash

    >> my_df.expect_column_values_to_be_in_set(
        "possible_benefactors",
        ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers"]
        result_obj_format="BOOLEAN_ONLY"
    )
    {
        'success': False
    }

    >> my_df.expect_column_values_to_be_in_set(
        "possible_benefactors",
        ["Joe Gargery", "Mrs. Gargery", "Mr. Pumblechook", "Ms. Havisham", "Mr. Jaggers", "Mr. Magwitch"]
        result_obj_format="BOOLEAN_ONLY"
    )
    {
        'success': False
    }


Behavior for `BASIC`
------------------------------------------------------------------------------
A `result_obj` is generated with a basic justification for why an expectation was met or not. The format is intended \
for quick, at-a-glance feedback. For example, it tends to work well in jupyter notebooks.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of \
unexpected values to justify the expectation result.


The basic `result_obj` includes:

.. code-block:: bash

    {
        "success" : Boolean,
        "result_obj" : {
            "partial_unexpected_list" : [A list of up to 20 values that violate the expectation]
            "unexpected_count" : The total count of unexpected values in the column
            "unexpected_percent" : The overall percent of unexpected values
            "unexpected_percent_nonmissing" : The percent of unexpected values, excluding missing values from the denominator
        }
    }

Note: when unexpected values are duplicated, `unexpected_list` will contain multiple copies of the value.

.. code-block:: bash

    [1,2,2,3,3,3,None,None,None,None]

    expect_column_values_to_be_unique

    {
        "success" : Boolean,
        "result_obj" : {
            "partial_unexpected_list" : [2,2,3,3,3]
            "unexpected_count" : 5,
            "unexpected_percent" : 0.5,
            "unexpected_percent_nonmissing" : 0.8333333
        }
    }


`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a single `true_value` \
to justify the expectation result.

The basic `result_obj` includes:

.. code-block:: bash


    {
        "success" : Boolean,
        "result_obj" : {
            "true_value" : The aggregate statistic computed for the column
        }
    }
    
For example:

.. code-block:: bash

    [1, 1, 2, 2]

    expect_column_mean_to_be_between

    {
        "success" : Boolean,
        "result_obj" : {
            "true_value" : 1.5
        }
    }


Behavior for `SUMMARY`
------------------------------------------------------------------------------
A `result_obj` is generated with a summary justification for why an expectation was met or not. The format is intended \
for more detailed exploratory work and includes additional information beyond what is included by `BASIC`.
For example, it can support generating dashboard results of whether a set of expectations are being met.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of \
unexpected values to justify the expectation result.

The summary `result_obj` includes:

.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            'element_count': The total number of values in the column
            'unexpected_count': The total count of unexpected values in the column (also in `BASIC`)
            'unexpected_percent': The overall percent of unexpected values (also in `BASIC`)
            'unexpected_percent_nonmissing': The percent of unexpected values, excluding missing values from the denominator (also in `BASIC`)
            "partial_unexpected_list" : [A list of up to 20 values that violate the expectation] (also in `BASIC`)
            'missing_count': The number of missing values in the column
            'missing_percent': The total percent of missing values in the column
            'partial_unexpected_counts': {A dictionary of the number of times each of the unexpected values occurs}
            'partial_unexpected_index_list': [A list of up to 20 of the indices of the unexpected values in the column]
        }
    }

For example:

.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            'element_count': 36,
            'unexpected_count': 6,
            'unexpected_percent': 0.16666666666666666,
            'unexpected_percent_nonmissing': 0.16666666666666666,
            'missing_count': 0,
            'missing_percent': 0.0,
            'partial_unexpected_counts': {'A': 1, 'E': 5},
            'partial_unexpected_index_list': [0, 10, 11, 12, 13, 14],
            'partial_unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }


`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a `true_value` \
to justify the expectation result. It also includes additional information regarding observed values and counts, \
depending on the specific expectation.


The summary `result_obj` includes:


.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            'true_value': The aggregate statistic computed for the column (also in `BASIC`)
            'element_count': The total number of values in the column
            'missing_count':  The number of missing values in the column
            'missing_percent': The total percent of missing values in the column
            <expectation-specific result justification fields>
        }
    }

For example:

.. code-block:: bash

    [1, 1, 2, 2, NaN]

    expect_column_mean_to_be_between

    {
        "success" : Boolean,
        "result_obj" : {
            "true_value" : 1.5,
            'element_count': 5,
            'missing_count: 1,
            'missing_percent: 0.2
        }
    }


Behavior for `COMPLETE`
------------------------------------------------------------------------------
A `result_obj` is generated with all available justification for why an expectation was met or not. The format is \
intended for debugging pipelines or developing detailed regression tests.

Great Expectations has standard behavior for support for describing the results of `column_map_expectation` and
`column_aggregate_expectation` expectations.

`column_map_expectation` applies a boolean test function to each element within a column, and so returns a list of \
unexpected values to justify the expectation result.

The complete `result_obj` includes:

.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            "unexpected_list" : [A list of all values that violate the expectation]
            'unexpected_index_list': [A list of the indices of the unexpected values in the column]
            'element_count': The total number of values in the column (also in `SUMMARY`)
            'unexpected_count': The total count of unexpected values in the column (also in `SUMMARY`)
            'unexpected_percent': The overall percent of unexpected values (also in `SUMMARY`)
            'unexpected_percent_nonmissing': The percent of unexpected values, excluding missing values from the denominator (also in `SUMMARY`)
            'missing_count': The number of missing values in the column  (also in `SUMMARY`)
            'missing_percent': The total percent of missing values in the column  (also in `SUMMARY`)
        }
    }

For example:

.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            'element_count': 36,
            'unexpected_count': 6,
            'unexpected_percent': 0.16666666666666666,
            'unexpected_percent_nonmissing': 0.16666666666666666,
            'missing_count': 0,
            'missing_percent': 0.0,
            'unexpected_index_list': [0, 10, 11, 12, 13, 14],
            'unexpected_list': ['A', 'E', 'E', 'E', 'E', 'E']
        }
    }


`column_aggregate_expectation` computes a single aggregate value for the column, and so returns a `true_value` \
to justify the expectation result. It also includes additional information regarding observed values and counts, \
depending on the specific expectation.


The complete `result_obj` includes:


.. code-block:: bash

    {
        'success': False,
        'result_obj': {
            'true_value': The aggregate statistic computed for the column (also in `SUMMARY`)
            'element_count': The total number of values in the column (also in `SUMMARY`)
            'missing_count':  The number of missing values in the column (also in `SUMMARY`)
            'missing_percent': The total percent of missing values in the column (also in `SUMMARY`)
            <expectation-specific result justification fields, which may be more detailed than in `SUMMARY`>
        }
    }

For example:

.. code-block:: bash

    [1, 1, 2, 2, NaN]

    expect_column_mean_to_be_between

    {
        "success" : Boolean,
        "result_obj" : {
            "true_value" : 1.5,
            'element_count': 5,
            'missing_count: 1,
            'missing_percent: 0.2
        }
    }



Result_obj Output Format Reference
-------------------------------------------------------------------------------

+---------------------------------------+----------------------+------------------------+----------------+
|Fields within `result_obj `            |BASIC                 |SUMMARY                 |COMPLETE        |
+=======================================+======================+========================+================+
|    unexpected_index_list*             |no                    |no                      |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    unexpected_list*                   |no                    |no                      |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    partial_unexpected_list*           |yes                   |yes                     |no              |
+---------------------------------------+----------------------+------------------------+----------------+
|    partial_unexpected_index_list*     |no                    |yes                     |no              |
+---------------------------------------+----------------------+------------------------+----------------+
|    partial_unexpected_counts*         |no                    |yes                     |no              |
+---------------------------------------+----------------------+------------------------+----------------+
|    true_value+                        |yes                   |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    unexpected_count                   |yes                   |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    unexpected_percent                 |yes                   |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    unexpected_percent_nonmissing      |yes                   |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    element_count                      |no                    |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    missing_count                      |no                    |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    missing_percent                    |no                    |yes                     |yes             |
+---------------------------------------+----------------------+------------------------+----------------+
|    Other...                           |Defined on per-expectation basis                                |
+---------------------------------------+----------------------+------------------------+----------------+

* : These variables are only defined for `column_map_expectation` type expectations.
+ : These variables are only defined for `column_aggregate_expectation` type expectations.



Top-level return objectReference
-------------------------------------------------------------------------------
After evaluating an expectation, Great Expectations will return the following fields:
 - success (boolean): always
 - result_obj: Included if result_obj_output_format is not `NONE`
 - expectation_config: Included if and only if include_config=True
 - raised_exception (boolean): Included if and only if catch_exceptions=True
 - exceptions: Included if and only if catch_exceptions=True
 - exception_traceback (string or None) Included if and only if catch_exceptions=True
