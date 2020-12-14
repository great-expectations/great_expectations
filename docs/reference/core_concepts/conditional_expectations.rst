.. _conditional_expectations:

########################
Conditional Expectations
########################

Sometimes one may hold an Expectation not for a dataset in its entirety but only for a particular subset. Alternatively, what one expects of some variable may depend on the value of another.
One may, for example, expect a column that holds the country of origin to not be null only for people of foreign descent.

Great Expectations allows you to express such Conditional Expectations via a :code:`row_condition` argument that can be passed to all Dataset Expectations.

Today, conditional Expectations are available only for the Pandas but not for the Spark and SQLAlchemy backends. The feature is **experimental**. Please expect changes to API as additional backends are supported.

For Pandas, the :code:`row_condition` argument should be a boolean
expression string, which can be passed to :code:`pandas.DataFrame.query()` before Expectation Validation (see `pandas docs <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html>`_).

Additionally, the :code:`condition_parser` argument must be provided, which defines the syntax of conditions.
Since the feature is **experimental** and only available for Pandas this argument must be set to *"pandas"* by default, thus, demanding the appropriate syntax. Other engines might be implemented in future.

The feature can be used, e.g., to test if different encodings of identical pieces of information are consistent with each other:

.. code-block:: bash

    >>> import great_expectations as ge
    >>> my_df = ge.read_csv("./tests/test_sets/Titanic.csv")
    >>> my_df.expect_column_values_to_be_in_set(
            column='Sex',
            value_set=['male'],
            row_condition='SexCode==0'
        )
    {
        "success": true,
        "result": {
            "element_count": 851,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "unexpected_percent_nonmissing": 0.0,
            "partial_unexpected_list": []
        }
    }

It is also possible to add multiple Expectations of the same type to the Expectation Suite for a single column. At most one Expectation can be unconditional while an arbitrary number of Expectations -- each with a different condition -- can be conditional.

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_in_set(
            column='Survived',
            value_set=[0, 1]
        )
    >>> my_df.expect_column_values_to_be_in_set(
            column='Survived',
            value_set=[1],
            row_condition='PClass=="1st"'
        )
    >>> my_df.get_expectation_suite(discard_failed_expectations=False)  # The second one is failing.
    {
        "expectation_suite_name": "default",
        "expectations": [
            {
                "meta": {},
                "kwargs": {
                    "column": "Survived",
                    "value_set": [0, 1]
                },
                "expectation_type": "expect_column_values_to_be_in_set"
            },
            {
                "meta": {},
                "kwargs": {
                    "column": "Survived",
                    "value_set": [1],
                    "row_condition": "PClass==\"1st\"",
                    "condition_parser": "pandas"
                },
                "expectation_type": "expect_column_values_to_be_in_set"
            }
        ],
        "data_asset_type": "Dataset"
    }


*********
Data Docs
*********

Conditional Expectations are displayed differently from standard Expectations in the Data Docs. Each Conditional Expectation is qualified with *if 'row_condition_string', then values must be...*

.. image:: /images/conditional_data_docs_screenshot.png

If *'row_condition_string'* is a complex expression, it will be split into several components for better readability.


*********************
Scope and Limitations
*********************

While conditions can be attached to most Expectations, the following Expectations cannot be conditioned by their very nature and therefore do not take the :code:`row_condition` argument:

* :func:`expect_column_to_exist <great_expectations.dataset.dataset.Dataset.expect_column_to_exist>`
* :func:`expect_table_columns_to_match_ordered_list <great_expectations.dataset.dataset.Dataset.expect_table_columns_to_match_ordered_list>`
* :func:`expect_table_column_count_to_be_between <great_expectations.dataset.dataset.Dataset.expect_table_column_count_to_be_between>`
* :func:`expect_table_column_count_to_equal <great_expectations.dataset.dataset.Dataset.expect_table_column_count_to_equal>`

For more information, see the :ref:`data_docs` feature guide.
