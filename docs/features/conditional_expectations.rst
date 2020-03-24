.. _conditional_expectations:

########################
Conditional Expectations
########################

Great Expectations offers an extension for the standard Expectations, namely, the Conditional Expectations. The main
difference is the :code:`condition` argument, which is passed to all Dataset Expectations in the :ref:`expectation_glossary`.

Conditional Expectations can be used to narrow the scope of data where expectation is applied, e.g. you can expect
values in particular column to not be null, but only if other column or columns take on certain values.

*************************************
How to build conditional expectations
*************************************

So far Conditional Expectations are only implemented for Pandas backend, thus, the feature does not work with Spark and
SQLAlchemy platforms yet. For Pandas, the value passed to the :code:`condition` argument should be a valid boolean
expression string, which is then forwarded to :code:`pandas.DataFrame.query()` before Expectation Validation (see `pandas docs <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html>`_).

For example, you can test if depending columns in your data are consistent (e.g. encodings):

.. code-block:: bash

    >>> import great_expectations as ge
    >>> my_df = ge.read_csv("./tests/test_sets/Titanic.csv")
    >>> my_df.expect_column_values_to_be_in_set(
            column='Sex',
            value_set=['male'],
            condition='SexCode==0'
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

This feature makes it also possible to add two Expectations of the same type and for the same column, whereby one of
them is conditional and the other one is not. They will be stored in Expectation Suite as two different Expectations.

.. code-block:: bash

    >>> my_df.expect_column_values_to_be_in_set(
            column='Survived',
            value_set=[0, 1]
        )
    >>> my_df.expect_column_values_to_be_in_set(
            column='Survived',
            value_set=[1],
            condition='PClass=="1st"'
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
                    "condition": "PClass==\"1st\""
                },
                "expectation_type": "expect_column_values_to_be_in_set"
            }
        ],
        "data_asset_type": "Dataset"
    }


*********
Data Docs
*********

Conditional Expectations are also shown differently from standard Expectations in Data Docs. Each Conditional Expectation
has the following appearance: *if 'condition_string', then values must be...*

.. image:: ../images/conditional_data_docs_screenshot.png

If *'condition_string'* is a complex expression, it will be divided into several chunks to enhance readability.

The following Expectations are not meant to be shown as conditional ones, since it is not reasonable and not intended
to use conditions with them:

* :func:`expect_column_to_exist <great_expectations.dataset.dataset.Dataset.expect_column_to_exist>`
* :func:`expect_table_columns_to_match_ordered_list <great_expectations.dataset.dataset.Dataset.expect_table_columns_to_match_ordered_list>`
* :func:`expect_table_column_count_to_be_between <great_expectations.dataset.dataset.Dataset.expect_table_column_count_to_be_between>`
* :func:`expect_table_column_count_to_equal <great_expectations.dataset.dataset.Dataset.expect_table_column_count_to_equal>`

You can find further information in the :ref:`data_docs` feature guide.
