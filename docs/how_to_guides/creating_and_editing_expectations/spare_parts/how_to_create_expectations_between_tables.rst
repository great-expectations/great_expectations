.. _tutorial_create_expectations_between_tables:

How to create Expectations between tables
==========================================

This tutorial covers the workflow of creating and editing :ref:`Expectations`_ that relate to data stored in different tables or datasets.

Unfortunately, no Expectations natively support cross-table comparisons today (but we hope to in the future!). Consequently, there are two available paths:

1. Use "check assets", where you create a new table that joins the two tables; or
2. Use :ref:`Evaluation Parameters`_ to supply relevant metrics to expectations.

Check asset Pattern
-------------------------

See :ref:`how_to__use_check_assets` for more information on the check asset pattern.

Use Evaluation Parameters
-------------------------

To compare two tables using evaluation parameters, you create Expectations for each table, and reference a property from one :ref:`Validation Result <Validation Results>`_ in the other. To use Evaluation Parameters, both assets need to be validated during the same run.

.. code-block:: python

    table_1 = context.get_batch(table_1_batch_kwargs, expectation_suite_name='table_1.warning')
    table_2 = context.get_batch(table_2_batch_kwargs, expectation_suite_name='table_2.warning')
    # Create an expectation that will always pass, but will produce a metric corresponding to the true observed value
    table_1.expect_column_unique_value_count_to_be_between('id', min_value=0, max_value=None)

    # Reference the value from the first
    table_2.expect_table_row_count_to_equal(value={
        "$PARAMETER": "urn:great_expectations:validations:table_1.warning:expect_column_unique_value_count_to_be_between.result.observed_value:col=id"
    })


    # Now, validation of both assets within the same run will support a form of cross-table comparison
    results = context.run_validation_operator("action_list_operator", assets_to_validate=[table_1, table_2])
