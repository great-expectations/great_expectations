.. _custom_expectations_feature:

####################################
Custom expectations
####################################

Expectations are especially useful when they capture critical aspects of data understanding that analysts and
practitioners know based on its *semantic* meaning. It's common to want to extend Great Expectations with application-
or domain-specific Expectations. For example:

.. code-block:: bash

    expect_column_text_to_be_in_english
    expect_column_value_to_be_valid_icd_code

These Expectations aren't included in the default set, but could be very useful for specific applications.

Fear not! Great Expectations is designed for customization and extensibility.

Building custom expectations is easy and allows your custom logic to become part of the validation, documentation, and
even profiling workflows that make Great Expectations stand out. See the guide on :ref:`custom_expectations_reference`
for more information on building expectations and updating DataContext configurations to automatically load batches
of data with custom Data Assets.


