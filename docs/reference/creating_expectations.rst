.. _creating_expectations:


#########################
Creating Expectations
#########################

Creating expectations involves generating and saving an expectation suite. This reference assumes familiarity with
our general philosophy for using expectations outlined in the :ref:`expectations` feature guide.


********************************
Expectation Creation Notebooks
********************************



Saving expectations
=====================

At the end of your exploration, call `save_expectation_suite` to store all Expectations from your session to your
pipeline test files. *By default, when you save an expectation suite, only expectations that succeeded on their last
run are included in the saved suite.*

This is how you always know what to expect from your data.

.. code-block:: bash

    >> my_df.save_expectation_suite("my_titanic_expectations.json")

For more detail on how to control expectation output, please see :ref:`standard_arguments` and :ref:`result_format`.
