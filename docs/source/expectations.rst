.. _expectations:

================================================================================
Expectations
================================================================================

Expectations are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code.

Connect-and-expect
------------------------------------------------------------------------------

Great Expectations's connect-and-expect API makes it easy to declare Expectations within the tools you already use for data exploration: jupyter notebooks, the ipython console, scratch scripts, etc.

.. code-block:: bash

    >> import great_expectations as ge
    >> my_df = ge.read_csv("./tests/examples/titanic.csv")

    >> my_df.expect_column_values_to_be_in_set(
        "Sex",
        ["male", "female"]
    )
    {
        'success': True,
        'summary_obj': {
            'exception_count': 0,
            'exception_percent': 0.0,
            'exception_percent_nonmissing': 0.0,
            'partial_exception_list': []
        }
    }



Instant feedback
------------------------------------------------------------------------------

When you invoke an Expectation method from a notebook or console, it will immediately return a dictionary containing the result and a list of exceptions.

For example:

.. code-block:: bash

    >> print my_df.PClass.value_counts()
    3rd    711
    1st    322
    2nd    279
    *        1
    Name: PClass, dtype: int64

    >> my_df.expect_column_values_to_be_in_set(
        "PClass",
        ["1st", "2nd", "3rd"]
    )
    {
        'success': False,
        'summary_obj': {
            'exception_count': 1,
            'exception_percent': 0.0007616146230007616,
            'exception_percent_nonmissing': 0.0007616146230007616,
            'partial_exception_list': ['*']
        }
    }

Another example:

.. code-block:: bash

    >> my_df.expect_column_values_to_match_regex(
        "Name",
        "^[A-Za-z\, \(\)\']+$"
    )
    {
        'success': False,
        'summary_obj': {
            'exception_count': 16,
            'exception_percent': 0.012185833968012186,
            'exception_percent_nonmissing': 0.012185833968012186,
            'partial_exception_list': [
                'Bjornstrm-Steffansson, Mr Mauritz Hakan',
                'Brown, Mrs James Joseph (Margaret Molly" Tobin)"',
                'Frolicher-Stehli, Mr Maxmillian',
                'Frolicher-Stehli, Mrs Maxmillian (Margaretha Emerentia Stehli)',
                'Lindeberg-Lind, Mr Erik Gustaf',
                'Roebling, Mr Washington Augustus 2nd',
                'Rothes, the Countess of (Noel Lucy Martha Dyer-Edwardes)',
                'Simonius-Blumer, Col Alfons',
                'Thorne, Mr George (alias of: Mr George Rosenshine)',
                'Downton (?Douton), Mr William James',
                'Aijo-Nirva, Mr Isak',
                'Johannesen-Bratthammer, Mr Bernt',
                'Larsson-Rondberg, Mr Edvard',
                'Nicola-Yarred, Miss Jamila',
                'Nicola-Yarred, Master Elias',
                'Thomas, Mr John (? 1st/2nd class)'
            ]
        }
   }


This instant feedback helps you zero in on exceptions very quickly, taking a lot of the pain and guesswork out of early data exploration.

Capture More About Your Data
------------------------------------------------------------------------------

Build expectations as you conduct exploratory data analysis to ensure insights about data processes and pipelines remain part of your team's knowldege. Great Expectations's library of Expectations has been developed by a broad cross-section of data scientists and engineers. Check out the :ref:`glossary`; it covers all kinds of practical use cases:

* Foreign key verification and row-based accounting for ETL
* Form validation and regex pattern-matching for names, URLs, dates, addresses, etc.
* Checks for missing data
* Crosstabs
* Distributions for statistical modeling. 
* etc.

You can also add notes or even structured metadata to expectations to describe the intent of an expectation or anything else relevant for understanding it:

.. code-block:: bash

    >> my_df.expect_column_values_to_match_regex(
        "Name",
        "^[A-Za-z\, \(\)\']+$",
        meta = { "notes": "A simple experimental regex for name matching.", "source": "http://great-expectations.readthedocs.io/en/latest/glossary.html" })


Saving Expectations
------------------------------------------------------------------------------

At the end of your exploration, call `save_expectations` to store all Expectations from your session to your pipeline test files.

This is how you always know what to expect from your data.

.. code-block:: bash

    >> my_df.save_expectations_config("my_titanic_expectations.json")

For more detail on how to control expectation output, please see :ref:`standard_arguments` and :ref:`output_format`.

