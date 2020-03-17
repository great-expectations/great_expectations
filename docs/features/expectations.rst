.. _expectations:

##############
Expectations
##############

Expectations are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests,
Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests,
Great Expectations applies Expectations to data instead of code.

Expectations *enhance communication* about your data and *amplify quality* in data applications. Using expectations
helps reduce trips to domain experts and avoids leaving insights about data on the "cutting room floor."

**************************
How to build expectations
**************************

Generating expectations is one of the most important parts of using Great Expectations effectively, and there are
a variety of methods for generating and encoding expectations. When expectations are encoded in the GE format, they
become shareable and persistent sources of truth about how data was expected to behave-and how it actually did.

There are several paths to generating expectations:

1. Automated inspection of datasets. Currently, the profiler mechanism in GE produces expectation suites that can be
   used for validation. In some cases, the goal is :ref:`profiling` your data, and in other cases automated inspection
   can produce expectations that will be used in validating future batches of data.

2. Expertise. Rich experience from Subject Matter Experts, Analysts, and data owners is often a critical source of
   expectations. Interviewing experts and encoding their tacit knowledge of common distributions, values, or failure
   conditions can be can excellent way to generate expectations.

3. Exploratory Analysis. Using GE in an exploratory analysis workflow (e.g. within Jupyter notebooks) is an important \
   way to develop experience with both raw and derived datasets and generate useful and
   testable expectations about characteristics that may be important for the data's eventual purpose, whether
   reporting or feeding another downstream model or data system.


Expectations come to your data
================================

Great Expectations's connect-and-expect API makes it easy to declare Expectations within the tools you already use for
data exploration: jupyter notebooks, the ipython console, scratch scripts, etc.

>>> import great_expectations as ge
>>> my_df = ge.read_csv("./tests/examples/titanic.csv")
>>> my_df.expect_column_values_to_be_in_set("Sex", ["male", "female"])
{
    'success': True,
    'summary_obj': {
        'unexpected_count': 0,
        'unexpected_percent': 0.0,
        'unexpected_percent_nonmissing': 0.0,
        'partial_unexpected_list': []
    }
}



Instant feedback
==================

When you invoke an Expectation method from a notebook or console, it will immediately return a dictionary containing
the result and a list of exceptions.

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
            'unexpected_count': 1,
            'unexpected_percent': 0.0007616146230007616,
            'unexpected_percent_nonmissing': 0.0007616146230007616,
            'partial_unexpected_list': ['*']
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
            'unexpected_count': 16,
            'unexpected_percent': 0.012185833968012186,
            'unexpected_percent_nonmissing': 0.012185833968012186,
            'partial_unexpected_list': [
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


This instant feedback helps you zero in on exceptions very quickly, taking a lot of the pain and guesswork out of
early data exploration.

Iterative exploratory analysis
===============================

Build expectations as you conduct exploratory data analysis to ensure insights about data processes and pipelines
remain part of your team's knowldege. Great Expectations's library of Expectations has been developed by a broad
cross-section of data scientists and engineers. Check out the :ref:`expectation_glossary`; it covers all kinds of
practical use cases:

* Foreign key verification and row-based accounting for ETL
* Form validation and regex pattern-matching for names, URLs, dates, addresses, etc.
* Checks for missing data
* Crosstabs
* Distributions for statistical modeling.
* etc.

You can also add notes or even structured metadata to expectations to describe the intent of an expectation or anything
else relevant for understanding it:

.. code-block:: bash

    >> my_df.expect_column_values_to_match_regex(
        "Name",
        "^[A-Za-z\, \(\)\']+$",
        meta = {
            "notes": "A simple experimental regex for name matching.",
            "source": "max@company.com"
            }
       )


