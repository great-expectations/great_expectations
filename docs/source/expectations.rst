.. _expectations:

================================================================================
Expectations
================================================================================

Expectations are the workhorse abstraction in Great Expectations. Like assertions in traditional python unit tests, Expectations provide a flexible, declarative language for describing expected behavior. Unlike traditional unit tests, Great Expectations applies Expectations to data instead of code. (We find that this makes intuitive sense to many data scientists and engineers. See :ref:`pipeline_testing` for a more detailed explanation.

Connect-and-expect
------------------------------------------------------------------------------

Great Expectations's connect-and-expect API makes it easy to declare Expectations within the tools you already use for data exploration: jupyter notebooks, the ipython console, scratch scripts, etc.

.. code-block:: bash

    >> import great_expectations as ge
    >> ge.list_sources()
    ['our_postgres_db', 'last_minute_inventory.xlsx',]

    >> our_postgres_db = ge.connect_source('our_postgres_db')
    >> our_postgres_db.list_tables()
    ['users', 'inventory', 'discoveries']


    >> # Connect to a specific Table
    >> users_table = our_postgres_db.users
    >>
    >> # Create a new Expectation
    >> users_table.user_id.expect_column_values_to_be_unique()
    >>
    >> # Save the Expectation to great_expectations/my_postgres_users_table.json
    >> users_table.save_expectations()
    ```


Instant feedback
------------------------------------------------------------------------------

When you invoke an Expectation method from a notebook or console, it will immediately return a dictionary containing the result and information about any execptions

For example:

.. code-block:: bash

    >> users_table.user_id.list()
    [3,5,4,6,9,7,8,0,2,10,11,12,13,14,15,1,16,17,18,19,20,21,26,27,28,29,22,23,24,25]

    >> users_table.user_id.expect_column_values_to_be_unique()
    {
        "success" : True,
        "exception_list" : []
    }


Another example:

.. code-block:: bash

    >> discoveries_table.discoverer_first_name.expect_column_values_to_be_in_set(['Edison', 'Bell'])
    {
        "success" : False,
        "exception_list" : ["Curie", "Curie"]
    }

    >> discoveries_table.discoverer_first_name.expect_column_values_to_be_in_set([
        'Edison', 'Bell', 'Curie'
       ])
    {
        "success" : True,
        "exception_list" : []
    }


This instant feedback helps you zero in on exceptions very quickly, taking a lot of the pain and guesswork out of early data exploration.

Great Expectations's Expectations have been developed by a broad cross-section of data scientists and engineers. Check out the :ref:`glossary`; it covers all kinds of practical use cases:

* Foreign key verification and row-based accounting for ETL
* Form validation and regex pattern-matching for names, URLs, dates, addresses, etc.
* Checks for missing data
* Crosstabs
* Distributions for statistical modeling. 
* etc.

Saving Expectations
------------------------------------------------------------------------------

At the end of your exploration, call `save_expectations` to store all Expectations from your session to your pipeline test files. (See :ref:`under_the_hood` for a more detailed explanation of how this all works.)

This is how you always know what to expect from your data.

.. code-block:: bash

    >> our_postgres_db.save_expectations()


