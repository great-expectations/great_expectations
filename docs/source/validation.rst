.. _validation:

================================================================================
Validation
================================================================================

Once you've constructed Expectations, you can use them to validate new data.

.. code-block:: bash

    >> import great_expectations as ge
    >> users_table = ge.connect_to_table('our_postgres_db', 'users')
    >> users_table.validate()
    user_id    expect_column_values_to_be_unique : True, []


Calling great_expectations's validation method generates a JSON-formatted report describing the outcome of all expectations.

.. code-block:: bash

    >> discoveries_table = ge.connect_to_table('our_postgres_db', 'discoveries')
    >> discoveries_table.validate()
    {
        "expectation_type" : "expect_column_values_to_be_in_set",
        "column" : "discoverer_first_name",
        "values" : ["Edison", "Bell"],
        "success" : false,
        "exception_list" : ["Curie", "Curie"]
    }

Command-line validation
------------------------------------------------------------------------------

This is especially powerful when combined with great_expectations's command line tool, which lets you validate in a one-line bash script. You can validate a single Table:

.. code-block:: bash

    $ great_expectations validate our_postgres_db.users

\...or a whole Data Source...

.. code-block:: bash

    $ great_expectations validate our_postgres_db

\...or the entire project.

.. code-block:: bash

    $ great_expectations validate


Deployment patterns
------------------------------------------------------------------------------

Useful deployment patterns include:

* Include validation at the end of a complex data transformation, to verify that no cases were lost, duplicated, or improperly merged.
* Include validation at the *beginning* of a script applying a machine learning model to a new batch of data, to verify that its distributed similarly to the training and testing set.
* Automatically trigger table-level validation when new data is dropped to an FTP site or S3 bucket, and send the validation report to the uploader and bucket owner by email.
* Schedule database validation jobs using cron, then capture errors and warnings (if any) and post them to Slack.
* Validate as part of an Airflow task: if Expectations are violated, raise an error and stop DAG propagation until the problem is resolved. Alternatively, you can implement expectations that raise warnings without halting the DAG.


