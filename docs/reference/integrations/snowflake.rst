.. _snowflake:

##############
Snowflake
##############

*************
Prerequisites
*************

If you plan to use Great Expectations with the data in Snowflake, then please install the required modules first:
::
    pip install sqlalchemy snowflake-connector-python snowflake-sqlalchemy

Otherwise, your ``great_expectations init`` workflow will be interrupted, and you will be be prompted to do so, which
means that you will have to rerun ``great_expectations init`` after these packages are installed.  Except for having to
switch back and forth between ``great_expectations init`` and your Terminal ``shell``, the procedure will continue
without any problems, because Great Expectations will pick up from where it left off.

Please also be aware that Snowflake has certain naming conventions for database, schema, and table names (explained in
`<https://support.snowflake.net/s/article/faq-when-i-retrieve-database-schema-table-or-column-names-why-does-snowflake-display-them-in-uppercase>`_
and other associated Snowflake documentation articles).  It is assumed that the dataset of interest is properly loaded
into Snowflake in accordance with the Snowflake conventions and that the user performing the Great Expectations
installation has access to the *role* with sufficient privileges in order for Great Expectations to access the
Snowflake resources (account, data warehouse, database, schema, and tables), needed for a successful installation.

*******
Remarks
*******

#.
    If you end up having to execute ``great_expectations init`` multiple times in order to get Great Expectations
    properly installed for your project, you might notice a message, such as this, appearing on your Terminal screen:
    ::
        Great Expectations added some missing files required to run.
          - You may see new files in `great_expectations/uncommitted`.
          - You may need to add secrets to `great_expectations/uncommitted/config_variables.yml` to finish
            on-boarding.

    Please do not be alarmed.  This is an informational message only.  If you have supplied your Snowflake credentials
    correctly, then these credentials (also called "secrets", because they are considered confidential) will be stored
    in 'great_expectations/uncommitted/config_variables.yml', and this file will not be modified, unless you also
    (perhaps at a later time) decide to add a Slack webhook, or other confidential configuration parameters.  The other
    files and/or subdirectories that you may see in the ``great_expectations/uncommitted`` directory are validation
    results and ``data_docs``, which contains the HTML rendering of the validation results, generated for visualization
    and collaboration.
#.
    When using the Snowflake dialect, `SqlAlchemyDataset` will create a **transient** table instead of a **temporary**
    table when passing in `query` Batch Kwargs or providing `custom_sql` to its constructor. Consequently, users
    **must** provide a `snowflake_transient_table` in addition to the `query` parameter. Any existing table with that
    name will be overwritten.
#.
    When the prompt,
    ::
        Which table would you like to use?
    appears on your Terminal screen during the ``great_expectations init`` process, asking you to specify the data set,
    one of the valid options is:
    ::
        Do not see the table in the list above? Just type the SQL query

    Specifying the SQL query is straightforward: the user simply enters the desired SQL statement inline, without any
    additional punctuation (e.g., no quotes, semicolons, newlines, and other delimiting characters should be entered).

    For example:
    ::
        SELECT COL_1, COL_2 FROM SCHEMA_NAME.TABLE_NAME
    To minimize the possibility errors (and thus having to rerun ``great_expectations init``), it might be helpful to
    leave the ``great_expectations init`` aside (without terminating it) in its own Terminal window, and then develop
    and test your SQL query outside of the Great Expectations installation/configuration workflow (i.e., in a separate
    window or tool).  Once the query is confirmed to be working as desired, return to the Terminal window where
    ``great_expectations init`` was left running and copy and paste the final SQL query in response to the above prompt
    and press `ENTER` in order to continue.
