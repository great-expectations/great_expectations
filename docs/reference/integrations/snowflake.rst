.. _snowflake:

#######################################
How to Configure a Snowflake Datasource
#######################################

This guide shows how to connect to a Snowflake Datasource.

-----
Steps
-----

To add a Snowflake datasource do this:

#.
    Run ``great_expectations datasource new``
#.
    Choose the *Relational database (SQL)* option from the menu (i.e., type ``2`` and press `ENTER`).
#.
    When asked *Which database backend are you using?*, choose ``Snowflake`` (i.e., type ``4`` and press `ENTER`).
#.
    When prompted to *Give your new data source a short name.*, choose the suggested default, or provide a custom name
    for your Snowflake data source.
#.
    Next, you will be asked to supply the credentials for your Snowflake instance:

    * login name,
    * password,
    * account name,
    * database name,
    * schema name,
    * warehouse name, and
    * role.

    Great Expectations will store these secrets privately on your machine (e.g., they will not be committed to GitHub).
#.
    You will then see the following message on your Terminal screen:
    ::
        Attempting to connect to your database. This may take a moment...
#.
    Finally, if all goes well, the message
    ::
        Great Expectations connected to your database!
        A new datasource '<your_new_snowflake_data_source>' was added to your project.

    will appear on your Terminal screen. After this confirmation, you can proceed with exploring the data sets in your
    new Snowflake data source.

----------------
Additional Notes
----------------

#.
    Assuming that you intend to use Great Expectations with the data in Snowflake, you may wish to install the required
    modules first:
    ::
        pip install sqlalchemy snowflake-connector-python snowflake-sqlalchemy

    Otherwise, your ``great_expectations init`` workflow will be interrupted, and you will be be prompted to do so,
    which means that you will have to rerun ``great_expectations init`` after these packages are installed.  Except for
    having to switch back and forth between ``great_expectations init`` and your Terminal ``shell``, the procedure will
    continue without any problems, because Great Expectations will pick up from where it left off.
#.
    When using the Snowflake dialect, `SqlAlchemyDataset` will create a **transient** table instead of a **temporary**
    table when passing in `query` Batch Kwargs or providing `custom_sql` to its constructor. Consequently, users
    **must** provide a `snowflake_transient_table` in addition to the `query` parameter. Any existing table with that
    name will be overwritten.
