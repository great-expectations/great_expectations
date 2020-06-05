.. _how_to_guides__configuring_datasources__how_to_configure_a_redshift_datasource:

######################################
How to configure a Redshift Datasource
######################################

This guide shows how to connect to a Redshift Datasource.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <getting_started>`

-----
Steps
-----

To add a Redshift datasource do this:

#.
    Run ``great_expectations datasource new``
#.
    Choose the *Relational database (SQL)* option from the menu (i.e., type ``2`` and press `ENTER`).
#.
    When asked *Which database backend are you using?*, choose ``Redshift`` (i.e., type ``3`` and press `ENTER`).
#.
    When prompted to *Give your new data source a short name.*, choose the suggested default, or provide a custom name
    for your Redshift data source.
#.
    Next, you will be asked to supply the credentials for your Redshift instance:

    * host,
    * port (with the port number 5439 offered as a default),
    * username,
    * password,
    * database name, and
    * sslmode (with the value "prefer" offered as a default).

    Great Expectations will store these secrets privately on your machine (e.g., they will not be committed to GitHub).
#.
    You will then see the following message on your Terminal screen:
    ::
        Attempting to connect to your database. This may take a moment...
#.
    Finally, if all goes well, the message
    ::
        Great Expectations connected to your database!
        A new datasource '<your_new_redshift_data_source>' was added to your project.

    will appear on your Terminal screen. After this confirmation, you can proceed with exploring the data sets in your
    new Redshift data source.

#. Should you need to modify your connection string you can manually edit the ``great_expectations/uncommitted/config_variables.yml`` file.

----------------
Additional Notes
----------------

#.
    Assuming that you intend to use Great Expectations with the data in Redshift, you may wish to install the required
    modules first:

    .. code-block:: python

        pip install sqlalchemy 

        pip install psycopg2 (or if on macOS: pip install psycopg2-binary)

    Otherwise, your ``great_expectations datasource new`` workflow will be interrupted, and you will be be prompted to do so,
    which means that you will have to rerun ``great_expectations datasource new`` after these packages are installed.  Except for
    having to switch back and forth between ``great_expectations datasource new`` and your Terminal ``shell``, the procedure will
    continue without any problems, because Great Expectations will pick up from where it left off.
#.
    Note that your Redshift connection can be equivalently described under the '<your_new_redshift_data_source>' key in your
    "uncommitted/config_variables.yml" file as follows:

    .. code-block:: python

        "postgresql+psycopg2://username:password@host:port/database_name?sslmode=require"
#.
    Depending on your Redshift cluster configuration, you may or may not need the ``sslmode`` parameter.

--------
Comments
--------

    .. discourse::
        :topic_identifier: 169

