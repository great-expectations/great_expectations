.. _how_to_guides__configuring_datasources__how_to_configure_an_athena_datasource:

How to configure an Athena Datasource
=========================================================

This guide will help you add an Athena instance (or a database) as a Datasource. This will allow you to validate tables and queries within this instance. When you use an Athena Datasource, the validation is done in Athena itself. Your data is not downloaded.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

  - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
  - Installed the pyathena package for the Athena sqlalchemy dialect (``pip install "pyathena[SQLAlchemy]"``)

Steps
-----


1. Run the following CLI command to begin the interactive Datasource creation process:

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. code-block:: bash

            great_expectations datasource new

    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        .. code-block:: bash

            great_expectations --v3-api datasource new


2. Choose "other" from the list of database engines, when prompted.
3. Identify the connection string you would like Great Expectations to use to connect to Athena, using the examples below and the `PyAthena <https://github.com/laughingman7743/PyAthena#sqlalchemy>`_ documentation.

    The following urls dont include credentials as it is recommended to use either the instance profile or the boto3 configuration file.

    If you want Great Expectations to connect to your Athena instance (without specifying a particular database), the URL should be:

    .. code-block:: bash

        awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}

    Note the url parameter "s3_staging_dir" needed for storing query results in S3.

    If you want Great Expectations to connect to a particular database inside your Athena, the URL should be:

    .. code-block:: bash

        awsathena+rest://@athena.{region}.amazonaws.com/{database}?s3_staging_dir={s3_path}


.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        5. Enter the connection string when prompted (and press Enter when asked "Would you like to proceed? [Y/n]:").

        6. Should you need to modify your connection string, you can manually edit the
           ``great_expectations/uncommitted/config_variables.yml`` file.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API

        5. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.

        6. Follow the steps in this Jupyter Notebook including entering the connection string in the yaml configuration.




Additional notes
----------------

Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

Additional resources
--------------------
