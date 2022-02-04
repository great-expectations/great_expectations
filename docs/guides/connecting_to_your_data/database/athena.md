---
title: How to connect to a Athena database
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'

This guide will help you add an Athena instance (or a database) as a Datasource. This will allow you to validate tables and queries within this instance. When you use an Athena Datasource, the validation is done in Athena itself. Your data is not downloaded.

<Prerequisites>

  - [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
  - Installed the pyathena package for the Athena SQLAlchemy dialect (``pip install "pyathena[SQLAlchemy]"``)

</Prerequisites>

Steps
-----

1. Run the following CLI command to begin the interactive Datasource creation process:

    ```bash
    great_expectations datasource new
    ```

2. Choose "other" from the list of database engines, when prompted.

3. Identify the connection string you would like Great Expectations to use to connect to Athena, using the examples below and the [PyAthena](https://github.com/laughingman7743/PyAthena#sqlalchemy) documentation.

    The following urls dont include credentials as it is recommended to use either the instance profile or the boto3 configuration file.

    If you want Great Expectations to connect to your Athena instance (without specifying a particular database), the URL should be:

    ```bash
    awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}
    ```

    Note the url parameter "s3_staging_dir" needed for storing query results in S3.

    If you want Great Expectations to connect to a particular database inside your Athena, the URL should be:

    ```bash
    awsathena+rest://@athena.{region}.amazonaws.com/{database}?s3_staging_dir={s3_path}
    ```

5. You will be presented with a Jupyter Notebook which will guide you through the steps of creating a Datasource.

6. Follow the steps in this Jupyter Notebook including entering the connection string in the yaml configuration.


Additional notes
----------------

Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

