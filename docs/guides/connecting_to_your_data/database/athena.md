---
title: How to connect to an Athena database
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will help you add an Athena instance (or a database) as a <TechnicalTag tag="datasource" text="Datasource" />. This will allow you to <TechnicalTag tag="validation" text="Validate" /> tables and queries within this instance. When you use an Athena Datasource, the validation is done in Athena itself. Your data is not downloaded.

<Prerequisites>

  - [Set up a working deployment of Great Expectations](../../../tutorials/getting_started/intro.md)
  - Installed the pyathena package for the Athena SQLAlchemy dialect (``pip install "pyathena[SQLAlchemy]"``)

</Prerequisites>

## Steps

### 1. Run the following CLI command to begin the interactive Datasource creation process:

```bash
great_expectations datasource new
```

When prompted to choose from the list of database engines, chose `other`.

### 2. Identify your connection string

In order to for Great Expectations to connect to Athena, you will need to provide a connection string.  To determine your connection string, reference the examples below and the [PyAthena documentation](https://github.com/laughingman7743/PyAthena#sqlalchemy).

The following urls don't include credentials as it is recommended to use either the instance profile or the boto3 configuration file.

If you want Great Expectations to connect to your Athena instance (without specifying a particular database), the URL should be:

```bash
awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}
```

Note the url parameter "s3_staging_dir" needed for storing query results in S3.

If you want Great Expectations to connect to a particular database inside your Athena, the URL should be:

```bash
awsathena+rest://@athena.{region}.amazonaws.com/{database}?s3_staging_dir={s3_path}
```

After providing your connection string, you will then be presented with a Jupyter Notebook.

### 3. Follow the steps in the Jupyter Notebook

The Jupyter Notebook will guide you through the remaining steps of creating a Datasource.  Follow the steps in the presented notebook, including entering the connection string in the yaml configuration.

## Additional notes

Environment variables can be used to store the SQLAlchemy URL instead of the file, if preferred - search documentation for "Managing Environment and Secrets".

