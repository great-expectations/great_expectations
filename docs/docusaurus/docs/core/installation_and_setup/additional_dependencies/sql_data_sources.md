---
title: Install and set up SQL Data Source support
---

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';

To validate data stored on SQL databases with GX Core, you create your GX Python environment, install GX Core locally, and then configure the necessary dependencies.

## Prerequisites

- <PrereqPythonInstalled/>
- pip. See [Installation and downloads](https://pypi.org/project/pip/).
- The necessary environment variables are set to allow access to the SQL database. See [Manage credentials](../manage_credentials.md).

## SQL database dependency commands

The following table lists the installation commands used to install GX Core dependencies for specific SQL databases. These dependencies are required for the successful operation of GX Core.

| SQL Database | Command |
| :-- | :-- | 
| AWS Athena | `pip install 'great_expectations[athena]'` |
| BigQuery | `pip install 'great_expectations[bigquery]'` |
| MSSQL | `pip install 'great_expectations[mssql]'` |
| PostgreSQL | `pip install 'great_expectations[postgresql]'` |
| Redshift | `pip install 'great_expectations[redshift]'` |
| Snowflake | `pip install 'great_expectations[snowflake]'` |
| Trino | `pip install 'great_expectations[trino]'` |

## Installation

1. Run the following code to confirm your Python version:

    ```bash title="Terminal input"
    python --version
    ```
    If your existing Python version is not 3.8 to 3.11, see [Active Python Releases](https://www.python.org/downloads/).

2. Run the following code to create a virtual environment and a directory named `my_venv`:

    ```bash title="Terminal input"
    python -m venv my_venv
    ```
    Optional. Replace `my_venv` with another directory name. 

    If you prefer, you can use virtualenv, pyenv, and similar tools to install GX in virtual environments.

3. Run the following code to activate the virtual environment: 

    ```bash title="Terminal input"
    source my_venv/bin/activate
    ```

4. Run the following code to install optional dependencies for SQLAlchemy:

    ```bash title="Terminal input"
    python -m pip install 'great_expectations[sqlalchemy]'
    ```
    To install optional dependencies for a different SQL database, see [SQL database dependency commands](#sql-database-dependency-commands).

5. Run the following code to confirm GX was installed successfully:

    ```bash title="Terminal input"
    great_expectations --version
    ```
    The output should be `great_expectations, version <version_number>`.

## Next steps

- [Manage Data Contexts](/core/installation_and_setup/manage_data_contexts.md)