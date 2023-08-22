---
title: Connect to a SQL database
tag: [how-to, setup]
keywords: [Great Expectations, SQL]

---

# How to set up Great Expectations to work with general SQL databases

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ## Prerequisites -->

<!-- ### 1. Check your Python version -->
import PythonCheckVersion from '/docs/components/setup/python_environment/_python_check_version.mdx'

<!-- ### 2. Create a Python virtual environment -->
import PythonCreateVenv from '/docs/components/setup/python_environment/_python_create_venv.md'
import TipPythonOrPython3Executable from '/docs/components/setup/python_environment/_tip_python_or_python3_executable.md'

<!-- ### 3. Install GX with optional dependencies for databases -->
import InstallDependencies from '/docs/components/setup/dependencies/_sql_install_dependencies.mdx'

<!-- ### 4. Verify that GX has been installed correctly -->
import GxVerifyInstallation from '/docs/components/setup/_gx_verify_installation.md'

<!-- ## Next steps -->
import LinksAfterInstallingGx from '/docs/components/setup/next_steps/_links_after_installing_gx.md'


This guide will walk you through best practices for creating your GX Python environment and demonstrate how to locally install Great Expectations along with the necessary dependencies for working with SQL databases.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python modules with pip

</Prerequisites>

## Steps

### 1. Check your Python version

<PythonCheckVersion />

<TipPythonOrPython3Executable />

### 2. Create a Python virtual environment

<PythonCreateVenv />

### 3. Install GX with optional dependencies for SQL databases

<InstallDependencies install_key="sqlalchemy" database_name="SQL databases"/>

:::caution Additional dependencies for some SQL dialects

The above pip instruction will install GX with basic SQL support through SqlAlchemy.  However, certain SQL dialects require additional dependencies.  Depending on the SQL database type you will be working with, you may wish to use one of the following installation commands, instead:

- AWS Athena: `pip install 'great_expectations[athena]'`
- BigQuery: `pip install 'great_expectations[bigquery]'`
- MSSQL: `pip install 'great_expectations[mssql]'`
- PostgreSQL: `pip install 'great_expectations[postgresql]'`
- Redshift: `pip install 'great_expectations[redshift]'`
- Snowflake: `pip install 'great_expectations[snowflake]'`
- Trino: `pip install 'great_expectations[trino]'`

:::

### 4. Verify that GX has been installed correctly

<GxVerifyInstallation />

### 5. Setting up credentials

Different SQL dialects have different requirements for connection strings and methods of configuring credentials.  By default, GX allows you to define credentials as environment variables or as values in your Data Context ([once you have initialized one](/docs/guides/setup/configuring_data_contexts/initializing_data_contexts/how_to_initialize_a_filesystem_data_context_in_python)).

There may also be third party utilities for setting up credentials of a given SQL database type.  For more information on setting up credentials for a given source database, please reference the official documentation for that SQL dialect as well as our guide on [how to set up credentials(/docs/guides/setup/configuring_data_contexts/how_to_configure_credentials).

## Next steps

Now that you have installed GX with the necessary dependencies for working with SQL databases, you are ready to initialize your <TechnicalTag tag="data_context" text="Data Context" />.  The Data Context will contain your configurations for GX components, as well as provide you with access to GX's Python API.

<LinksAfterInstallingGx />





