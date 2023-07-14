---
sidebar_label: 'Quickstart'
title: Quickstart
tag: [tutorial, getting started]
---
import Prerequisites from '/docs/components/_prerequisites.jsx'
import PythonVersion from '/docs/components/prerequisites/_python_version.md'
import SetupAndInstallGx from '/docs/components/setup/link_lists/_setup_and_install_gx.md'
import DataContextInitializeInstantiateSave from '/docs/components/setup/link_lists/_data_context_initialize_instatiate_save.md'

Use this quickstart to install GX, connect to sample data, build your first Expectation, validate data, and review the validation results. This is a great place to start if you're new to GX and aren't sure if it's the right solution for you or your organization. If you're using Databricks or SQL to store data, see [Get Started with GX and Databricks](../getting_started/how_to_use_great_expectations_in_databricks.md) or [Get Started with GX and SQL](../getting_started/how_to_use_great_expectations_with_sql.md).

:::note Great Expectations Cloud

You can use this quickstart with the open source Python version of GX or with Great Expectations Cloud.

If you're interested in participating in the Great Expectations Cloud Beta program, or you want to receive progress updates, [**sign up for the Beta program**](https://greatexpectations.io/cloud).

:::

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::

## Prerequisites

- <PythonVersion>
- pip
- An internet browser

## Install GX

1. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete.

2. In Python run the following command to import the `great_expectations` module:

    ```python name="tutorials/quickstart/quickstart.py import_gx"
    ```
## Create a DataContext

- Run the following command to import the existing `DataContext` object:

    ```python name="tutorials/quickstart/quickstart.py get_context"
    ```
## Connect to Data

- Run the following command to connect to existing `.csv` data stored in the `great_expectations` GitHub repository:

    ```python name="tutorials/quickstart/quickstart.py connect_to_data"
    ```

    The example code uses the default Data Context Datasource for Pandas to access the `.csv` data in the file at the specified `path`.

## Create Expectations

- Run the following command to create two Expectations:

    ```python name="tutorials/quickstart/quickstart.py create_expectation"
    ```

The first Expectation uses domain knowledge (the `pickup_datetime` shouldn't be null), and the second Expectation uses [`auto=True`](../../guides/expectations/how_to_use_auto_initializing_expectations.md#using-autotrue) to detect a range of values in the `passenger_count` column.

## Validate data

1. Run the following command to define a Checkpoint and examine the data to determine if it matches the defined Expectations:

    ```python name="tutorials/quickstart/quickstart.py create_checkpoint"
    ```

2. Run the following command to return the Validation results:

    ```python name="tutorials/quickstart/quickstart.py run_checkpoint"
    ```

3. Run the following command to view an HTML representation of the Validation results:

    ```python name="tutorials/quickstart/quickstart.py view_results"
    ```

## Related documentation

If you're ready to continue your Great Expectations journey, the following topics can help you implement a tailored solution for your specific environment and business requirements:

- [Install GX in a specific environment with support for a specific source data system](/docs/guides/setup/installation/install_gx).
- [Initialize, instantiate, and save a Data Contex](/docs/guides/setup/configure_data_contexts_lp).

