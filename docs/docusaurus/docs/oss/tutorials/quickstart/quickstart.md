---
sidebar_label: 'Quickstart'
title: Quickstart
tag: [tutorial, getting started]
---
import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrereqPython from '/docs/components/prerequisites/_python_version.md'
import SetupAndInstallGx from '/docs/components/setup/link_lists/_setup_and_install_gx.md'
import DataContextInitializeInstantiateSave from '/docs/components/setup/link_lists/_data_context_initialize_instatiate_save.md'
import TechnicalTag from '@site/docs/oss/term_tags/_tag.mdx';

Use this quickstart to install GX, connect to sample data, build your first Expectation, validate data, and review the validation results. This is a great place to start if you're new to GX and aren't sure if it's the right solution for you or your organization. If you're using Databricks or SQL to store data, see [Get Started with GX and Databricks](../getting_started/how_to_use_great_expectations_in_databricks.md) or [Get Started with GX and SQL](../getting_started/how_to_use_great_expectations_with_sql.md).

:::note Great Expectations Cloud

You can use this quickstart with the open source Python version of GX or with Great Expectations Cloud.

If you're interested in participating in the Great Expectations Cloud Beta program, or you want to receive progress updates, [**sign up for the Beta program**](https://greatexpectations.io/cloud).

:::

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::


## Data validation workflow

The following diagram illustrates the end-to-end GX data validation workflow that you'll implement with this quickstart. Click a workflow step to view the related content.

```mermaid
flowchart LR
%%{init: {"themeVariables": {"fontSize" : "24px"}}}%%

    1(Install\n<b>GX</b>) --> 2("Create a\n<b>Data Context</b>")

    2 --> 3

    3("Connect\nto data</b>")

    3 --> 4

    4("Create a\n<b>Validator</b>") --> 5("Create\n<b>Expectations</b>")

    5 --> 6

    6("Run a\n<b>Checkpoint</b>") --> 7("View\n<b>Validation Results</b>")

%% Link each workflow step to section on page.
click 1 "#install-gx"
click 2 "#create-a-data-context"
click 3 "#connect-to-data"
click 4 "#connect-to-data"
click 5 "#create-expectations"
click 6 "#validate-data"
click 7 "#validate-data"
```

## Prerequisites

- <PrereqPython />
- pip
- An internet browser


## Install GX

1. Run the following command in an empty base directory inside a Python virtual environment:

    ```bash title="Terminal input"
    pip install great_expectations
    ```

    It can take several minutes for the installation to complete.

2. Run the following Python code to import the `great_expectations` module:

    ```python name="tutorials/quickstart/quickstart.py import_gx"
    ```
## Create a Data Context

- Run the following command to create a <TechnicalTag tag="data_context" text="Data Context"/> object:

    ```python name="tutorials/quickstart/quickstart.py get_context"
    ```
## Connect to data

- Run the following command to connect to existing `.csv` data stored in the `great_expectations` GitHub repository and create a <TechnicalTag tag="validator" text="Validator"/> object:

    ```python name="tutorials/quickstart/quickstart.py connect_to_data"
    ```

    The code example uses the default <TechnicalTag tag="data_context" text="Data Context"/> <TechnicalTag tag="datasource" text="Data Source"/> for Pandas to access the `.csv` data from the file at the specified URL path.

## Create Expectations

- Run the following commands to create two <TechnicalTag tag="expectation" text="Expectations"/> and save them to the <TechnicalTag tag="expectation_suite" text="Expectation Suite"/>:

    ```python name="tutorials/quickstart/quickstart.py create_expectation"
    ```

  The first <TechnicalTag tag="expectation" text="Expectation"/> uses domain knowledge (the `pickup_datetime` shouldn't be null).

  The second <TechnicalTag tag="expectation" text="Expectation"/> uses [`auto=True` to detect a range of values](/docs/oss/guides/expectations/how_to_use_auto_initializing_expectations#using-autotrue) in the `passenger_count` column.

## Validate data

1. Run the following command to define a <TechnicalTag tag="checkpoint" text="Checkpoint"/> and examine the data to determine if it matches the defined <TechnicalTag tag="expectation" text="Expectations"/>:

    ```python name="tutorials/quickstart/quickstart.py create_checkpoint"
    ```

2. Run the following command to return the <TechnicalTag tag="validation_result" text="Validation Results"/>:

    ```python name="tutorials/quickstart/quickstart.py run_checkpoint"
    ```

3. Run the following command to view an HTML representation of the <TechnicalTag tag="validation_result" text="Validation Results"/> in the generated <TechnicalTag tag="data_docs" text="Data Docs"/>:

    ```python name="tutorials/quickstart/quickstart.py view_results"
    ```

## Related documentation

If you're ready to continue your GX journey, the following topics can help you implement a solution for your specific environment and business requirements:

- [Install GX in a specific environment with support for a specific Data Source](/docs/oss/guides/setup/installation/install_gx).
- [Initialize, instantiate, and save a Data Context](/docs/oss/guides/setup/configure_data_contexts_lp).
- [Connect to Data Sources](/docs/oss/guides/connecting_to_your_data/connect_to_data_lp).
- [Create and manage Expectations and Expectation Suites](/docs/oss/guides/expectations/expectations_lp/).
- [Create, manage, and run Checkpoints](/docs/oss/guides/validation/checkpoints/checkpoint_lp/).