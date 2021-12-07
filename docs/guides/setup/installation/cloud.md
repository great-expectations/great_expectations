---
title: How to install Great Expectations in the cloud
---
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations on a Spark EMR cluster.

## Steps

### 1. Install Great Expectations

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the [Great Expectations Command Line Interface (CLI)](/docs/guides/setup/configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli)

```console
sc.install_pypi_package("great_expectations")
```

### 2. Configure a Data Context in code
Follow the steps for creating an in-code Data Context in [How to instantiate a Data Context without a yml file](/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file)

[Here is Python code](/docs/guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file) that instantiates and configures a Data Context in code for an EMR Spark cluster. Copy this snippet into a cell in your EMR Spark notebook or use the other examples to customize your configuration. Execute the snippet to instantiate a Data Context in memory.

Then copy the following code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:
```console
context.list_datasources()
```

<Congratulations />

