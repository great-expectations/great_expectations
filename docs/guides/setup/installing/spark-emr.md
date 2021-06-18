---
title: Installing great_expectations in Spark EMR cluster
---
import Prerequisites from '/docs/guides/setup/components/install_prereq.jsx'
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with python

<Prerequisites>

</Prerequisites>

## Steps

### 1. Install Great Expectations

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the [Great Expectations Command Line Interface (CLI)](/docs/guides/setup/configuring-data-contexts/how-to-create-a-new-data-context-with-the-cli)

```console
sc.install_pypi_package("great_expectations")
```

### 2. Configure a Data Context in code
Follow the steps for creating an in-code Data Context in [How to instantiate a Data Context without a yml file](/docs/guides/setup/configuring-data-contexts/how-to-instantiate-a-data-context-without-a-yml-file)

The snippet at the end of the guide shows Python code that instantiates and configures a Data Context in code for an EMR Spark cluster. Copy this snippet into a cell in your EMR Spark notebook or use the other examples to customize your configuration.

Test your configuration.

Execute the cell with the snippet above.

Then copy this code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:
```console
context.list_datasources()
```

<Congratulations />

## Additional Notes

To view the full scripts used in this page, see them on GitHub:

## Next Steps

<NextSteps />
