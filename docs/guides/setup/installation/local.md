---
title: How to install Great Expectations locally
---
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with Python

## Steps

### 1. Install required dependencies

First, check that you have python3 with pip installed

```console
python --version
# or if multiple versions of Python installed
python3 --version
python3 -m pip --version
```

### 2. Install Great Expectations
```console
pip install great_expectations
great_expectations --version
```

To configure your Data Context, please look at [How to configure a new data context with the cli](../configuring_data_contexts/how_to_configure_a_new_data_context_with_the_cli.md)

<Congratulations />

