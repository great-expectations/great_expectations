---
title: How to install Great Expectations locally
---
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with Python

## Steps

:::note Prerequisites
- Great Expectations requires Python 3. For details on how to download and install Python on your platform, see [python.org](https://www.python.org/downloads/).
:::

### 1. Install Python

First, check that you have Python 3 with pip installed. You can confirm your version of python with `python --version`.

```bash
# if python is version 3
python --version
python -m ensurepip --upgrade

# or if multiple versions of Python installed
python3 --version
python3 -m ensurepip --upgrade
```

### 2. Install Great Expectations

Use pip to install Great Expectations.

```bash
# if python is version 3
python -m pip install great_expectations

# or if multiple versions of Python installed
python3 -m pip install great_expectations
```

You can confirm that great_expectations was successfully installed with:
```console
great_expectations --version
```

<Congratulations />