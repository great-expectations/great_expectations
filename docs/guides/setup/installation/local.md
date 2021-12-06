---
title: How to install Great Expectations locally
---
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with Python.

## Steps

:::note Prerequisites
- Great Expectations requires Python 3. For details on how to download and install Python on your platform, see [python.org](https://www.python.org/downloads/).
:::

### 1. Check Python version and upgrade pip

First, check that you have Python 3 with pip installed. You can check your version of Python by running:

```bash
python --version
```

If this command returns something other than a Python 3 version number (like Python 3.X.X), you may need to try this:

```bash
python3 --version
```

Once you have confirmed that Python 3 is installed locally. You will want to ensure that pip is installed and upgraded to the latest version. Depending on whether you found that you needed to run `python` or `python3` in the previous step, you will run either:

```bash
python -m ensurepip --upgrade
```

or

```bash
python3 -m ensurepip --upgrade
```

This command installs pip if it is not already installed, and upgrades to the latest version.

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