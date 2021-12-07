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

### 1. Check Python version

First, check that you have Python 3 with pip installed. You can check your version of Python by running:

```bash
python --version
```

If this command returns something other than a Python 3 version number (like Python 3.X.X), you may need to try this:

```bash
python3 --version
```

### 2. Install Great Expectations

You can use either pip or Anaconda to install Great Expectations.

<Tabs
  groupId="pip-or-conda"
  defaultValue='pip'
  values={[
  {label: 'pip', value:'pip'},
  {label: 'conda', value:'conda'},
  ]}>
<TabItem value="pip">

Once you have confirmed that Python 3 is installed locally, you will run two commands that will:

1. Ensure that pip is installed and upgraded to the latest version
2. Install Great Expectations using pip

Depending on whether you found that you needed to run `python` or `python3` in the previous step, you will run either:

```python file=../../../../tests/integration/docusaurus/setup/installation/local.py#L2-L3
```

or

```python file=../../../../tests/integration/docusaurus/setup/installation/local.py#L7-L8
```

</TabItem>
<TabItem value="conda">

Once you have confirmed that Python 3 is installed locally, you will want to ensure that Anaconda is installed by running:

```python file=../../../../tests/integration/docusaurus/setup/installation/local.py#L12
```

If no version number is printed, [you can download Anaconda here](https://www.anaconda.com/products/individual).

Depending on whether you found that you needed to run `python` or `python3` in the previous step, you will run either:

```python file=../../../../tests/integration/docusaurus/setup/installation/local.py#L13
```

or

```python file=../../../../tests/integration/docusaurus/setup/installation/local.py#L18
```

</TabItem>
</Tabs>

You can confirm that great_expectations was successfully installed with:
```bash
great_expectations --version
```

<Congratulations />