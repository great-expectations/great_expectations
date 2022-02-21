---
title: How to install Great Expectations locally
---
import NextSteps from '/docs/guides/setup/components/install_nextsteps.md'
import Congratulations from '/docs/guides/setup/components/install_congrats.md'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This guide will help you Install Great Expectations locally for use with Python.

:::note
- Great Expectations is developed and tested on macOS and Linux Ubuntu. Installation for Windows users may vary from the steps listed below. If you have questions, feel free to reach out to the community on our [Slack channel](https://greatexpectationstalk.slack.com/join/shared_invite/zt-sugx45gn-SFe_ucDBbfi0FZC0mRNm_A#/shared-invite/email).
- If you have the Mac M1, you may need to follow the instructions in this blog post: [Installing Great Expectations on a Mac M1](https://greatexpectations.io/blog/m-one-mac-instructions/).
:::


## Steps

:::note Prerequisites
- Great Expectations requires Python 3. For details on how to download and install Python on your platform, see [python.org](https://www.python.org/downloads/).
:::

### 1. Check Python version

First, check that you have Python 3 installed. You can check your version of Python by running:

```console
python --version
```

If this command returns something other than a Python 3 version number (like Python 3.X.X), you may need to try this:

```console
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

Once you have confirmed that Python 3 is installed locally, you can create a virutal environment with venv.

<details>
<summary>Python Virtual Environments</summary>
We have chosen to use venv for virtual environments in this guide, because it is included with Python 3. You are not limited to using venv, and can just as easily install Great Expectations into virtual environments with tools such as virtualenv, pyenv, etc.
</details>

Depending on whether you found that you needed to run `python` or `python3` in the previous step, you will run either:

```console
python -m venv venv
```

or

```console
python3 -m venv venv
```

This command will create a new directory called `venv` where your virtual environment is located. In order to activate the virtual environment run:

```console
source venv/bin/activate
```

Finally, you can ensure the latest version of pip is installed, and install Great Expectations, by running the appropriate pair of `python` commands below:

```console
python -m ensurepip --upgrade
python -m pip install great_expectations
```

or

```console
python3 -m ensurepip --upgrade
python3 -m pip install great_expectations
```

</TabItem>
<TabItem value="conda">

Once you have confirmed that Python 3 is installed locally, you will want to ensure that Anaconda is installed by running:

```console
conda --version
```

If no version number is printed, [you can download Anaconda here](https://www.anaconda.com/products/individual).

Once Anaconda is installed, you can create and activate a new virtual environment by running:

```console
conda create --name YOUR_ENVIRONMENT_NAME
conda activate YOUR_ENVIRONMENT_NAME
```

Replace "YOUR_ENVIRONMENT_NAME" with the name you wish you use for your environment.

Finally, you can install Great Expectations by running:

```console
conda install -c conda-forge great-expectations
```

</TabItem>
</Tabs>

You can confirm that great_expectations was successfully installed with:

```console
great_expectations --version
```

<Congratulations />