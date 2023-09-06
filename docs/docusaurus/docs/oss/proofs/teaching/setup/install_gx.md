---
title: Install Great Expectations
description: A guide to setting installing Great Expectations
tag: [tutorial]
---

This guide will walk you through verifying that your version of Python is supported by Great Expectations (GX), preparing a virtual environment for your GX project, and finally installing the GX library itself.

## Prerequisites

- An installation of Python, version 3.8 to 3.11.  To download and install Python, see [Python's downloads page](https://www.python.org/downloads/).
- Internet access to install Python libraries

:::info Windows Support

Windows support for the open source Python version of GX is currently unavailable. If you’re using GX in a Windows environment, you might experience errors or performance issues.

:::

## Check your Python version

You should already have a copy of Python installed.  GX supports Python versions 3.8 to 3.11.  You can check your Python version by running the following from your terminal's command prompt:

```commandline
python --version
```

If a Python 3 version number is not returned, run the following command:

```commandline
python3 --version
```

If a Python 3 version number is still not returned, you should verify your Python installation.

:::info 

The rest of this guide will use `python` as the executable for relevant command prompts.  However, if you needed to use `python3` to recieve a Python 3 version number when verifying your Python version then you should replace `python` in the following examples with `python3`.

:::

## Set up a virtual environment

After you have confirmed that Python 3 is installed locally, you can create a virtual environment with `venv` before installing your packages with `pip`.  The following examples use `venv` for virtual environments because it is included with Python 3.  However, you can use alternate tools such as `virtualenv` and `pyenv` to install GX in virtual environments.

Run the following from your terminal's command prompt to create your virtual environment in your current working directory:

```commandline
python -m venv my_venv
```

A new directory named `my_venv` is created in your current working directory.  This will contain your virtual environment.

:::tip 

You can create a virtual environment with a different name (and correspondingly named directory) by replacing `my_venv` in the example code.

:::

To activate your virtual environment run the following from your terminal's command prompt:

```commandline
source my_venv/bin/activate
```

:::tip Reminder 

If you changed the name of your virtual environment you will have to change `my_venv` in the above example as well.

:::


##  Update pip

Python includes pip as a tool for installing Python packages. After you've activated your virtual environment you should ensure that you have the latest version of pip installed.

Run the following from your terminal command prompt to ensure you have the latest version of pip installed:

```commandline
python -m ensurepip --upgrade
```

## Install GX

To use pip to install GX, run the following from your terminal command prompt:

```commandline
python -m pip install great_expectations
```

## Next steps

You now have a Python 3 virtual environment prepared for your GX project and the GX library installed to that environment.  

Next, you should connect your GX project to some data.