---
title: Install Python
---

import GxData from '../_core_components/_data.jsx';
import PythonVersion from '../_core_components/_python_version.md';
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';

To use Great Expectations (GX) you need to install Python and the GX Core Python library. GX also recommends you set up a virtual environment for your GX Python projects.

## Prerequisites

- Internet access
- Permissions to download and install packages in your environment

## Install Python

1. Reference the [official Python documentation](https://www.python.org/) to install an appropriate version of Python.

   GX Requires <PythonVersion/>, which can be found on the [official Python downloads page](https://www.python.org/downloads/).


2. Verify your Python installation.

   Run the following command to display your Python version:

   ```shell title="Terminal input"
   python --version
   ```

   You should receive a response similar to the following:

   ```shell title="Terminal output"
   Python 3.9.19
   ```

## Optional. Create a virtual environment

   Although it is optional, the best practice when working with a Python project is to do so in a virtual environment.  A virtual environment ensures that any libraries and dependencies you install as part of your project do not encounter or cause conflicts with libraries and dependencies installed for other Python projects.

   There are various tools such as virtualenv and pyenv which can be used to create a virtual environment.  This example uses `venv` because it is included with Python 3.

1. Create the virtual environment with `venv`.

   To create your virtual environment, run the following code from the folder that the environment will be created in:

   ```shell title="Terminal input"
   python -m venv my_venv
   ```

   This command creates a new directory named `my_venv` for your virtual environment.

   :::tip Virtual environment names

   To use a different name for your virtual environment, replace `my_venv` in the example with the name you would prefer.  You will also have to replace `my_venv` with your virtual environment's actual name in any other example code that includes `my_venv`.

   :::

2. Optional. Test your virtual environment by activating it.

   Activate your virtual environment by running the following command from the same folder it was installed from:

   ```shell title="Terminal input"
   source my_venv/bin/activate
   ```
