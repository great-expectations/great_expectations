---
title: Quickstart with GX
tag: [tutorial, getting started]
---
# Quickstart with Great Expectations

import Prerequisites from '/docs/components/_prerequisites.jsx'

import TipUseAVenv from '/docs/components/setting_up/installing_gx/_tip_use_a_venv.md'
import GxInstallationVerifySuccess from '/docs/components/setting_up/installing_gx/_gx_installation_verify_success.md'
import TipUseTheExpectationGallery from '/docs/components/creating_expectations/individually/_tip_use_the_expectation_gallery.md'

## Introduction

We here at GX know that few things are as daunting as taking your first steps with a new piece of software.  That's why the purpose of this guide is to demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of installing GX, connecting to some sample data, building your first Expectation based off of an initial Batch of that data, and finally saving that Expectation so that it can be used to validate additional data in the future.

Once you have completed this guide you will have a foundation in the basics of using GX.  In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python packages with pip
- A working internet browser
- A passion for data quality

</Prerequisites> 


<TipUseAVenv />

## Steps

### 1. Setting up

#### 1.1 Take a look at the code you'll be using

With GX you can get up and running with just a few lines of code.  The full process you'll be using will look like:

```bash title="Terminal input"
$ pip install great_expectations
```

```python title="Python code"
import great_expectations as gx

context = gx.get_context()

validator = context.datasources.default.pandas.read_csv("https://raw.githubusercontent.com/great_expectations/taxi_data.csv")

validator.expect_column_values_to_not_be_null("pickup_datetime")

# ---

result = validator.validate()

result.view()

context.convert_to_file_context()
```

That's not a lot of code, is it?  In the following steps we'll break down exactly what is happening here so that you can follow along and perform a Validation yourself.

#### 1.2 Install GX using pip

Starting from an empty base directory inside a Python virtual environment, we use pip to install Great Expectations:

```bash title="Terminal input"
$ pip install great_expectations
```

When you run this command from the terminal you will see `pip` go through the process of installing GX and it's related dependencies.  This may take a moment to complete.

:::tip Tip: How to verify your GX installation succeeded

<GxInstallationVerifySuccess />

:::


#### 1.3 Open a Jupyter Notebook (or your preferred Python interpreter)

For the rest of this tutorial we will be working with Python code in a Jupyter Notebook. Jupyter is included with GX and provides a very convenient interface that lets us easily edit code and immediately see the result of our changes.

We can open a Jupyter Notebook from the terminal with the command:

```bash title="Terminal input"
$ jupyter notebook
```

When you do this your browser will open to a Jupyter Notebook showing navigation for a filesystem with a root of the directory you ran the `jupyter notebook` command from.  You will use the dropdown above this to launch a new Python kernel.

#### 1.4 Import Great Expectations

The code to import the `great_expectations` module is:

```python title="Python code"
import great_expectations as gx
```

The `great_expectations` module is the only import you will need to specify.  With it, you can instantiate a Data Context which will provide you with access to everything else you need to follow this guide and Validate your first Batch of data.

#### 1.5 Instantiate a Data Context

We will get a `DataContext` object with the following code:

```python title="Python code"
context = gx.get_context()
```

The Data Context will provide you with access to a variety of utility and convenience methods.  It is the entry point for using the GX Python API.

### 2. Connect to data

For the purpose of this guide, we will connect to `.csv` data stored in our GitHub repo.  This is done with the code:

```python title="Python code"
validator = context.datasources.default.pandas.read_csv("https://raw.githubusercontent.com/great_expectations/taxi_data.csv")
```

This single line of code does a lot of things.  We're using our Data Context object (`context`) to provide a Datasource (`.datasource`) with a standard configuration (`.default`) that uses Pandas as an Execution Engine (`.pandas`) to provide the contents of a file to GX (`.read_csv`).  With one line of code we've loaded a specific `.csv` file into an object that will allow us to evaluate its data and use that evaluation to validate other files in the future.

### 3. Create Expectations

At this point, we will describe our Expectations for the data in question.  In this example, we will define a single Expectation based on our domain knowledge (that is: based on what we know _should_ be true about our data, without looking at the actual state of the data).

The code we will use for this is:

```python title="Python code"
validator.expect_column_values_to_not_be_null("pickup_datetime")
```

The `validator` object that we received when we connected to our data is capable of introspecting the data in its associated Datasource.  With the Expectation defined above, we are stating that we _expect_ the column `pickup_datetime` to always be populated.  That is: none of the column's values should be null.

In the future, you may define numerous Expectations by using the `validator.expect_*` syntax.  When you Validate data with the `validator` object, all of you Expectations will be run against the `validator`'s associated Datasource.

<TipUseTheExpectationGallery /> 

### 4. Validate data

#### 4.1 Execute your defined Expectations

Now that we have defined our Expectations it is time for GX to introspect our data and see if it corresponds to them.  (We call this process Validation.)  The code to run this process is:

```python title="Python code"
result = validator.validate()
```

#### 4.1 Review your results

Once the `validator` object finishes executing the `validate()` command, you can view the results of your Validation through the code:

```python title="Python code"
result.view()
```

#### 4.2 Save your Expectations for use with future data

## Next Steps 

Now that you've seen how easy it is to implement the GX workflow, it is time to customize that workflow to suit your specific use cases.

To help with this we have prepared some more detailed installation and setting up guides tailored to specific environments and resources:
- Installing and setting up GX for local filesystem data with pip
- Installing and setting up GX for local filesystem data with conda
- Installing and setting up GX for PostgreSQL
- Installing and setting up GX for Amazon S3
- Installing and setting up GX for Azure Blob Storage
- Installing and setting up GX for GCS

## Additional information

### Code examples

To see the full source code used for the examples in this guide, please reference the following script in our GitHub repository:
- [quickstart.py](https://path/to/the/script/on/github.com)

### Python APIs

For more information on the GX Python objects and APIs used in this guide, please reference the following pages of our public API documentation:

- [get_context()](https://docs.greatexpectations.io/docs/reference/api/util.py/#great_expectations.util.get_context)

- Validator
  - [.validate()](https://docs.greatexpectations.io/docs/reference/api/validator/validator/Validator_class#great_expectations.validator.validator.Validator.validate)
  - [.expect_column_values_to_not_be_null()](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)


For more information on the `venv` module, please reference [Python's official `venv` documentation](https://docs.python.org/3/library/venv.html).