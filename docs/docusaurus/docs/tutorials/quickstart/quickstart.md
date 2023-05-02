---
title: Quickstart with GX
tag: [tutorial, getting started]
---
# Quickstart with Great Expectations

import Prerequisites from '/docs/components/_prerequisites.jsx'
import SetupAndInstallGx from '/docs/components/setup/link_lists/_setup_and_install_gx.md'
import DataContextInitializeInstantiateSave from '/docs/components/setup/link_lists/_data_context_initialize_instatiate_save.md'

Few things are as daunting as taking your first steps with a new piece of software.  This guide will introduce you to GX and demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of installing GX, connecting to some sample data, building your first Expectation based off of an initial Batch of that data, validating your data with that Expectation, and finally reviewing the results of your validation.

Once you have completed this guide you will have a foundation in the basics of using GX.  In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python packages with pip
- A working internet browser

</Prerequisites> 

## Overview

With GX you can get up and running with just a few lines of code.  The full process you'll be using will look like:

```bash title="Terminal input"
pip install great_expectations
```

```python name="tutorials/quickstart/quickstart.py all"
```

In the following steps we'll break down exactly what is happening here so that you can follow along and perform a Validation yourself.


## Steps

### 1. Install GX and set up your code environment

#### 1.1 Install GX using pip

Starting from an empty base directory inside a Python virtual environment, we use pip to install Great Expectations:

```bash title="Terminal input"
pip install great_expectations
```

When you run this command from the terminal you will see `pip` go through the process of installing GX and it's related dependencies.  This may take a moment to complete.

#### 1.2 Import Great Expectations

For the rest of this tutorial we will be working with Python code in a Jupyter Notebook. Jupyter is included with GX and provides a very convenient interface that lets us easily edit code and immediately see the result of our changes.

The code to import the `great_expectations` module is:

```python name="tutorials/quickstart/quickstart.py import_gx"
```

#### 1.3 Instantiate a Data Context

We will get a `DataContext` object with the following code:

```python name="tutorials/quickstart/quickstart.py get_context"
```

The Data Context will provide you with access to a variety of utility and convenience methods.  It is the entry point for using the GX Python API.

### 2. Connect to data

For the purpose of this guide, we will connect to `.csv` data stored in our GitHub repo:


```python name="tutorials/quickstart/quickstart.py connect_to_data"
```

The above code uses our Data Context's default Datasource for Pandas to access the `.csv` data in the file at the provided `path`.

### 3. Create Expectations

When we read our `.csv` data, we got a Validator instance back.  A Validator is a robust object capable of storing Expectations about the data it is associated with, as well as performing introspections on that data.  

In this guide, we will define two Expectations, one based on our domain knowledge (knowing that the `pickup_datetime` should not be null), and one by using GX to detect the range of values in the `passenger_count` column (using `auto=True`).

The code we will use for this is:

```python name="tutorials/quickstart/quickstart.py create_expectation"
```

With the Expectation defined above, we are stating that we _expect_ the column `pickup_datetime` to always be populated.  That is: none of the column's values should be null.

In the future, you may define numerous Expectations about a Validator's associated data by calling multiple methods that follow the `validator.expect_*` syntax.


### 4. Validate data

#### 4.1 Execute your defined Expectations

Now that we have defined our Expectations it is time for GX to introspect our data and see if it corresponds to what we told GX to expect.  To do this, we define a Checkpoint (which will allow us to repeat the Validation in the future).

```python name="tutorials/quickstart/quickstart.py create_checkpoint"
```
Once we have created the Checkpoint, we will run it and get back the results from our Validation.

```python name="tutorials/quickstart/quickstart.py run_checkpoint"
```

#### 4.2 Review your results

Great Expectations provides a friendly, human-readable way to view the results of Validations: Data Docs.  Our Checkpoint will have automatically compiled new Data Docs to include the results of the Validation we ran, so we can view them immediately:

```python name="tutorials/quickstart/quickstart.py view_results"
```


## Next Steps 

Now that you've seen how easy it is to implement the GX workflow, it is time to customize that workflow to suit your specific use cases! To help with this we have prepared some more detailed guides on setting up and installing GX and getting an initial Data Context that are tailored to specific environments and resources.

:::info Great Expectations Cloud

This guide has introduced you to the open source Python and command line use of Great Expectations.  GX also offers an online interface, currently in Beta.  The GX Cloud interface significantly simplifies collaboration between data teams and domain experts.

If you are interested in GX Cloud, you should join the GX Cloud Beta.  During this program limited seats are available, but signing up will keep you informed of the product's process.

**[Sign up for the GX Cloud Beta!](https://greatexpectations.io/cloud)**
:::

### Installing GX for specific environments and source data systems

<SetupAndInstallGx />

### Initializing, instantiating, and saving a Data Context

<DataContextInitializeInstantiateSave />

