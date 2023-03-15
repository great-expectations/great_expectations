---
title: Quickstart with GX
tag: [tutorial, getting started]
---
# Quickstart with Great Expectations

import Prerequisites from '/docs/components/_prerequisites.jsx'
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import SetupAndInstallForFilesystemData from '/docs/components/setup/link_lists/_setup_and_install_for_filesystem_data.md'
import SetupAndInstallForHostedData from '/docs/components/setup/link_lists/_setup_and_install_for_hosted_data.md'
import SetupAndInstallForCloudData from '/docs/components/setup/link_lists/_setup_and_install_for_cloud_data.md'

## Introduction

Few things are as daunting as taking your first steps with a new piece of software.  This guide will introduce you to GX and demonstrate the ease with which you can implement the basic GX workflow. We will walk you through the entire process of installing GX, connecting to some sample data, building your first Expectation based off of an initial Batch of that data, validating your data with that Expectation, and finally reviewing the results of your validation.

Once you have completed this guide you will have a foundation in the basics of using GX.  In the future you will be able to adapt GX to suit your specific needs by customizing the execution of the individual steps you will learn here.

## Prerequisites

<Prerequisites requirePython = {true} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- The ability to install Python packages with pip
- A working internet browser
- A passion for data quality

</Prerequisites> 

## Overview

With GX you can get up and running with just a few lines of code.  The full process you'll be using will look like:

```bash title="Terminal input"
pip install great_expectations
```

```python title="Python code"
import great_expectations as gx

# Set up
context = gx.get_context()

# Connect to data
validator = context.datasources.pandas_default.read_csv(
    filepath_or_buffer="https://raw.githubusercontent.com/great_expectations/taxi_data.csv"
)

# Create Expectations
validator.expect_column_values_to_not_be_null("pickup_datetime")

# Validate data
checkpoint = SimpleCheckpoint( 
    name=f"{csv_asset.name}_{expectation_suite_name}",
    context=context,
    validator=validator,
)
checkpoint_result = checkpoint.run()

# View results
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)

# Save the Data Context for future use
context.convert_to_file_context()
```

That's not a lot of code, is it?  In the following steps we'll break down exactly what is happening here so that you can follow along and perform a Validation yourself.


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

```python title="Python code"
import great_expectations as gx
```

#### 1.3 Instantiate a Data Context

We will get a `DataContext` object with the following code:

```python title="Python code"
context = gx.get_context()
```

The Data Context will provide you with access to a variety of utility and convenience methods.  It is the entry point for using the GX Python API.

### 2. Connect to data

For the purpose of this guide, we will connect to `.csv` data stored in our GitHub repo:

```python title="Python code"
validator = context.datasources.pandas_default.read_csv(
    filepath_or_buffer="https://raw.githubusercontent.com/great_expectations/taxi_data.csv"
)
```

The above code uses our Data Context's default Datasource for Pandas to access the `.csv` data in the file at the provided `path`.

### 3. Create Expectations

When we read our `.csv` data, we got a Validator instance back.  A Validator is a robust object capable of storing Expectations about the data it is associated with, as well as performing introspections on that data.  

In this guide, we will define a single Expectation based on our domain knowledge (that is: based on what we know _should_ be true about our data, without looking at the actual state of the data).

The code we will use for this is:

```python title="Python code"
validator.expect_column_values_to_not_be_null("pickup_datetime")
```

With the Expectation defined above, we are stating that we _expect_ the column `pickup_datetime` to always be populated.  That is: none of the column's values should be null.

In the future, you may define numerous Expectations about a Validator's associated data by calling multiple methods that follow the `validator.expect_*` syntax.


### 4. Validate data

#### 4.1 Execute your defined Expectations

Now that we have defined our Expectations it is time for GX to introspect our data and see if it corresponds to what we told GX to expect.  To do this, we define a Checkpoint (which will allow us to repeat the Validation in the future).

```python title="Python code"
checkpoint = SimpleCheckpoint( 
    name="my_quickstart_checkpoint",
    context=context,
    validator=validator,
)
```
Once we have created the Checkpoint, we will run it and get back the results from our Validation.

```python title="Python code"
checkpoint_result = checkpoint.run()
```

#### 4.2 Review your results

Great Expectations provides a friendly, human-readable way to view the results of Validations: Data Docs.  Our Checkpoint will have automatically compiled new Data Docs to include the results of the Validation we ran, so we can view them immediately:

```python title="Python code"
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)
```

#### 4.3 Save the Data Context for future use
Because we did not previously initialize a Filesystem Data Context or specify a path at which to create one, the Data Context we recieved from `gx.get_context()` was a temporary, in-memory Ephemeral Data Context.  To save this Data Context for future use, we will convert it to a Filesystem Data Context:

```python title="Python code"
context = context.convert_to_file_context()
```

You can provide the path to a specific folder when you convert your Ephemeral Data Context to a Filesystem Data Context.  If you do, your Filesystem Data Context will be initialized at that location.  If you do not, your new Filesystem Data Context will be initialized in the folder that your script is executed in.

## Next Steps 

Now that you've seen how easy it is to implement the GX workflow, it is time to customize that workflow to suit your specific use cases! To help with this we have prepared some more detailed installation and setting up guides tailored to specific environments and resources.

For more details on installing GX for use with local filesystems, please see:

<SetupAndInstallForFilesystemData />

For guides on installing GX for use with cloud storage systems, please reference:

<SetupAndInstallForCloudData />

For information on installing GX for use with SQL databases, see:

<SetupAndInstallForSqlData />

And for instructions on installing GX for use with hosted data systems, read:

<SetupAndInstallForHostedData />
