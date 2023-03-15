---
title: How to request data from a Data Asset
tag: [how-to, connect to data]
description: A technical guide demonstrating how to request data from a Data Asset.
keywords: [Great Expectations, Data Asset, Batch Request, fluent configuration method]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

In this guide we will demonstrate the process of requesting data from a Datasource that has been defined using the `context.sources.add_*` method.

If you are using a Datasource that was created by using the block-config method of directly building the Datasource's yaml or Python dictionary configuration, please see:
- How to request data from a block-config style Datasource

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX
- A Datasource with a configured Data Asset
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

```python title="Python code"
import great_expectations as gx

context = gx.get_context()
```

### 2. Retrieve your Data Asset

If you already have an instance of your Data Asset stored in a Python variable, you do not need to retrieve it again.  If you do not, you can instantiate a previously defined Datasource with your Data Context's `get_datasource(...)` method.  Likewise, a Datasource's `get_asset(...)` method will instantiate a previously defined Data Asset.

In this example we will use a previously defined Datasource named `MyDatasource` and a previously defined Data Asset named `MyTaxiDataAsset`.

```python title="Python code
my_asset = context.get_datasource("MyDatasource").get_asset("MyTaxiDataAsset")
```

### 3. (Optional) Build an `options` dictionary for your Batch Request

An `options` dictionary can be used to limit the Batches returned by a Batch Request.  Omitting the `options` dictionary or using an `options` dictionary where all values are set to `None` will result in all available Batches being returned.

The structure of the `options` dictionary will depend on the type of Data Asset being used.  A template of the dictionary can be created with the Data Asset's `batch_request_options_template()` method.  This method will return an `options` dictionary with the correct structure for the Data Asset, but with all the options set to `None`.

```python title="Python code"
request_options = my_asset.batch_request_options_template()
print(request_options)
```

Once you have your `options` dictionary you can overwrite the values of individual elements within it to specify the Batch or Batches your Batch Request should return.

The template will show all the valid keys you can use to limit the returned Batches.


In a SQL Data Asset the `options` keys will 

### 4. Build your Batch Request

We will use the `build_batch_request(...)` method of our Data Asset and the `request_options` that were defined earlier to generate a Batch Request.

```python title="Python code"
my_batch_request = my_asset.build_batch_request(options=request_options)
```

### 5. Verify that the correct Batches were returned

The `get_batch_list_from_batch_request(...)` method will return a list of the Batches a given Batch Request returns.  Because Batch definitions are quite verbose, it is easiest to determine what data the Batch Request will return by printing just the `batch_spec` of each Batch.

```python title="Python code"
batches = datasource.get_batch_list_from_batch_request(my_batch_request)
for batch in batches:
    print(batch.batch_spec)
```

## Next steps

Now that you have a retrieved data from a Data Asset, you may be interested in creating Expectations about your data:
- How to create Expectations while interactively evaluating a set of data
- How to use a Data Assistant to evaluate data


