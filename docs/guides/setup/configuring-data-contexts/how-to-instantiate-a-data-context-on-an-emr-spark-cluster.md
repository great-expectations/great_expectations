---
title: How to instantiate a Data Context on an EMR Spark cluster
---

This guide will help you instantiate a Data Context on an EMR Spark cluster.

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations command line interface (CLI).

:::tip Prerequisites
This how-to guide assumes you have already:

Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration.
:::

##Steps

1. Install Great Expectations on your EMR Spark cluster.

Copy this code snippet into a cell in your EMR Spark notebook and run it:

````console
sc.install_pypi_package("great_expectations")
````

2. Configure a Data Context in code.

Follow the steps for creating an in-code Data Context in How to instantiate a Data Context without a yml file

The snippet at the end of the guide shows Python code that instantiates and configures a Data Context in code for an EMR Spark cluster. Copy this snippet into a cell in your EMR Spark notebook or use the other examples to customize your configuration.

## Test your configuration.

Execute the cell with the snippet above.

Then copy this code snippet into a cell in your EMR Spark notebook, run it and verify that no error is displayed:

````console
context.list_datasources()
````

