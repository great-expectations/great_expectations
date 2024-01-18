---
sidebar_label: 'Manage Expectations'
title: 'Manage Expectations'
description: Create and manage individual Expectations in Python with GX Core.
---

An Expectation is a verifiable assertion about your data. Expectations make implicit assumptions about your data explicit, and they provide a flexible, declarative language for describing expected behavior. They can help you better understand your data and help you improve data quality. 

## Prerequisites

This guide assumes you have:

- Installed Python.
- Installed the GX Core library.

## Create an Expectation

1. Import the `expectations` module from the GX Core library.
   
  ```python
import great_expectations.expectations as gxe
  ```

2. Initialize an Expectation class with the required parameters for that Expectation.

  ```python
expectation = gxe.ExpectColumnValuesToBeInSet(column="passenger_count", value_set=[1, 2, 3, 4, 5])
)
  ```

  The specific parameters provided when initializing an Expectation depend on the Expectation class.  You can view an Expectation and the parameters it takes in the Expectation Gallery.

## Test an Expectation

1. Retrieve a Batch of data to test the Expectation against.

  :::note
  The rest of this section assumes the variable `batch` is your Batch of data.
  :::

2. Get the Expectation to test.  This could be a [newly created](#create-an-expectation) Expectation, an Expectation [retrieved from an Expectation Suite](/core/expectations/manage_expectation_suites#get-a-specific-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.
  
  :::note
  The rest of this section assumes the variable `expectation` is the Expectation you wish to test.
  :::

3. Validate the Expectation against the Batch.

  ```python
validation_result = batch.validate(expectation)
  ```

4. (Optional) [Modify the Expectation](#modify-an-expectation) and test it again.
 
5. (Optional) [Add the Expectation to an Expectation Suite](/core/expectations/manage_expectation_suites#add-expectations-to-an-expectation-suite).
   
  :::caution 
  Expectations do not persist between Python sessions unless they are saved as part of an Expectation Suite.
  :::

## Modify an Expectation

1. Get the Expectation to modify.  This could be a [newly created](#create-an-expectation) Expectation that you wish to adjust, an Expectation [retrieved from an Expectation Suite](/core/expectations/manage_expectation_suites#get-a-specific-expectation-from-an-expectation-suite), or a pre-existing Expectation from your code.
  
  :::note
  The rest of this section assumes the variable `expectation` is the Expectation you wish to modify.
  :::

2. Modify the Expectation's attributes.
  ```python
expectation.column_name="new_column_name"
  ```

3. (Optional) If the Expectation belongs to an Expectation Suite, save the changes to the Expectation Suite.
  ```python
expectation.save()
  ```

## Next steps

- Customize Expectation classes
- Create Custom SQL Expectations
- Manage Expectation Suites