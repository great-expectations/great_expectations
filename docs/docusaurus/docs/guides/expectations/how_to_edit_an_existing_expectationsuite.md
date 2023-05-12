---
title: How to Edit an ExpecationSuite in Python
tag: [how-to, getting started]
description: Create and ExpectationsSuite with a DataAssistant, and examine, modify specific Expectations in the Suite.
keywords: [Expectations, ExpectationsSuite]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrerequisiteQuickstartGuideComplete from '/docs/components/prerequisites/_quickstart_completed.mdx'
import IfYouStillNeedToSetupGX from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import DataContextInitializeQuickOrFilesystem from '/docs/components/setup/link_lists/_data_context_initialize_quick_or_filesystem.mdx'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'

To Validate data we must first define a set of Expectations for that data to be Validated against.  In this guide, you'll learn how to create Expectations and interactively edit them Validating each against a Batch of data. Validating your Expectations as you define them allows you to quickly determine if the Expectations are suitable for our data, and identify where changes might be necessary.

:::info Does this process edit my data?
No.  The interactive method used to create and edit Expectations does not edit or alter the Batch data.
:::

## Prerequisites

<Prerequisites>

- Great Expectations installed in a Python environment
- A Filesystem Data Context for your Expectations
- Created a Datasource from which to request a Batch of data for introspection

</Prerequisites> 

<details>
<summary>

### If you haven't set up Great Expectations

</summary>

<IfYouStillNeedToSetupGX />

</details>

<details>
<summary>

### If you haven't initialized your Data Context

</summary>

See one of the following guides:

<DataContextInitializeQuickOrFilesystem />

</details>

<details>
<summary>

### If you haven't created a Datasource

</summary>

See one of the following guides:

<ConnectingToDataFluently />

</details>

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

The simplest way to create a new Data Context is by using the `create()` method.

From a Notebook or script where you want to deploy Great Expectations run the following command. Here the `full_path_to_project_directory` can be an empty directory where you intend to build your Great Expectations configuration.:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_context"
```

:::info Data Contexts and persisting data

If you're using an Ephemeral Data Context, your configurations will not persist beyond the current Python session.  However, if you're using a Filesystem or Cloud Data Context, they do persist.  The `get_context()` method returns the first Cloud or Filesystem Data Context it can find.  If a Cloud or Filesystem Data Context has not be configured or cannot be found, it provides an Ephemeral Data Context.  For more information about the `get_context()` method, see [How to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

:::

### 2. Create a Validator from Data 

Run the following command to connect to `.csv` data stored in the `great_expectations` GitHub repository:

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite create_validator"
```

### 3. Create Expectations with Validator 

Run the following command to create two Expectations. The first Expectation uses domain knowledge (the `pickup_datetime` shouldn't be null), and the second Expectation uses `auto=True` to detect a range of values in the `passenger_count` column.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite add_2_expectations"
```

As part of the validator work, we will be adding it to an ExpectationSuite, which you can save later. 

### 4. View the Expectations in the Expectation Suite

There are a number of different ways that this can be done, with one way being using the `show_expectations_by_expectation_type()` function, which will use the `prettyprint` function to print the Suite to console in a way that can be easily visualized. 

First load the `ExpectationSuite` from the `Validator`: 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite get_and_show_suite"
```

Now use the `show_expectations_by_expectation_type()` to print the Suite to console or Jupyter Notebook.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite show_expectations"
```


Your output will look something similar to this: 

```bash 
[ { 'expect_column_values_to_be_between': { 'auto': True,
                                            'column': 'passenger_count',
                                            'domain': 'column',
                                            'max_value': 6,
                                            'min_value': 1,
                                            'mostly': 1.0,
                                            'strict_max': False,
                                            'strict_min': False}},
  { 'expect_column_values_to_not_be_null': { 'column': 'pickup_datetime',
                                             'domain': 'column'}}]
```

### 5. Instantiate ExpectationConfiguration 

From the existing Expectation Suite, you will be able to create an ExpecationConfiguration using the configuration that you are interested in. Here is an example configuration of the first Expectation.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_dict_1"
```

Here is the same configuration, but this time as a `ExpectationConfiguration` object.  

It runs in the `passenger_count` column and expects and max and min values to be `6` and `1` respectively. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite example_expectation"
```

### 6. Update Configuration and ExpectationSuite

Let's say that you are interested in adjusting the `max_value` of the Expectation to be `4` instead of `6`. Then you could create a new `ExpectationConfiguration` with the new value: 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite updated_configuration"
```

And update the ExpectationSuite by calling `add_expectation()`. The `add_expectation()` function will perform an 'upsert' into the `ExpectationSuite`, meaning it will add update an existing Expectation if it already exists, or add a new one if it doesn't. 


```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite updated_configuration"
```

You can check that the ExpectationSuite has been correctly updated by eitehr running the `show_expectations_by_expectation_type()` function again, or by running `find_expectation()` and confirming that the expected configuration exists in the suite.  The search will need to be performed with a new `ExpectationConfiguration`, but will not need to inclued all of the detailed `kwarg` values.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite find_configuration"
```

### 7. (Optional) Remove Configuration 

If you would like to remove an ExpectationConfiguration, you can use the `remove_configuration()` function. 

The similar to `find_expectation()`, the `remove_configuration()` function needs to be called with an ExpectationConfiguration.

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite remove_configuration"
```

The output of `show_expectations_by_expectation_type()` should look like this: 
Your output will look something similar to this: 

```bash 
[ 
  { 'expect_column_values_to_not_be_null': { 'column': 'pickup_datetime',
                                             'domain': 'column'}}]
```

### 8. Save ExpectationSuite 

Finally, when you are done editing the `ExpectationSuite`, you can save it to your DataContext, by using the `save_suite()` function. 

```python name="tests/integration/docusaurus/expectations/how_to_edit_an_expectation_suite save_suite"
```

## for more information .
* Please look at the API docs: which you can find here. 
* For other ways