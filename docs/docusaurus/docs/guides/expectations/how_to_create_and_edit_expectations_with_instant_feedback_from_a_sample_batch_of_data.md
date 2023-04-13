---
title: How to create Expectations interactively in Python
tag: [how-to, getting started]
description: A how-to guide describing how to create Expectations in a Python interpreter or script while interactively receiving feedback by validating them against a Batch of data.
keywords: [Expectations, Interactive Mode, Interactive]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrerequisiteQuickstartGuideComplete from '/docs/components/prerequisites/_quickstart_completed.mdx'
import IfYouStillNeedToSetupGX from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import DataContextInitializeQuickOrFilesystem from '/docs/components/setup/link_lists/_data_context_initialize_quick_or_filesystem.mdx'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'

In order to Validate data, we must first define a set of Expectations for that data to be validated against.  In this guide, we will walk you through processes of interactively creating and editing Expectations while simultaneously validating them against a Batch of data. By validating our Expectations as we define them we can quickly see if the Expectations we are setting are suitable for our data, and adjust them if necessary.

:::tip Does this process edit my data?
Not at all.  We call this process the "Interactive method" of creating and editing Expectations because validating the Expectations as we work requires interacting with a Batch of data.  This data is introspected to see if it fits the defined Expectation, but it is not edited: only examined.
:::

## Prerequisites

<Prerequisites>

- Great Expectations installed in a Python environment
- Initialized a Filesystem Data Context to save your Expectations in
- Created a Datasource from which to request a Batch of data for introspection
- A passion for data quality

</Prerequisites> 

<details>
<summary>

### If you still need to set up Great Expectations...

</summary>

<IfYouStillNeedToSetupGX />

</details>

<details>
<summary>

### If you still need to initialize your Data Context...

</summary>

Please see the appropriate guide from the following:

<DataContextInitializeQuickOrFilesystem />

</details>

<details>
<summary>

### If you still need to create a Datasource...

</summary>

Please reference the appropriate guide from the following:

<ConnectingToDataFluently />

</details>

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

For this guide we will be working with Python code in a Jupyter Notebook. Jupyter is included with GX and provides a very convenient interface that lets us easily edit code and immediately see the result of our changes.

The code to import Great Expectations and instantiate a Data Context is:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py imports and data context"
```

:::info Data Contexts and persisting data

If you are using an Ephemeral Data Context, your configurations will not persist beyond the current Python session.  If you are using a Filesystem or Cloud Data Context, they will.  The `get_context()` method will return the first of a Cloud or Filesystem Data Context if it can find one, but if it can't then it will provide you with an Ephemeral Data Context.  For more information on the `get_context()` method, please see our guide on [how to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

:::

### 2. Use an existing Data Asset to create a Batch Request

From the Data Context we initialized we can access our previously created Datasources and Data Assets.  We will retrieve a previously configured Data Asset, and then create a Batch Request indicating the Batch of data that we intend to Validate our Expectations against:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_data_asset_and_build_batch_request"
```

:::info Limiting the Batches returned by a Batch Request

A Batch Request allows you to limit the Batches returned from a Data Asset by providing a dictionary as the `options` parameter.  If nothing is provided as the `options` parameter, your Batch Request will include all the Batches configured in the Data Asset.  To learn more, please see our guide on [how to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset).

:::

### 3. Create a Validator

We will use a Validator to interactively create our Expectations.  To do this, a Validator needs two parameters: one will indicate the data to run Expectations against, and the other will provide a name for the combined list of Expectations should we decide to save them for future use.

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_validator_and_inspect_data"
```

:::info Working outside a Jupyter Notebook

In a Jupyter Notebook you will automatically see the results of the above code in a new cell when the code is run.  However, if you are working with a different Python interpreter you may need to explicitly print your results:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py inspect_data_no_jupyter"
```

:::

### 4. Use the Validator to create and run an Expectation

The Validator provides access to all of our available Expectations as methods.  By running an `expect_*()` method from the Validator, two things will happen: The Validator will add the specified Expectation to an Expectation Suite (or edit an existing Expectation in the Expectation Suite, if applicable) in its configuration, and the specified Expectation will be run against the data we provided to the Validator with our Batch Request.

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation"
```

Since we are working in a Jupyter Notebook, the results of the Validation will be printed after we run an `expect_*()` method.  We can examine those results to determine if the Expectation needs to be edited (by running it again and providing additional optional parameters, or different parameter values).

:::info Working outside a Jupyter Notebook
As before, if you are not working in a Jupyter Notebook you may need to explicitly print your results:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation_no_jupyter"
```

:::

### 5. (Optional) Repeat step 4 to create additional Expectations or edit existing ones

After reviewing the Validation Results that were returned when we created an Expectation, we can choose to edit that Expectation by running the `validator.expect_*()` method with different parameters than previously.  We can also have the Validator run an entirely different `expect_*()` method and create additional Expectations.  All the Expectations that we create will be stored in a list in the Validator's configuration.

:::tip What if I want to use the same Expectation more than once?

GX takes into account certain parameters when determining if an Expectation is being added to the list or if an existing Expectation should be edited.  For example, if you are using an expectation such as `expect_column_*()` you would edit an existing Expectation by providing the same `column` parameter when repeating the previous steps, and different values for any other parameters.  If you used a different value for the `column` parameter, you will create an additional instance of the Expectation for the new `column` value, rather than overwrite the Expectation you defined with the first `column` value.

:::

### 6. Save your Expectations for future use

The Expectations we have created so far are saved in an Expectation Suite on the Validator object.  Since Validators do not persist outside the current Python session these Expectations will not be kept if we stop now.  This can be ideal if we are using a Validator to do some quick data discovery, but in most cases we will want our newly created Expectation Suite to be available in the future.

To keep our Expectations for future use, we will save them to our Data Context.  A Filesystem or Cloud Data Context will persist outside the current Python session, so saving the Expectation Suite in our Data Context's Expectations Store will ensure we can access it in the future:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py save_expectation_suite"
```

:::caution Ephemeral Data Contexts and persistence

Ephemeral Data Contexts do not persist beyond the current Python session.  If you are working with an Ephemeral Data Context, you will need to convert it to a Filesystem Data Context using the Data Context's `convert_to_file_context()` method.  Otherwise, your saved configurations will not be available in future Python sessions as the Data Context itself will no longer be available.

:::

## Next steps

Now that you have created and saved an Expectation Suite, you are ready to begin [Validating your data](/docs/guides/validation/validate_data_overview).