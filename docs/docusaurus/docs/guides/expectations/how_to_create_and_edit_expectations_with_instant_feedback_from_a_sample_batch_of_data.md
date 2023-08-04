---
title: Create Expectations interactively with Python
tag: [how-to, getting started]
description: Create Expectations with a Python interpreter or a script and then use interactive feedback to validate them with batch data.
keywords: [Expectations, Interactive Mode, Interactive]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import PrerequisiteQuickstartGuideComplete from '/docs/components/prerequisites/_quickstart_completed.mdx'
import IfYouStillNeedToSetupGX from '/docs/components/prerequisites/_if_you_still_need_to_setup_gx.md'
import DataContextInitializeQuickOrFilesystem from '/docs/components/setup/link_lists/_data_context_initialize_quick_or_filesystem.mdx'
import ConnectingToDataFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_data_fluently.md'

To Validate data we must first define a set of Expectations for that data to be Validated against.  In this guide, you'll learn how to create Expectations and interactively edit them with feedback from Validating each against a Batch of data. Validating your Expectations as you define them allows you to quickly determine if the Expectations are suitable for our data, and identify where changes might be necessary.

:::info Does this process edit my data?
No.  The interactive method used to create and edit Expectations does not edit or alter the Batch data.
:::

## Prerequisites

<Prerequisites>

- Great Expectations installed in a Python environment
- A Filesystem Data Context for your Expectations
- Created a Data Source from which to request a Batch of data for introspection

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

### If you haven't created a Data Source

</summary>

See one of the following guides:

<ConnectingToDataFluently />

</details>

## Steps

### 1. Import the Great Expectations module and instantiate a Data Context

For this guide we will be working with Python code in a Jupyter Notebook. Jupyter is included with GX and lets us easily edit code and immediately see the results of our changes.

Run the following code to import Great Expectations and instantiate a Data Context:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py imports and data context"
```

:::info Data Contexts and persisting data

If you're using an Ephemeral Data Context, your configurations will not persist beyond the current Python session.  However, if you're using a Filesystem or Cloud Data Context, they do persist.  The `get_context()` method returns the first Cloud or Filesystem Data Context it can find.  If a Cloud or Filesystem Data Context has not be configured or cannot be found, it provides an Ephemeral Data Context.  For more information about the `get_context()` method, see [How to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

:::

### 2. Use an existing Data Asset to create a Batch Request

Add the following method to retrieve a previously configured Data Asset from the Data Context you initialized and create a Batch Request to identify the Batch of data that you'll use to validate your Expectations:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_data_asset_and_build_batch_request"
```

:::info Limit the Batches returned by a Batch Request

You can provide a dictionary as the `options` parameter of `build_batch_request()` to limit the Batches returned by a Batch Request.  If you leave the `options` parameter empty, your Batch Request will include all the Batches configured in the corresponding Data Asset.  For more information about Batch Requests, see [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset).

:::

### 3. Create a Validator

When you use a Validator to interactively create your Expectations, the Validator needs two parameters. One parameter identifies the Batch that contains the data that is used to Validate the Expectations. The second parameter provides a name for the combined list of Expectations you create.

:::info Working outside a Jupyter Notebook

If you're using a Jupyter Notebook you'll automatically see the results of the code you run in a new cell when you run the code. If you're using a different interpreter, you might need to explicitly print these results to view them. For example:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py inspect_data_no_jupyter"
```

:::

1. Optional. Run the following command if you haven't created an Expectation Suite:

    ```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py create_expectation_suite"
    ```

2. Run the following command to create a Validator:

    ```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_validator_and_inspect_data"
    ```

### 4. Use the Validator to create and run an Expectation

The Validator provides access to all the available Expectations as methods.  When an `expect_*()` method is run from the Validator, the Validator adds the specified Expectation to an Expectation Suite (or edits an existing Expectation in the Expectation Suite, if applicable) in its configuration, and then the specified Expectation is run against the data that was provided when the Validator was initialized with a Batch Request.

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation"
```

Since we are working in a Jupyter Notebook, the results of the Validation are printed after we run an `expect_*()` method.  We can examine those results to determine if the Expectation needs to be edited.

:::info Working outside a Jupyter Notebook
If you are not working in a Jupyter Notebook you may need to explicitly print your results:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py interactive_validation_no_jupyter"
```

:::

### 5. (Optional) Repeat step 4 to edit Expectations or create additional Expectations

If you choose to edit an Expectation after you've viewed the Validation Results that were returned when it was created, you can do so by running the `validator.expect_*()` method with different parameters than you supplied previously.  You can also have the Validator run an entirely different `expect_*()` method and create additional Expectations.  All the Expectations that you create are stored in a list in the Validator's in-memory configuration.

:::tip What if I want to use the same Expectation more than once?

GX takes into account certain parameters when determining if an Expectation is being added to the list or if an existing Expectation should be edited.  For example, if you are created an Expectation with a method such as `expect_column_*()` you could later edit it by providing the same `column` parameter when running the `expect_column_*()` method a second time, and different values for any other parameters.  However, if you ran the same `expect_column_*()` method and provided a different `column` parameter, you will create an additional instance of the Expectation for the new `column` value, rather than overwrite the Expectation you defined with the first `column` value.

:::

### 6. (Optional) Save your Expectations for future use

The Expectations you create with the interactive method are saved in an Expectation Suite on the Validator object.  Validators do not persist outside the current Python session and for this reason these Expectations will not be kept unless you save them to your Data Context.  This can be ideal if you are using a Validator for quick data validation and exploration, but in most cases you'll want to reuse your newly created Expectation Suite in future Python sessions.

To keep your Expectations for future use, you save them to your Data Context.  A Filesystem or Cloud Data Context persists outside the current Python session, so saving the Expectation Suite in your Data Context's Expectations Store ensures you can access it in the future:

```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py save_expectation_suite"
```

:::caution Ephemeral Data Contexts and persistence

Ephemeral Data Contexts don't persist beyond the current Python session.  If you're working with an Ephemeral Data Context, you'll need to convert it to a Filesystem Data Context using the Data Context's `convert_to_file_context()` method.  Otherwise, your saved configurations won't be available in future Python sessions as the Data Context itself is no longer available.

:::

## Next steps

Now that you have created and saved an Expectation Suite, you can [Validate your data](/docs/guides/validation/validate_data_overview).