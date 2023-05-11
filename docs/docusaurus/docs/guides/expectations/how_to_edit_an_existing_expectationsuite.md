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

To Validate data we must first define a set of Expectations for that data to be Validated against.  In this guide, you'll learn how to create Expectations and interactively edit them with feedback from Validating each against a Batch of data. Validating your Expectations as you define them allows you to quickly determine if the Expectations are suitable for our data, and identify where changes might be necessary.

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

For this guide we will be working with Python code in a Jupyter Notebook. Jupyter is included with GX and lets us easily edit code and immediately see the results of our changes.

Run the following code to import Great Expectations and instantiate a Data Context:

<!-- ```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py imports and data context"
``` -->

:::info Data Contexts and persisting data

If you're using an Ephemeral Data Context, your configurations will not persist beyond the current Python session.  However, if you're using a Filesystem or Cloud Data Context, they do persist.  The `get_context()` method returns the first Cloud or Filesystem Data Context it can find.  If a Cloud or Filesystem Data Context has not be configured or cannot be found, it provides an Ephemeral Data Context.  For more information about the `get_context()` method, see [How to quickly instantiate a Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_quickly_instantiate_a_data_context).

:::

### 2. Use an existing Data Asset to create a Batch Request

Add the following method to retrieve a previously configured Data Asset from the Data Context you initialized and create a Batch Request to identify the Batch of data that you'll use to validate your Expectations:

<!-- ```python name="tests/integration/docusaurus/validation/validator/how_to_create_and_edit_expectations_with_instant_feedback_fluent.py get_data_asset_and_build_batch_request"
``` -->

:::info Limit the Batches returned by a Batch Request

You can provide a dictionary as the `options` parameter of `build_batch_request()` to limit the Batches returned by a Batch Request.  If you leave the `options` parameter empty, your Batch Request will include all the Batches configured in the corresponding Data Asset.  For more information about Batch Requests, see [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset).

:::

:::caution Batch Requests and Datasources built with the advanced block-config method

If you are working with a Datasource that was created using the advanced block-config method, you will need to build your Batch Request differently than was demonstrated earlier.  For more information, please see our guide on [How to get one or more batches from a Datasource configured with the block-config method](/docs/guides/connecting_to_your_data/how_to_get_one_or_more_batches_of_data_from_a_configured_datasource).

:::

### 3. (Optional) Use the OnboardingDataAssistant to populate an ExpectationSuite

Next use an Onboarding DataAssistant to populate an ExpectationSuite.

* Description of Onboarding DataAssistant and linke
* link to onboarding DataAssitant work 

* Create ExpectationSuite
* Run the Assistant 


### 4. View the Expectations in the Expectation Suite
* There are a few ways to do this, with one of htem being print with ()
* this way you will be able to see your 
* they will also be able to see if any of the have values that you dont want 

### 5. Create ExpectationConfiguration from the one that you want 

### 6. Add/Update With the COnfiguration 
* Check that it isn'tthere using the find it and see that it isn't there. 

### 7. Remove Configuration 
* You can also remove a configuration ify ou dont want. 
* * there are also remove all from a domain. 

### 8. Save ExpectationSuite to Context
* When you are done with the the work, then you save yourExpectationSuite DAtaContext
* this will save it to whatever store you have configured.

## for more information .
* Please look at the API docs: which you can find here. 