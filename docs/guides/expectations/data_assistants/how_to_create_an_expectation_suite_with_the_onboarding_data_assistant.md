---
title: How to create an Expectation Suite with the Onboarding Data Assistant
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide demonstrates how to use the Onboarding Data Assistant to Profile your data and automate the generation of an
Expectation Suite, which you can then adjust to be suited for your specific needs.

:::caution

Data Assistants are currently an experimental feature and this workflow may be subject to change.

:::

:::note

This process mirrors that of the Jupyter Notebook that is created when you run the following <TechnicalTag tag="cli" text="CLI" /> command:

```terminal
great_expectations suite new --profile onboarding-data-assistant
```
:::

<Prerequisites>

- A [configured Data Context](../../../tutorials/getting_started/tutorial_setup.md).
- The knowledge to [configure and save a Datasource](../../connecting_to_your_data/connect_to_data_overview.md).
- The knowledge to [configure and save a Batch Request](../../connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.md).

</Prerequisites>

## Steps

### 1. Prepare your Batch Request

Data Assistants excel at automating the Profiling process across multiple batches.  Therefore, for this guide you will
 be using a Batch Request that covers multiple Batches.  The Datasource that our Batch Request queries will consist of
 the New York taxi trip data.

This is the configuration that you will use for your Datasource:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L30-L48
```

And this is the configuration that you will use for your Batch Request:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L79-L84
```

### 2. Prepare a new Expectation Suite

Preparing a new Expectation Suite is done with the Data Context's `.create_expectation_suite(...)` method, as seen in
this code example:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L69-L73
```

### 3. Run the Onboarding Data Assistant

Running a Data Assistant is as simple as calling the `run(...)` method for the appropriate assistant.  

That said, there are numerous parameters available for the `run(...)` method of the Onboarding Data Assistant.  For
 instance, the `exclude_column_names` parameter allows you to provide a list columns that should not be Profiled.  

For this guide, you will exclude the following columns:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L90-L109
```

The following code shows how to run the Onboarding Assistant.  In this code block, `context` is an instance of your Data
 Context.  All the parameters available to the Onboarding Data Assistant's `run(...)` method are listed in this example,
 but those that are not used in this guide are commented out.

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L113-L154
```

### 4. Save your Expectation Suite

Once you have executed the Onboarding Data Assistant's `run(...)` method and generated Expectations for your data, you
 need to load them into your Expectation Suite and save them.  You will do this by using a Validator:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L160-L163
```

Once you have your Validator, you can load the Onboarding Data Assistant's Expectations into it with the following
 command:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L169-L171
```

And once the Expectation Suite has been populated in the Validator, you can save it like so:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L175
```

### 5. Test your Expectation Suite with a `SimpleCheckpoint`

To verify that your Expectation Suite is working, you can use a `SimpleCheckpoint`.  First, you will configure one to
 operate with the Expectation Suite and Batch Request that you have already defined:

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L181-L189
```

Once you have our `SimpleCheckpoint`'s configuration defined, you can instantiate a `SimpleCheckpoint` and run it.  You
 will check the `"success"` key of the `SimpleCheckpoint`'s results to verify that your Expectation Suite worked.

```python file=../../../../tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py#L193-L198
```

### 6. (Optional) Edit your Expectation Suite, save, and test again.

The Onboarding Data Assistant will create as many applicable Expectations as it can for the permitted columns.  This
 provides a solid base for analizing your data, but may exceed your needs.  It is also possible that you may possess
 some domain knowledge that is not reflected in the data that was sampled for the Profiling process.  In either of these
 (or any other) cases, you can edit your Expectation Suite to more closely suite your needs.

To edit an existing Expectation Suite (such as the one that you just created and saved with the Onboarding Data
 Assistant) you need only execute the following console command:

```markdown title="Terminal command"
great_expectations suite edit NAME_OF_YOUR_SUITE_HERE
```

This will open a Jupyter Notebook that will permit you to review, edit, and save changes to the specified Expectation
 Suite.

## Additional Information

:::note Example Code

To view the full script used for example code on this page, see it on GitHub:
- [how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py](https://github.com/great-expectations/tests/integration/docusaurus/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant.py)

:::