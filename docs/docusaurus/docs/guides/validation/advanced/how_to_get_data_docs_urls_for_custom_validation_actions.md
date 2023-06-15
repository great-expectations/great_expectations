---
title: Use Data Docs URLs in custom Validation Actions
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

If you would like to a custom Validation Action that includes a link to <TechnicalTag tag="data_docs" text="Data Docs"/>,
you can access the Data Docs URL for the respective <TechnicalTag tag="validation_result" text="Validation Results"/> page from your Validation Results following a <TechnicalTag tag="checkpoint" text="Checkpoint"/> run following the steps below.

This will work to get the URLs for any type of Data Docs site setup. For example, S3 or a local setup.

## Prerequisites

<Prerequisites>

  - [An Expectation Suite for Validation](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
  - [Familiarity with Validation Actions](../../../terms/action.md)

</Prerequisites>

### 1. Instantiate

First, within the `_run` method of your custom Validation Action, instantiate an empty `dict` to hold your sites:

```python name="great_expectations/checkpoint/actions.py empty dict"
```

### 2. Acquire

Next, call `get_docs_sites_urls` to get the urls for all the suites processed by this Checkpoint:

```python name="great_expectations/checkpoint/actions.py get_docs_sites_urls"
```


### 3. Iterate

The above step returns a list of dictionaries containing the relevant information. Now, we need to iterate through the entries to build the object we want:

```python name="great_expectations/checkpoint/actions.py iterate"
```

### 4. Utilize

You can now include the urls contained within the `data_docs_validation_results` dictionary as links in your custom notifications, for example in an email, Slack, or OpsGenie notification, which will allow users to jump straight to the relevant Validation Results page.

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just accessed Data Docs URLs for use in custom Validation Actions! &#127881;
</b></p>
</div>

:::note
For more on Validation Actions, see our current [guides on Validation Actions here.](https://docs.greatexpectations.io/docs/guides/validation/#actions)

To view the full script used in this page, and see this process in action, see it on GitHub:
- [actions.py](https://github.com/great-expectations/great_expectations/blob/26e855271092fe365c62fc4934e6713529c8989d/great_expectations/checkpoint/actions.py#L1085-L1096)
:::
