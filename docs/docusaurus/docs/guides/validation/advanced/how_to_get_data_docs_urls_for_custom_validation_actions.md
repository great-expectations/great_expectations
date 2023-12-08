---
title: Use Data Docs URLs in custom Validation Actions
---
import Prerequisites from '../../connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

To create a custom Validation Action that includes a link to the <TechnicalTag tag="data_docs" text="Data Docs"/>,
you get the Data Docs URL for the <TechnicalTag tag="validation_result" text="Validation Results"/> page from your Validation Results after you run a <TechnicalTag tag="checkpoint" text="Checkpoint"/>. This method returns the URLs for any type of Data Docs site setup including S3 or a local setup.

The code used in this topic is available on GitHub here: [actions.py](https://github.com/great-expectations/great_expectations/blob/26e855271092fe365c62fc4934e6713529c8989d/great_expectations/checkpoint/actions.py#L1085-L1096)

## Prerequisites

<Prerequisites>

  - [An Expectation Suite for Validation](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
  - [Familiarity with Validation Actions](../../../terms/action.md)

</Prerequisites>

## Instantiate

First, within the `_run` method of your custom Validation Action, instantiate an empty `dict` to hold your sites:

```python name="great_expectations/checkpoint/actions.py empty dict"
```

## Acquire

Next, call `get_docs_sites_urls` to get the urls for all the suites processed by this Checkpoint:

```python name="great_expectations/checkpoint/actions.py get_docs_sites_urls"
```


## Iterate

The above step returns a list of dictionaries containing the relevant information. Now, we need to iterate through the entries to build the object we want:

```python name="great_expectations/checkpoint/actions.py iterate"
```

## Utilize

You can now include the urls contained within the `data_docs_validation_results` dictionary as links in your custom notifications, for example in an email, Slack, or OpsGenie notification, which will allow users to jump straight to the relevant Validation Results page.

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just accessed Data Docs URLs for use in custom Validation Actions! &#127881;
</b></p>
</div>
