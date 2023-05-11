---
title: How to use a Custom Expectation
---
import Prerequisites from '../creating_custom_expectations/components/prerequisites.jsx'
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Custom <TechnicalTag tag="expectation" text="Expectations"/> are extensions to the core functionality of Great Expectations. Many Custom Expectations may be fit for a very specific purpose,
or be at lower levels of stability and feature maturity than the core library.

As such, they are not available for use from the core library, and require registration and import to become available.

This guide will walk you through the process of utilizing Custom Expectations, whether they were built by you or came from the Great Expectations Experimental Library.

## Prerequisites

<Prerequisites>

- A <TechnicalTag tag="custom_expectation" text="Custom Expectation"/> ***or*** identified a Custom Expectation for use from the [Great Expectations Experimental Library](https://github.com/great-expectations/great_expectations/tree/develop/contrib/experimental/great_expectations_experimental/expectations)

</Prerequisites>

There are slight differences when working with custom expectations or those contributed to GX.

## Installation

<Tabs
  groupId="expectation-type"
  defaultValue='custom-expectations'
  values={[
  {label: 'Custom Expectations You\'ve Built', value:'custom-expectations'},
  {label: 'Custom Expectations Contributed To Great Expectations', value:'contrib-expectations'},
  ]}>

<TabItem value="custom-expectations">

If you're using a Custom Expectation you've built, you'll need to place it in the `great_expectations/plugins/expectations` folder of your Great Expectations deployment.

When you

</TabItem>

<TabItem value="contrib-expectations">

If you're using a Custom Expectation that is coming from the `Great Expectations Experimental` library, it will need to either be imported from there directly. To do this, we'll first need to `pip install great_expectations_experimental`.

</TabItem>
</Tabs>

## Usage

When you instantiate your <TechnicalTag tag="data_context" text="Data Context"/>, it will automatically make all plugins in the directory available for use,
allowing you to import your Custom Expectation from that directory whenever and wherever it will be used.
This import will be needed when an <TechnicalTag tag="expectation_suite" text="Expectation Suite"/> is created, *and* when a <TechnicalTag tag="checkpoint" text="Checkpoint"/> is defined and run.

<Tabs
  groupId="expectation-type"
  defaultValue='custom-expectations'
  values={[
  {label: 'Custom Expectations You\'ve Built', value:'custom-expectations'},
  {label: 'Custom Expectations Contributed To Great Expectations', value:'contrib-expectations'},
  ]}>

<TabItem value="custom-expectations">

```python
from expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
# ...
validator.expect_column_values_to_be_alphabetical(column="test")
```

</TabItem>

<TabItem value="contrib-expectations">

```python
from great_expectations_experimental.expectations.expect_column_values_to_be_alphabetical import ExpectColumnValuesToBeAlphabetical
# ...
validator.expect_column_values_to_be_alphabetical(column="test")
```

</TabItem>
</Tabs>

<div style={{"text-align":"center"}}>
<p style={{"color":"#8784FF","font-size":"1.4em"}}><b>
Congratulations!<br/>&#127881; You've just used a custom expectation! &#127881;
</b></p>
</div>
