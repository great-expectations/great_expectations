---
title: How to use self-initializing Expectations
---

import Prerequisites from '../../guides/connecting_to_your_data/components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

This guide will walk you through the process of using a self-initializing <TechnicalTag tag="expectation" text="Expectations" /> to automate parameter estimation when you are creating Expectations interactively by using a <TechnicalTag tag="batch" text="Batch" /> or Batches that have been loaded into a <TechnicalTag tag="validator" text="Validator" />.

<Prerequisites>

- [Configured a Data Context](../../tutorials/getting_started/tutorial_setup.md).
- [Configured a Data Source](../../tutorials/getting_started/tutorial_connect_to_data.md)
- [An understanding of how to configure a BatchRequest](../../guides/connecting_to_your_data/how_to_get_a_batch_of_data_from_a_configured_datasource.md)
- [An understanding of how to create and edit expectations with instant feedback from a sample batch of data](./how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data.md)

</Prerequisites>

## Steps

### 1. Determine if your Expectation is self-initializing

Not all Expectations are self-initializng.  In order to be a self-initializing Expectation, an Expectation must have parameters that can be estimated.  As an example: `ExpectColumnToExist` only takes in a `Domain` (which is the column name) and checks whether the column name is in the list of names in the table's metadata.  This would be an example of an Expectation that would not work under the self-initializing framework.

An example of Expectations that would work under the self-initializing framework would be the ones that have numeric ranges, like `ExpectColumnMeanToBeBetween`, `ExpectColumnMaxToBeBetween`, and `ExpectColumnSumToBeBetween`.

