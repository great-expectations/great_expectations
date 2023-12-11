---
title: Add Validation data or Expectation Suites to a Checkpoint
---

import Prerequisites from '/docs/oss/guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/oss/term_tags/_tag.mdx';

Add validation data or <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> to an existing <TechnicalTag tag="checkpoint" text="Checkpoint" /> to aggregate individual validations across Expectation Suites or <TechnicalTag tag="datasource" text="Data Sources" /> into a single Checkpoint. You can also use this process to <TechnicalTag tag="validation" text="Validate" /> multiple source files before and after their ingestion into your data lake.

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/oss/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context).
- [An Expectations Suite](/docs/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](./how_to_create_a_new_checkpoint.md).

</Prerequisites>

## Add an Expectation Suite to the Checkpoint

To add a second Expectation Suite (in the following example, `users.error`) to your Checkpoint configuration, you update the Validations in your Checkpoint.  The configuration appears similar to this example:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_expectation_suite"
```

## Add Validation data to the Checkpoint

In the previous example, you added a Validation and it was paired with the same Batch as the original Expectation Suite.  However, you can also specify different <TechnicalTag tag="batch_request" text="Batch Requests" /> when you add an Expectation Suite.  Adding multiple Validations with different Expectation Suites and specific <TechnicalTag tag="action" text="Actions" /> is shown in the following example:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_validation"
```

In this Checkpoint configuration, the Expectation Suite `users.warning` runs against the `batch_request` and the results are processed by the Actions specified in the `action_list`. Similarly, the Expectation Suite `users.error` runs against the `batch_request` and the results processed by the actions specified in the `action_list`. In addition, the Expectation Suite `users.delivery` runs against the `batch_request` and the results are processed by the actions in the Validation `action_list` and its `action_list`.

For additional Checkpoint configuration information, see [Manage Checkpoints](./checkpoint_lp.md).
