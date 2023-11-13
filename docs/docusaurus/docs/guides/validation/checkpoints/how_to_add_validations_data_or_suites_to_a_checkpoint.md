---
title: Add validation data or Expectation suites to a Checkpoint
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Add validation data or <TechnicalTag tag="expectation_suite" text="Expectation Suites" /> to an existing <TechnicalTag tag="checkpoint" text="Checkpoint" /> to aggregate individual validations across Expectation Suites or <TechnicalTag tag="datasource" text="Data Sources" /> into a single Checkpoint. You can also use this process to <TechnicalTag tag="validation" text="Validate" /> multiple source files before and after their ingestion into your data lake.

**This topic will be updated.**

## Prerequisites

<Prerequisites>

- [A Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context).
- [An Expectations Suite](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data).
- [A Checkpoint](./how_to_create_a_new_checkpoint.md).

</Prerequisites>

## Add an Expectation Suite to the Checkpoint

To add a second Expectation Suite (in this example we add ``users.error``) to your Checkpoint configuration, update the validations in your Checkpoint.  The resulting configuration will look like this:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_expectation_suite"
```

## Add validation data to the Checkpoint

In the previous example, the validation you added with your Expectation Suite was paired with the same Batch of data as the original Expectation Suite.  However, you may also specify different <TechnicalTag tag="batch_request" text="Batch Requests" /> (and thus different Batches of data) when you add an Expectation Suite.  The flexibility of easily adding multiple Validations of Batches of data with different Expectation Suites and specific <TechnicalTag tag="action" text="Actions" /> can be demonstrated using the following example:

```python name="tests/integration/docusaurus/validation/checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.py add_validation"
```

According to this Checkpoint configuration, the Expectation Suite ``users.warning`` is run against the ``batch_request`` with the results processed by the Actions specified in the top-level ``action_list``. Similarly, the Expectation Suite ``users.error`` is run against the ``batch_request`` with the results also processed by the actions specified in the top-level ``action_list``. In addition, the top-level Expectation Suite ``users.delivery`` is run against the ``batch_request`` with the results processed by the union of actions in the validations ``action_list`` and in the top-level ``action_list``.

For additional Checkpoint configuration information, see [Manage Checkpoints](./checkpoint_lp.md).
