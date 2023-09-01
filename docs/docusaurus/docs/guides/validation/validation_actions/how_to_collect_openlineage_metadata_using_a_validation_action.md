---
title: Collect OpenLineage metadata using an Action
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

[OpenLineage](https://openlineage.io) is an open framework for collection and analysis of data lineage. It tracks the movement of data over time, tracing relationships between datasets. Data engineers can use data lineage metadata to determine the root cause of failures, identify performance bottlenecks, and simulate the effects of planned changes.

Enhancing the metadata in OpenLineage with results from an <TechnicalTag tag="expectation_suite" text="Expectation Suite" /> makes it possible to answer questions like:
* have there been failed assertions in any upstream datasets?
* what jobs are currently consuming data that is known to be of poor quality?
* is there something in common among failed assertions that seem otherwise unrelated?

This guide will explain how to use an <TechnicalTag tag="action" text="Action" /> to emit results to an OpenLineage backend, where their effect on related datasets can be studied.

## Prerequisites

<Prerequisites>

 - Created at least one Expectation Suite
 - [Created at least one Checkpoint](../checkpoints/how_to_create_a_new_checkpoint.md) - you will need it in order to test that the OpenLineage <TechnicalTag tag="validation" text="Validation" /> is working.

</Prerequisites>

## Ensure that the `openlineage-integration-common` package has been installed in your Python environment.

 ```bash
 % pip3 install openlineage-integration-common
 ```

## Update the `action_list` key in your Validation Operator config.

 Add the ``OpenLineageValidationAction`` action to the ``action_list`` key your Checkpoint configuration.

 ```yaml
action_list:
 - name: openlineage
   action:
     class_name: OpenLineageValidationAction
     module_name: openlineage.common.provider.great_expectations
     openlineage_host: ${OPENLINEAGE_URL}
     openlineage_apiKey: ${OPENLINEAGE_API_KEY}
     job_name: ge_validation # This is user-definable
     openlineage_namespace: ge_namespace # This is user-definable
 ```

 The `openlineage_host` and `openlineage_apiKey` values can be set via the environment, as shown above, or can be implemented as variables in `uncommitted/config_variables.yml`. The `openlineage_apiKey` value is optional, and is not required by all OpenLineage backends.

 A Great Expecations <TechnicalTag tag="checkpoint" text="Checkpoint" /> is recorded as a Job in OpenLineage, and will be named according to the `job_name` value. Similarly, the `openlineage_namespace` value can be optionally set. For more information on job naming, consult the [Naming section](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md#job-namespace-and-constructing-job-names) of the OpenLineage spec.

## Test your Action by Validating a Batch of data.

Run the following command to retrieve and run a Checkpoint to Validate a <TechnicalTag tag="batch" text="Batch" /> of data and then emit lineage events to the OpenLineage backend:

```python name="tests/integration/docusaurus/reference/glossary/checkpoints.py retrieve_and_run"
```

:::note Reminder
Our [guide on how to Validate data by running a Checkpoint](../checkpoints/how_to_create_a_new_checkpoint.md) has more detailed instructions for this step, including instructions on how to run a checkpoint from a Python script instead of from the <TechnicalTag tag="cli" text="CLI" />.
:::

## Related documentation

- [Checkpoints overview page](../../../terms/checkpoint.md)
- [Actions overview page](../../../terms/action.md)
- The [OpenLineage Spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md)
- Blog: [Expecting Great Quality with OpenLineage Facets](https://openlineage.io/blog/dataquality_expectations_facet/)
