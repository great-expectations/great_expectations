---
title: How to collect OpenLineage metadata using a Validation Action
---

import Prerequisites from '../../../guides/connecting_to_your_data/components/prerequisites.jsx';

[OpenLineage](https://openlineage.io) is an open framework for collection and analysis of data lineage. It tracks the movement of data over time, tracing relationships between datasets. Data engineers can use data lineage metadata to determine the root cause of failures, identify performance bottlenecks, and simulate the effects of planned changes.

Enhancing the metadata in OpenLineage with results from an Expectation Suite makes it possible to answer questions like:
* have there been failed assertions in any upstream datasets?
* what jobs are currently consuming data that is known to be of poor quality?
* is there something in common among failed assertions that seem otherwise unrelated?

This guide will explain how to use a Validation Action to emit results to an OpenLineage backend, where their effect on related datasets can be studied.

<Prerequisites>

 - Created at least one Expectation Suite.
 - Created at least one [Checkpoint](/docs/guides/validation/checkpoints/how_to_create_a_new_checkpoint) - you will need it in order to test that the OpenLineage Validation Action is working.

</Prerequisites>

Steps
------

1. Ensure that the `openlineage-common` package has been installed in your Python environment.

    ```bash
    % pip3 install openlineage-common
    ```

2. Update the action_list key in your Checkpoint config.

    Add the ``OpenLineageValidationAction`` Action to the end of your Checkpoint's ``action_list``.

    ```yaml
    name: taxi_data
    module_name: great_expectations.checkpoint
    class_name: Checkpoint
    # ...
    action_list:
      - name: openlineage  # this name is user-definable
        action:
          class_name: OpenLineageValidationAction
          module_name: openlineage.common.provider.great_expectations
          openlineage_host: ${OPENLINEAGE_URL}
          openlineage_apiKey: ${OPENLINEAGE_API_KEY}
          job_name: ge_validation # This is user-definable
          openlineage_namespace: ge_namespace # This is user-definable
    ```

    The `openlineage_host` and `openlineage_apiKey` values can be set via the environment, as shown above, or can be implemented as variables in `uncommitted/config_variables.yml`. The `openlineage_apiKey` value is optional, and is not required by all OpenLineage backends.

    A Great Expectations checkpoint is recorded as a Job in OpenLineage, and will be named according to the `job_name` value. Similarly, the `openlineage_namespace` value can be optionally set. For more information on job naming, consult the [Naming section](https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md#job-namespace-and-constructing-job-names) of the OpenLineage spec.

3. Run your Checkpoint to validate a batch of data and emit lineage events to the OpenLineage backend. This can be done in code:

    ```python
    import great_expectations as ge
    context = ge.get_context()
    checkpoint_name = "your checkpoint name here"
    context.run_checkpoint(checkpoint_name=checkpoint_name)
    ```

    Alternatively, this can be done through the Great Expectations CLI:
    ```bash
    % great_expectations checkpoint run <checkpoint_name>
    ```

Additional resources
--------------------

- [Checkpoints and Actions](/docs/reference/checkpoints_and_actions)
- The [OpenLineage Spec](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md)
- Blog: [Expecting Great Quality with OpenLineage Facets](https://openlineage.io/blog/dataquality_expectations_facet/)