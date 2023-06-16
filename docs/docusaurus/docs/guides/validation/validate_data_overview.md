---
title: "Data Validation Workflow"
---

import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

Great Expectations recommends using Checkpoints to validate data.  Checkpoints validate data, save <TechnicalTag tag="validation_result" text="Validation Results" />, run any <TechnicalTag tag="action" text="Actions" /> you have specified, and finally, create <TechnicalTag tag="data_docs" text="Data Docs" /> with their results.  A Checkpoint can be reused to <TechnicalTag tag="validation" text="Validate" /> data in the future, and you can create and configure additional Checkpoints for different business requirements.

![How a Checkpoint works](../../images/universal_map/overviews/how_a_checkpoint_works.png)

After you've created your Checkpoint, configured it, and specified the Actions you want it to take based on the Validation Results, all you'll need to do in the future is run the Checkpoint.

## Prerequisites

- Completion of the [Quickstart guide](tutorials/quickstart/quickstart.md)

## Create a Checkpoint

See [How to create a new Checkpoint](./checkpoints/how_to_create_a_new_checkpoint.md).

## Configure your Checkpoint

When you configure your Checkpoint you can add additional validation data, or specify that validation data must be specified at run time.  You can add additional <TechnicalTag tag="expectation_suite" text="Expectation Suites" />, and you can add Actions which the Checkpoint executes when it finishes Validating data.  To learn more about Checkpoint configuration, see [Checkpoints](../../terms/checkpoint.md) and [Actions](../../terms/action.md).

### Checkpoints, Batch Requests, and Expectation Suites

<p class="markdown"><TechnicalTag tag="batch_request" text="Batch Requests" /> are used to specify the data that a Checkpoint Validates.  You can add additional validation data to your Checkpoint by assigning it Batch Requests, or specifying that a Batch Request is required at run time.</p>

Expectation Suites contain the <TechnicalTag tag="expectation" text="Expectations" /> that the Checkpoint runs against the validation data specified in its Batch Requests.  Checkpoints are assigned Expectation Suites and Batch Requests in pairs, and when the Checkpoint is run it will Validate each of its Expectation Suites against the data provided by its paired Batch Request.

For more information on adding Batch Requests and Expectation Suites to a Checkpoint, see [How to add validations data or suites to a Checkpoint](./checkpoints/how_to_add_validations_data_or_suites_to_a_checkpoint.md).

### Checkpoints and Actions

Actions are optional and are executed after a Checkpoint validates data. Some of the more common Actions include updating Data Docs, sending emails, posting Slack notifications, or sending custom notifications. You can create custom Actions to complete business specific actions after a Checkpoint Validates. For more information about Actions, see [Validation Actions](./index.md#validation-actions).

## Run your Checkpoint

See [How to validate data by running a Checkpoint](./how_to_validate_data_by_running_a_checkpoint.md).

## Validation Results and Data Docs

When a Checkpoint finishes Validation, its Validation Results are automatically compiled as Data Docs.  You can find these results in the Validation Results tab of your Data Docs, and clicking an individual Validation Result in the Data Docs displays a detailed list of all the Expectations that ran, as well as which Expectations passed or failed.

For more information, see the <TechnicalTag tag="data_docs" text="Data Docs"/> documentation. 

## Checkpoint reuse

After your Checkpoint is created, and you have used it to validate data, you can reuse it in a Python script. If you want your Checkpoint to run on a schedule, see [How to deploy a scheduled Checkpoint with cron](./advanced/how_to_deploy_a_scheduled_checkpoint_with_cron.md). If your pipeline architecture supports it, you can run your Checkpoints with Python scripts.  Regardless of the method you use to run your Checkpoint, Actions let you customize what is done with the generated Validation Results. 