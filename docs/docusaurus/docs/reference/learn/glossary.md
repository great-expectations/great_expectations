---
id: glossary
title: "Glossary"
---

[Action](/core/trigger_actions_based_on_results/trigger_actions_based_on_results.md): A component that is configurable on Checkpoints and integrates Great Expectations with other tools based on a Validation Result, like sending notifications based on the validation's outcomes.

[Batch Definition](/core/connect_to_data/connect_to_data.md): A configuration for how a Data Asset should be divided into individual Batches for testing.

[Batch](/core/connect_to_data/connect_to_data.md): A representation of a group of records that validations can be run on.  Used in testing individual Expectations, engaging in data exploration, and internally in Validation Definitions and Checkpoints.

[Checkpoint](/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions.md): An object that groups Validation Definitions and runs them with shared parameters and automated Actions.  Checkpoints are the primary means for validating data in a production deployment of Great Expectations.

[Data Asset](/core/connect_to_data/connect_to_data.md): A collection of records within a Data Source which are usually grouped based on the underlying data system and can be further sliced into Batches according to a desired specification.

[Data Context](/core/set_up_a_gx_environment/create_a_data_context.md): A Data Context stores all of the configuration for a project in Great Expectations, and provides an API to access and update it.

[Data Docs](/core/configure_project_settings/configure_data_docs/configure_data_docs.md): Static websites generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[Data Source](/core/connect_to_data/connect_to_data.md): An object that tells GX how to connect to a specific source of external data and provides a unified API within GX for organizing that data into Data Assets which can be retrieved. 

[Expectation](/core/define_expectations/create_an_expectation.md): A verifiable assertion about data.

[Expectation Suite](/core/define_expectations/organize_expectation_suites.md): A group of Expectations that describe how data should be tested.

[Validation Definition](/core/run_validations/create_a_validation_definition.md): A reference that links an Expectation Suite to data that it describes.

<!-- Update this link to point to the "Review a Validation Result object" guide when it is complete. -->
[Validation Result](/core/run_validations/run_a_validation_definition.md): An object generated when data is validated against an Expectation or Expectation Suite.