---
id: glossary
title: "Glossary"
---

[Action](/core/validate_data/checkpoints/manage_checkpoints.md): A component that integrates Great Expectations with other tools based on a Validation Result, like sending notifications based on the validation's outcomes.

[Batch](/core/manage_and_access_data/manage_batches.md): A set of data that is validated as part of a single test run.

[Batch Definition](/core/manage_and_access_data/manage_batch_definitions/manage_batch_definitions.md): A configuration for how a Data Asset should be divided into individual batches for testing.

[Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md): An object that groups Validation Definitions and runs them with shared parameters and automated Actions.  Checkpoints are the primary means for validating data in a production deployment of Great Expectations.

[Data Asset](/core/manage_and_access_data/manage_data_assets.md): A collection of records within a Data Source which are usually grouped based on the underlying data system and can be further sliced into Batches according to a desired specification.

[Data Context](/core/installation_and_setup/manage_data_contexts.md): A Data Context stores all of the configuration for a project in Great Expectations, and provides an API to access and update it.

[Data Docs](/core/installation_and_setup/manage_data_docs.md): Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[Data Source](/core/manage_and_access_data/manage_data_sources/manage_data_sources.md): An object that tells GX how to access a specific source of external data and provides a unified API within GX for organizing that data into Data Assets which can be retrieved. 

[Expectation](/core/create_expectations/expectations/manage_expectations.md): A verifiable assertion about data.

[Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md): A group of Expectations that describe how data should be tested.

[Validation Definition](/core/validate_data/validation_definitions/manage_validation_definitions.md): A fixed reference that links an Expectation Suite to data that it describes.

[Validation Result](/core/validate_data/validation_results/manage_validation_results.md): A report generated when data is validated against an Expectation or Expectation Suite.
