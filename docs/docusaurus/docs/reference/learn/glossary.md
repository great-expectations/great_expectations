---
id: glossary
title: "Glossary"
---

[Action](/core/validate_data/checkpoints/manage_checkpoints.md): A Python class with a `run` method that takes a Validation Result and does something based on its contents.

[Batch](/core/manage_and_access_data/manage_batches.md): A selection of records retrieved from a Data Asset according to the parameters passed to a Batch Definition.

[Batch Definition](/core/manage_and_access_data/manage_batch_definitions/manage_batch_definitions.md): A reference that tells GX how to slice the records in a Data Asset into subsets.

[Checkpoint](/core/validate_data/checkpoints/manage_checkpoints.md): An object that groups Validation Definitions and runs them with shared parameters and automated Actions.  Checkpoints are the primary means for validating data in a production deployment of Great Expectations.

[Data Asset](/core/manage_and_access_data/manage_data_assets.md): A collection of records within a Data Source which are usually grouped based on the underlying data system and can be further sliced into Batches according to a desired specification.

[Data Context](/core/installation_and_setup/manage_data_contexts.md): A storage location for metadata such as configurations for GX components and output such as Validation Results and the metrics associated with them.

[Data Docs](/core/installation_and_setup/manage_data_docs.md): Human readable documentation generated from Great Expectations metadata detailing Expectations, Validation Results, etc.

[Data Source](/core/manage_and_access_data/manage_data_sources/manage_data_sources.md): An object that tells GX how to access a specific source of external data and provides a unified API within GX for organizing that data into Data Assets which can be retrieved. 

[Expectation](/core/create_expectations/expectations/manage_expectations.md): A verifiable assertion about data.

[Expectation Suite](/core/create_expectations/expectation_suites/manage_expectation_suites.md): A group of Expectations that describe the same set of data.

[Validation Definition](/core/validate_data/validation_definitions/manage_validation_definitions.md): A fixed reference that links an Expectation Suite to data that it describes.

[Validation Result](/core/validate_data/validation_results/manage_validation_results.md): A report generated when data is validated against an Expectation or Expectation Suite.
