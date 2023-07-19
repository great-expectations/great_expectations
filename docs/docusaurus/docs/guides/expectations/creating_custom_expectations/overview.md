---
title: Custom Expectations overview
---
import Prerequisites from './components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

You can create your own <TechnicalTag tag="custom_expectation" text="Custom Expectations" /> to extend the functionality of Great Expectations (GX). You can also contribute new <TechnicalTag tag="expectation" text="Expectations" /> to the open source project to enrich GX as a shared standard for data quality.

## Custom Expectation types

The following table lists the Custom Expectations types.

| Expectation Type              | Description                            |
|-------------------------------|----------------------------------------|
| Experimental             | <ul><li>Has a valid `library_metadata` object</li><li>Has a docstring, including a one-line short description</li><li>Has at least one positive and negative example case, and all test cases pass</li><li>Has core logic and passes tests on at least one <TechnicalTag tag="execution_engine" text="Execution Engine" /></li><li>Passes all linting checks</li></ul>                  |
| Beta                     |<ul><li>Has basic input validation and type checking</li><li>Has both Statement Renderers: prescriptive and diagnostic</li><li>Has core logic that passes tests for all applicable Execution Engines and SQL dialects</li></ul>                |
| Production                                                | <ul><li>Has a robust suite of tests, as determined by a code owner</li><li>Has passed a manual review by a code owner for code standards and style guides</li></ul> |


For more information about Feature and code readiness levels, see [Feature and code readiness](../../../contributing/contributing_maturity.md).

### How these docs are organized

The docs in `Creating Custom Expectations` focus on completing the five steps required for Experimental Expectations. 
Completing them will leave you with a Custom Expectation that meets our linting standards, can be executed against one backend, with a couple tests to verify correctness, and a basic docstring and metadata to support diagnostics. 

The code to achieve the first four steps looks somewhat different depending on the class of Expectation you're developing. Accordingly, there are separate how-to guides and templates for each class of Expectation.

| Guide: "How to create a custom..." |  Template |
|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Column Map Expectation](./how_to_create_custom_column_map_expectations.md)             | [column_map_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_map_expectation_template.py)       |
| [Column Aggregate Expectation](./how_to_create_custom_column_aggregate_expectations.md) | [column_aggregate_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_aggregate_expectation_template.py) |
| [Batch Expectation](./how_to_create_custom_batch_expectations.md) | [batch_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/batch_expectation_template.py) |
| [Regex-Based Column Map Expectation](./how_to_create_custom_regex_based_column_map_expectations.md) | [regex-based map column_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/regex_based_column_map_expectation_template.py) |
| [Set-Based Column Map Expectation](./how_to_create_custom_set_based_column_map_expectations.md) | [set-based map_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/set_based_column_map_expectation_template.py) |


:::note 
Not all classes of Expectation currently have guides and templates. <br></br>
If you'd like to develop a different kind of Expectation, please reach out on [Slack](https://greatexpectations.io/slack).
:::

Beyond the first four steps, additional features are generally similar across all Expectation classes. Accordingly, most of the remaining steps have their own how-to guide in the `Adding Features to Custom Expectations` section of the table of contents.

| Step | Guide |
|------|-------|
| Passes all linting checks                                                                        | [Great Expectations Code Style Guide: Linting](../../../contributing/style_guides/code_style.md#linting) |
| Has basic input validation and type checking                                                     | [How to add input validation and type checking for a Custom Expectation](../features_custom_expectations/how_to_add_input_validation_for_an_expectation.md) |
| Has core logic that passes tests for all applicable Execution Engines and SQL dialects  | [How to add SQLAlchemy support for Custom Expectations](../features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation.md)<br/> [How to add Spark support for Custom Expectations](../features_custom_expectations/how_to_add_spark_support_for_an_expectation.md)|

The final two checks required for acceptance into the Great Expectations codebase at a Production level require manual review and guidance by a code owner.

### Using your Expectation

You can find instructions for using your Custom Expectation in our guide: [how to use a Custom Expectation](./how_to_use_custom_expectations.md).

### Publishing your Expectation as an open source contribution

Optionally, you can also publish Custom Expectations to the [Great Expectations open source gallery](https://greatexpectations.io/expectations) by following the steps [here](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md).
