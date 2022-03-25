---
title: Overview
---
import Prerequisites from './components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

You can extend the functionality of Great Expectations by creating your own <TechnicalTag tag="custom_expectation" text="Custom Expectations" />. You can also enrich Great Expectations as a shared standard for data quality by contributing new <TechnicalTag tag="expectation" text="Expectations" /> to the open source project.

These processes compliment each other and their steps are streamlined so that one flows into the other. Once you have created a Custom Expectation, it is simple to contribute it to the open source project. This section will teach you how to do both.

<Prerequisites>
</Prerequisites>

## Creating Custom Expectations

A fully-developed, Production-ready Expectation needs to do a lot of things:
* Execute consistently across many types of data infrastructure
* Render itself and its <TechnicalTag tag="validation_result" text="Validation Results" /> into several formats
* Support <TechnicalTag tag="profiling" text="Profiling" /> against new data
* Be maintainable, with good tests, documentation, linting, type hints, etc.

In order to make development of Expectations as easy as possible, we've broken up the steps to create Custom Expectations into a series of bite-sized steps. Each step can be completed in minutes. They can be completed (and contributed) incrementally, unlocking value at each step along the way.

Grouped together, they constitute a Definition of Done for Expectations at each [Level of Maturity](../../../contributing/contributing_maturity.md).

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer"/>
<i class="fas fa-circle" style={{color: "#dc3545"}}></i> An Experimental Expectation...

* Has a `library_metadata` object
* Has a docstring, including a one-line short description
* Has at least one positive and negative example case, and all test cases pass
* Has core logic and passes tests on at least one <TechnicalTag tag="execution_engine" text="Execution Engine" />

<i class="fas fa-circle" style={{color: "#ffc107"}}></i> A Beta Expectation...

* Has basic input validation and type checking
* Has both Statement Renderers: prescriptive and diagnostic
<!-- * Has default `Parameter Builders` and Domain hooks to support Profiling -->
* Has core logic that passes tests for all applicable Execution Engines

<i class="fas fa-check-circle" style={{color: "#28a745"}}></i> A Production Expectation...

* Passes all linting checks
<!--  * Has all applicable Renderers, with fully typed and styled output -->
* Has a robust suite of tests, as determined by a code owner
* Has passed a manual review by a code owner for code standards and style guides

### How these docs are organized

The docs in `Creating Custom Expectations` focus on completing the four steps required for Experimental Expectations. 
Completing them will leave you with a Custom Expectation that can be executed against one backend, with a couple tests to verify correctness, and a basic docstring and metadata to support diagonstics. 

The code to achieve the first four steps looks somewhat different depending on the class of Expectation you're developing. Accordingly, there are separate how-to guides and templates for each class of Expectation.

| Guide: "How to create a custom..." |  Template |
|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Column Map Expectation](./how_to_create_custom_column_map_expectations.md)             | [column_map_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_map_expectation_template.py)       |
| [Column Aggregate Expectation](./how_to_create_custom_column_aggregate_expectations.md) | [column_aggregate_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_aggregate_expectation_template.py) |
| [Regex-Based Column Map Expectation](./how_to_create_custom_regex_based_column_map_expectations.md) | [regex-based map column_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/regex_based_column_map_expectation_template.py) |
| [Set-Based Column Map Expectation](./how_to_create_custom_set_based_column_map_expectations.md) | [set-based map_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/set_based_column_map_expectation_template.py) |


:::note 
Not all classes of Expectation currently have guides and templates. <br></br>
If you'd like to develop a different kind of Expectation, please reach out on [Slack](https://greatexpectations.io/slack).
:::

Beyond the first four steps, additional features are generally similar across all Expectation classes. Accordingly, most of the remaining steps have their own how-to guide in the `Adding Features to Custom Expectations` section of the table of contents.

| Step | Guide |
|------|-------|
| Has basic input validation and type checking                                                     | [How to add input validation and type checking for a Custom Expectation](../features_custom_expectations/how_to_add_input_validation_for_an_expectation.md) |
| Has both Statement Renderers: prescriptive and diagnostic              | [How to add Statement Renderers for Custom Expectations](../features_custom_expectations/how_to_add_statement_renderers_for_an_expectation.md) |
| Has core logic that passes tests for all applicable Execution Engines   | [How to add SQLAlchemy support for Custom Expectations](../features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation.md)<br/> [How to add Spark support for Custom Expectations](../features_custom_expectations/how_to_add_spark_support_for_an_expectation.md)|
| Passes all linting checks                                                                        | [Great Expectations Code Style Guide: Linting](../../../contributing/style_guides/code_style.md#linting) |

The final two checks required for acceptance into the Great Expectations codebase at a Production level require manual review and guidance by a code owner.

### Using your Expectation

You can find instructions for using your Custom Expectation in our guide: [how to use a Custom Expectation](./how_to_use_custom_expectations.md).

### Publishing your Expectation as an open source contribution

Optionally, you can also publish Custom Expectations to the [Great Expectations open source gallery](https://greatexpectations.io/expectations) by following the steps [here](../contributing/how_to_contribute_a_custom_expectation_to_great_expectations.md).
