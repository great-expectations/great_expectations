---
title: Overview
---
import Prerequisites from './components/prerequisites.jsx'

You can extend the functionality of Great Expectations by creating your own custom Expectations. You can also enrich Great Expectations as a shared standard for data quality by contributing new Expectations to the open source project.

Both processes follow the same streamlined steps. This section will teach you how.

<Prerequisites></Prerequisites>

## Steps to create a custom Expectation

A fully developed Expectation needs to do a lot of things:
* Execute consistently across many types of data infrastructure
* Render itself and its Validation Results into several different formats
* Support Profiling against new data
* Be maintainable, with good tests, documentation, linting, type hints, etc.

In order to make development of Expectations as easy as possible, we've broken up the steps to create custom Expectations into a series of bite-sized steps. Each step can be completed in minutes. They can be completed (and contributed) incrementally, unlocking value at each step along the way.

Grouped together, they constitute a Definition of Done for Expectations at each [Level of Maturity](/docs/contributing/contributing_maturity).

<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" crossorigin="anonymous" referrerpolicy="no-referrer"/>
<i class="fas fa-circle" style={{color: "#dc3545"}}></i> An experimental Expectation...

* Has a `library_metadata` object
* Has a docstring, including a one-line short description
* Has at least one positive and negative example case
* Has core logic and passes tests on at least one `Execution Engine`

<i class="fas fa-circle" style={{color: "#ffc107"}}></i> A beta Expectation...

* Has basic input validation and type checking
* Has all four statement `Renderers`: question, descriptive, prescriptive, diagnostic
* Has default `Parameter Builders` and Domain hooks to support Profiling   
* Has core logic exists and passes tests for all applicable `Execution Engines` and SQL dialects

<i class="fas fa-check-circle" style={{color: "#28a745"}}></i> A production Expectation...

* Passes all linting checks
* Has all applicable Renderers, with fully typed and styled output
* Has a full suite of tests, as determined by project code standards
* Has passed a manual review by a code owner for code standards and style guides

## How these docs are organized

The docs in `Creating custom Expectations` focus on completing the four steps for experimental Expectations. Completing them will get to the point where your Expectation can be executed against one backend, with a couple tests to verify correctness, and a basic docstring and metadata to support diagonstics. Optionally, you can also publish experimental Expectations to the [Great Expectations open source gallery](https://greatexpectations.io/expectations) by following the steps [here](overview#publishing-your-expectation-as-an-open-source-contribution).

These code to achieve the first four steps looks somewhat different depending on the class of Expectation you're developing. Accordingly, there are separate how-to guides and templates for each class of Expectation.

| Guide: "How to create a custom..." |  Template |
|-----------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Column Map Expectation](how_to_create_custom_column_map_expectations)             | [column_map_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_map_expectation_template.py)       |
| [Column Aggregate Expectation](how_to_create_custom_column_aggregate_expectations) | [column_aggregate_expectation_template](https://github.com/great-expectations/great_expectations/blob/develop/examples/expectations/column_map_expectation_template.py) |

Note: not all classes of Expectation currently have guides and templates. If you'd like to develop a different kind of Expectation, please reach out on [Slack](https://greatexpectations.io/slack).

Beyond the first four steps, additional features are generally similar across all Expectation classes. Accordingly, each of the remaining steps has its own how-to guide in the `Adding features to Great Expectations` section of the table of contents.

| Step | Guide |
|------|-------|
| Has basic input validation and type checking                                                     | [How to add configuration validation for an Expectation](../features_custom_expectations/how_to_add_input_validation_for_an_expectation) |
| Has all four statement `Renderers`: question, descriptive, prescriptive, diagnostic              | [How to add text renderers for custom Expectations](../features_custom_expectations/how_to_add_text_renderers_for_an_expectation) |
| Has default `Parameter Builders` and Domain hooks to support Profiling                           | |
| Has core logic exists and passes tests for all applicable `Execution Engines` and SQL dialects   | [How to add SQLAlchemy support for custom Metrics](../features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation)<br/> [How to add Spark support for custom Metrics](../features_custom_expectations/how_to_add_spark_support_for_an_expectation)|
| Passes all linting checks                                                                        | |
| Has all applicable Renderers, with fully typed and styled output                                 | |
| Has a full suite of tests, as determined by project code standards                               | |
| Has passed a manual review by a code owner for code standards and style guides                   | |

## Publishing your Expectation as an open source contribution

(Add this later; possibly in a different location in the TOC)

## Wrapping up