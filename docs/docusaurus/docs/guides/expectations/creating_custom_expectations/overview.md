---
title: Custom Expectations overview
---
import Prerequisites from './components/prerequisites.jsx'
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';

You can create your own <TechnicalTag tag="custom_expectation" text="Custom Expectations" /> to extend the functionality of Great Expectations (GX). You can also contribute new <TechnicalTag tag="expectation" text="Expectations" /> to the open source project to enrich GX as a shared standard for data quality.

## Custom Expectation maturity levels

The following table lists the Custom Expectations maturity levels.

| Expectation Type              | Description                            |
|-------------------------------|----------------------------------------|
| Experimental             | <ul><li>Has a valid `library_metadata` object</li><li>Has a docstring, including a one-line short description</li><li>Has at least one positive and negative example case, and all test cases pass</li><li>Has core logic and passes tests on at least one <TechnicalTag tag="execution_engine" text="Execution Engine" /></li><li>Passes all linting checks</li></ul>                  |
| Beta                     |<ul><li>Has basic input validation and type checking</li><li>Has both Statement Renderers: prescriptive and diagnostic</li><li>Has core logic that passes tests for all applicable Execution Engines and SQL dialects</li></ul>                |
| Production                                                | <ul><li>Has a robust suite of tests, as determined by a code owner</li><li>Has passed a manual review by a code owner for code standards and style guides</li></ul> |


For more information about feature and code readiness levels, see [Feature and code readiness](../../../contributing/contributing_maturity.md).