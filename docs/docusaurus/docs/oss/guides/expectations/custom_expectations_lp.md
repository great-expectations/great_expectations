---
sidebar_label: 'Create and manage Custom Expectations'
title: 'Create and manage Custom Expectations'
id: custom_expectations_lp
description: Create and manage Custom Expectations.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">Create Custom Expectations to extend the functionality of Great Expectations (GX) and satisfy your unique business requirements. To contribute new Expectations to the open source project, see <a href="https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md">Contribute Custom Expectations</a>.
</p>

<LinkCardGrid>
  <LinkCard topIcon label="Create a Custom Column Aggregate Expectation" description="Evaluates a single column and produces an aggregate Metric" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Column Map Expectation" description="Evaluates a single column and performs a yes or no query on every row in the column" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Batch Expectation" description="Evaluates an entire Batch, and answers a semantic question about the Batch" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Column Pair Map Expectation" description="Evaluates a pair of columns and performs a yes or no query about the row-wise relationship between the two columns" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Multicolumn Map Expectation" description="Evaluates a set of columns and performs a yes or no query about the row-wise relationship between the columns" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Regex-Based Column Map Expectation" description="Evaluates a single column and performs a yes or no regex-based query on every row in the column" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_regex_based_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Set-Based Column Map Expectation" description="Evaluates a single column and determines if each row in the column belongs to the specified set" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_set_based_column_map_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Query Expectation" description="Runs Expectations against custom query results and makes intermediate queries to your database" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Create a Custom Parameterized Expectation" description="Inherits classes from existing Expectations and then creates a new customized Expectation" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_create_custom_parameterized_expectations" icon="/img/custom_expectation_icon.svg" />
  <LinkCard topIcon label="Add auto-initializing framework support to a Custom Expectation" description="Automates Expectation parameter estimation" href="/docs/oss/guides/expectations/creating_custom_expectations/how_to_add_support_for_the_auto_initializing_framework_to_a_custom_expectation" icon="/img/custom_expectation_icon.svg" />
</LinkCardGrid>
