---
sidebar_label: 'Create and manage Expectations and Expectation Suites'
title: 'Create and manage Expectations and Expectation Suites'
hide_title: true
id: create_manage_expectations_lp
description: Create and manage Expectations and Expectation Suites.
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Create, edit, and implement Expectations and Expectation Suites. An Expectation is a verifiable assertion about your data, and an  Expectation Suite is a collection of verifiable assertions about your data.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Create and edit Expectations based on domain knowledge" description="Create an Expectation Suite without a sample Batch" to="/oss/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly" icon="/img/expectation_icon.svg" />
  <LinkCard topIcon label="Create Expectations interactively with Python" description="Create and manage Expectations and Expectation Suites with Python" to="/oss/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data" icon="/img/python_icon.svg" />
  <LinkCard topIcon label="Edit an existing Expectation Suite" description="Create Expectations and interactively edit the resulting Expectation Suite" to="/oss/guides/expectations/how_to_edit_an_existing_expectationsuite" icon="/img/edit_icon.svg" />
  <LinkCard topIcon label="Create Expectations that span multiple Batches" description="Create Expectations that span multiple Batches of data using Evaluation Parameters" to="/oss/guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters" icon="/img/multiple_batch_icon.svg" />
  <LinkCard topIcon label="Dynamically load evaluation parameters" description="Create an Expectation that loads part of its Expectation configuration from a database at runtime" to="/oss/guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database" icon="/img/load_icon.svg" />
  <LinkCard topIcon label="Identify failed table rows in an Expectation" description="Quickly identify problematic rows in an Expectation" to="/oss/guides/expectations/advanced/identify_failed_rows_expectations" icon="/img/configure_icon.svg" />
</LinkCardGrid>