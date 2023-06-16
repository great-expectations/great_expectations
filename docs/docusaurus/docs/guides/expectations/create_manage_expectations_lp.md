---
sidebar_label: 'Create and manage Expectations and Expectation Suites'
title: 'Create and manage Expectations and Expectation Suites'
id: create_manage_expectations_lp
description: Create and manage Expectations and Expectation Suites.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">Create, edit, and implement Expectations and Expectation Suites. An Expectation is a verifiable assertion about your data, and an  Expectation Suite is a collection of verifiable assertions about your data.</p>

<LinkCardGrid>
  <LinkCard topIcon label="Create and edit Expectations based on domain knowledge" description="Create an Expectation Suite without a sample Batch" href="/docs/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly" icon="/img/expectation_icon.svg" />
  <LinkCard topIcon label="Create Expectations interactively with Python" description="Create and manage Expectations and Expectation Suites" href="/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data" icon="/img/python_icon.svg" />
  <LinkCard topIcon label="Edit an Expectation Suite" description="Create Expectations and interactively edit the resulting Expectation Suite" href="/docs/guides/expectations/how_to_edit_an_existing_expectationsuite" icon="/img/edit_icon.svg" />
  <LinkCard topIcon label="Use auto-initializing Expectations" description="Use auto-initializing Expectations to automate parameter estimation" href="/docs/guides/expectations/how_to_use_auto_initializing_expectations" icon="/img/auto_initializing_icon.svg" />
  <LinkCard topIcon label="Create Expectations that span multiple Batches" description="Create Expectations that span multiple Batches of data using Evaluation Parameters" href="/docs/guides/expectations/advanced/how_to_create_expectations_that_span_multiple_batches_using_evaluation_parameters" icon="/img/multiple_batch_icon.svg" />
  <LinkCard topIcon label="Dynamically load evaluation parameters" description="Create an Expectation that loads part of its Expectation configuration from a database at runtime" href="/docs/guides/expectations/advanced/how_to_dynamically_load_evaluation_parameters_from_a_database" icon="/img/load_icon.svg" />
  <LinkCard topIcon label="Compare two tables with the Onboarding Data Assistant" description="Use the Onboarding Data Assistant to create an Expectation Suite that determines if two tables are identical" href="/docs/guides/expectations/advanced/how_to_compare_two_tables_with_the_onboarding_data_assistant" icon="/img/assistant_icon.svg" />
</LinkCardGrid>