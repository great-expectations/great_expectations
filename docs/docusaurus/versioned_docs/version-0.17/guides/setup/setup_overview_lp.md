---
sidebar_label: 'Configure your Great Expectations environment'
title: 'Configure your Great Expectations environment'
id: setup_overview_lp
description: Configure GX in your specific environment.
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for setting up Great Expectations (GX) in your specific environment.</p>

#### Install and configure

<LinkCardGrid>
  <LinkCard topIcon label="GX installation and configuration workflow" description="Learn more about the GX installation and configuration process" to="/guides/setup/setup_overview" icon="/img/workflow_icon.svg" />
  <LinkCard topIcon label="Install GX with source data system dependencies" description="Install and configure GX" to="/guides/setup/installation/install_gx" icon="/img/install_icon.svg" />
  <LinkCard topIcon label="Configure Data Contexts" description="Instantiate and convert a Data Context" to="/guides/setup/configure_data_contexts_lp" icon="/img/configure_icon.svg"  />
  <LinkCard topIcon label="Configure Expectation Stores" description="Configure a store for your Expectations" to="/guides/setup/configuring_metadata_stores/configure_expectation_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure Validation Result Stores" description="Configure a store for your Validation Results" to="/guides/setup/configuring_metadata_stores/configure_result_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure a MetricStore" description="Configure a store for Metrics computed during Validation" to="/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore" icon="/img/configure_icon.svg" />
</LinkCardGrid>

#### Host and share

<LinkCardGrid>
  <LinkCard topIcon label="Host and share Data Docs" description="Host and share Data Docs stored on a filesystem or a source data system" to="/guides/setup/configuring_data_docs/host_and_share_data_docs" icon="/img/host_and_share_icon.svg"  />
</LinkCardGrid>
