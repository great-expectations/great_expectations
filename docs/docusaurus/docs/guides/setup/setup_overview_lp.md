---
sidebar_label: 'Set up your Great Expectations environment'
title: 'Set up your Great Expectations environment'
id: setup_overview_lp
description: Set up and configure GX in your specific environment.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for setting up Great Expectations (GX) in your specific environment.</p>

<LinkCardGrid>

### Install and connect

  <LinkCard topIcon label="Install GX" description="Install and configure GX" href="/docs/guides/setup/installation/install_gx" icon="/img/install_icon.svg" />
  <LinkCard topIcon label="Connect to a Source Data System" description="Configure the dependencies necessary to access Source Data stored on databases" href="/docs/guides/setup/optional_dependencies/cloud/connect_gx_source_data_system" icon="/img/connect_icon.svg" />

### Configure

<LinkCard topIcon label="Configure Data Contexts" description="Instantiate and convert a Data Context" href="/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure Expectation Stores" description="Configure a store for your Expectations" href="/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure Validation Result Stores" description="Configure a store for your Validation Results" href="/docs/guides/setup/configuring_metadata_stores/configure_result_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure a MetricStore" description="Configure a store for Metrics computed during Validation" href="/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore" icon="/img/configure_icon.svg" />

### Host and share

<LinkCardGrid>
  <LinkCard topIcon label="Host and share Data Docs" description="Host and share Data Docs stored on a filesystem or a Source Data System" href="/docs/guides/setup/configuring_data_docs/host_and_share_data_docs" icon="/img/host_and_share_icon.svg"  />

</LinkCardGrid>