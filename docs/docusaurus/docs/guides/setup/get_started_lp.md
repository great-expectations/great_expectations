---
sidebar_label: 'Get started with GX'
title: 'Get started with Great Expectations'
id: get_started_lp
description: Install Great Expectations and initialize your deployment.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for installing Great Expectations (GX) and initializing your deployment.</p>

#### Install and connect

<LinkCardGrid>
  <LinkCard topIcon label="Install GX" description="Install and configure GX" href="/docs/guides/setup/installation/install_gx" icon="/img/install_icon.svg" />
  <LinkCard topIcon label="Connect to a Source Data System" description="Configure the dependencies necessary to access Source Data stored on databases" href="/docs/guides/setup/optional_dependencies/cloud/connect_gx_source_data_system" icon="/img/connect_icon.svg" />
</LinkCardGrid>

#### Configure

<LinkCardGrid>
  <LinkCard topIcon label="Configure Data Contexts" description="Instantiate and convert a Data Context" href="/docs/guides/setup/configure_data_contexts_lp" icon="/img/configure_icon.svg"  />
  <LinkCard topIcon label="Configure Expectation Stores" description="Configure a store for your Expectations" href="/docs/guides/setup/configuring_metadata_stores/configure_expectation_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure Validation Result Stores" description="Configure a store for your Validation Results" href="/docs/guides/setup/configuring_metadata_stores/configure_result_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure a MetricStore" description="Configure a store for Metrics computed during Validation" href="/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore" icon="/img/configure_icon.svg" />
</LinkCardGrid>