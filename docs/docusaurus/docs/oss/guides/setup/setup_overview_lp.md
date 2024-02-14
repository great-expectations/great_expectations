---
sidebar_label: 'Configure your Great Expectations environment'
title: 'Configure your Great Expectations environment'
hide_title: true
id: setup_overview_lp
description: Configure GX in your specific environment.
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  This is where you'll find information for setting up GX in your specific environment.
</OverviewCard>

#### Install and configure

<LinkCardGrid>
  <LinkCard topIcon label="GX installation and configuration workflow" description="Learn more about the GX installation and configuration process" to="/oss/guides/setup/setup_overview" icon="/img/workflow_icon.svg" />
  <LinkCard topIcon label="Install GX with Data Source dependencies" description="Install and configure GX" to="/oss/guides/setup/installation/install_gx" icon="/img/install_icon.svg" />
  <LinkCard topIcon label="Configure Data Contexts" description="Instantiate and convert a Data Context" to="/oss/guides/setup/configure_data_contexts_lp" icon="/img/configure_icon.svg"  />
  <LinkCard topIcon label="Configure Expectation Stores" description="Configure a store for your Expectations" to="/oss/guides/setup/configuring_metadata_stores/configure_expectation_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure Validation Result Stores" description="Configure a store for your Validation Results" to="/oss/guides/setup/configuring_metadata_stores/configure_result_stores" icon="/img/configure_icon.svg" />
  <LinkCard topIcon label="Configure a MetricStore" description="Configure a store for Metrics computed during Validation" to="/oss/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore" icon="/img/configure_icon.svg" />
</LinkCardGrid>

#### Host and share

<LinkCardGrid>
  <LinkCard topIcon label="Host and share Data Docs" description="Host and share Data Docs stored on a filesystem or a Data Source" to="/oss/guides/setup/configuring_data_docs/host_and_share_data_docs" icon="/img/host_and_share_icon.svg"  />
</LinkCardGrid>
