---
title: 'GX OSS'
hide_title: true
description: Get started with GX OSS and learn more about GX OSS features and functionality.
hide_table_of_contents: true
pagination_next: null
pagination_prev: null
slug: '/oss/'
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  Test and validate your Data Assets with our open source offering.
</OverviewCard>

### Prepare

<LinkCardGrid>
  <LinkCard topIcon label="Get started with GX OSS" description="This is a great place to start if you're unfamiliar with GX OSS, or you want to use GX OSS with Databricks or a SQL Data Source in a production environment." to="/oss/guides/setup/get_started_lp" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Configure your GX OSS environment" description="Set up GX OSS in your specific environment." to="/oss/guides/setup/setup_overview_lp" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Connect to source data" description="Connect to source data stored on databases and local filesystems, request data from a Data Source, organize Batches in a file-based Data Asset, and connect GX OSS to SQL tables and data returned by SQL database queries." to="/oss/guides/connecting_to_your_data/connect_to_data_lp" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Review the changelog" description="View a summary of all changes released to GX Cloud and GX OSS." to="/oss/changelog" icon="/img/release_notes_icon.svg" />
</LinkCardGrid>

### Identify, validate, and integrate

<LinkCardGrid>
  <LinkCard topIcon label="Create Expectations" description="Create and manage Expectations and Expectation Suites." to="/oss/guides/expectations/expectations_lp" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Validate Data" description="Validate Data, save Validation Results, run Actions, and create Data Docs." to="/oss/guides/validation/validate_data_lp" icon="/img/small_gx_logo.png" />
  <LinkCard topIcon label="Integrations" description="Integrate GX OSS with commonly used data engineering tools." to="/category/integrate" icon="/img/small_gx_logo.png" />
</LinkCardGrid>

### Contribute

<LinkCardGrid>
  <LinkCard topIcon label="Contribute" description="Contribute to GX OSS documentation or code." to="/oss/contributing/contributing" icon="/img/small_gx_logo.png" />
</LinkCardGrid>