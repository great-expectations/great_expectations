---
sidebar_label: 'Set up a GX environment'
title: 'Set up a GX environment'
id: setup_overview_tiles
slug: /
description: Set up and configure GX in your specific environment.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for setting up Great Expectations in your specific environment.</p>

<LinkCardGrid>
  <LinkCard topIcon label="Local filesystems" description="Install and configure GX locally." href="/docs/guides/setup/installation/local" icon="/components/images/cloud_storage.svg" />
  <LinkCard topIcon label="Hosted environments" description="Install and configure GX in environments such as Databricks, AWS EMR, Google Cloud Composer, and others." href="/docs/guides/setup/installation/hosted_environment" icon="/components/images/cloud_storage.svg" />
  <LinkCard topIcon label="Cloud storage" description="Install and configure GX in environments where data is stored on a Cloud service." href="/docs/guides/setup/optional_dependencies/cloud/how_to_set_up_gx_to_work_with_data_on_aws_s3" icon="/components/images/cloud_storage.svg" />
  <LinkCard topIcon label="SQL databases" description="Install and configure GX in environments using SQL databases." href="/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases" icon="https://docs.greatexpectations.io/img/gx-logo.svg" />
</LinkCardGrid>