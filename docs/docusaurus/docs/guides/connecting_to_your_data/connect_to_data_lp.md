---
sidebar_label: 'Connect to source data'
title: 'Connect to source data'
id: connect_to_data_lp
description: Connect to source data stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for connecting to source data stored on databases and local filesystems, how to request data from a Data Source, how to organize Batches in a file-based Data Asset, and how to connect Great Expectations (GX) to SQL tables and data returned by SQL database queries.</p>

<LinkCardGrid>
  <LinkCard topIcon label="Connect to filesystem source data" description="Connect to filesystem source data" href="/docs/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Connect to in-memory source data using Pandas" description="Connect to in-memory source data using Pandas" href="/docs/guides/connecting_to_your_data/fluent/in_memory/how_to_connect_to_in_memory_data_using_pandas" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Connect to SQL database source data" description="Connect to source data stored on SQL databases" href="/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Manage Data Assets" description="Request data from a Data Source and organize Batches in file-based and SQL Data Assets" href="/docs/guides/connecting_to_your_data/manage_data_assets_lp" icon="/img/manage_icon.svg" />
</LinkCardGrid>
