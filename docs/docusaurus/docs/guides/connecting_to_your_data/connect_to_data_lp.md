---
sidebar_label: 'Connect to a Data Source'
title: 'Connect to a Data Source'
id: connect_to_data_lp
description: Connect to data stored on Amazon S3, Google Cloud Storage (GCS), Microsoft Azure Blob Storage, or local filesystems.
---

import LinkCardGrid from '/docs/components/LinkCardGrid';
import LinkCard from '/docs/components/LinkCard';

<p class="DocItem__header-description">This is where you'll find information for connecting to data stored on databases and local filesystems, how to request data from a Data Source, how to organize Batches in a file-based Data Asset, and how to connect Great Expectations (GX) to SQL tables and data returned by SQL database queries.</p>

<LinkCardGrid>
  <LinkCard topIcon label="Connect to filesystem Data Assets" description="Connect to filesystem Data Assets" href="/docs/guides/connecting_to_your_data/fluent/filesystem/connect_filesystem_source_data" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Connect to in-memory Data Assets" description="Connect to an in-memory pandas or Spark DataFrame" href="/docs/guides/connecting_to_your_data/fluent/in_memory/connect_in_memory_data" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Connect to SQL database Data Assets" description="Connect to Data Assets on SQL databases" href="/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data" icon="/img/connect_icon.svg" />
  <LinkCard topIcon label="Manage Data Assets" description="Request data from a Data Source and organize Batches in file-based and SQL Data Assets" href="/docs/guides/connecting_to_your_data/manage_data_assets_lp" icon="/img/manage_icon.svg" />
</LinkCardGrid>
