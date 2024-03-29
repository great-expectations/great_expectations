---
sidebar_label: 'Manage Data Assets'
title: 'Manage Data Assets'
hide_title: true
id: manage_data_assets_lp
description: Request data from a Data Source and organize Batches in file-based and SQL Data Assets.
hide_feedback_survey: true
---

import LinkCardGrid from '@site/src/components/LinkCardGrid';
import LinkCard from '@site/src/components/LinkCard';
import OverviewCard from '@site/src/components/OverviewCard';

<OverviewCard title={frontMatter.title}>
  This is where you'll find information for managing your Data Assets. A Data Asset is a collection of records within a Data Source that define how GX organizes data into Batches.
</OverviewCard>

<LinkCardGrid>
  <LinkCard topIcon label="Request data from a Data Asset" description="Request data from a Data Source" to="/oss/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset" icon="/img/request_icon.svg" />
  <LinkCard topIcon label="Organize Batches in a file-based Data Asset" description="Organize Batches in a file-based Data Asset" to="/oss/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_file_based_data_asset" icon="/img/organize_icon.svg" />
  <LinkCard topIcon label="Manage SQL Data Assets" description="Connect GX to SQL tables and data returned by SQL database queries, and organize Batches in a SQL Data Asset" to="/oss/guides/connecting_to_your_data/fluent/database/sql_data_assets" icon="/img/manage_sql_icon.svg" />
</LinkCardGrid>
