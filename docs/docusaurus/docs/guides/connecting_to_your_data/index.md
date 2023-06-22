---
title: "Connect to Data"
pagination_next: null
pagination_prev: null
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import FilesystemLinks from '/docs/components/connect_to_data/link_lists/_connect_to_filesystem_data_fluently.md'
import InMemoryLinks from '/docs/components/connect_to_data/link_lists/_connect_to_in_memory_data_fluently.md'
import SqlLinks from '/docs/components/connect_to_data/link_lists/_connecting_to_sql_datasources_fluently.md'
import DataAssetLinks from '/docs/components/connect_to_data/link_lists/_work_with_fluent_data_assets.md'

<Tabs
  groupId="yaml-or-python"
  defaultValue='filesystem'
  values={[
  {label: 'Filesystem Datasources', value:'filesystem'},
  {label: 'In-memory Datasources', value:'in-memory'},
  {label: 'SQL Datasources', value:'sql'},
  {label: 'Data Assets', value:'data assets'},
  ]}>

  <TabItem value="filesystem">

  <FilesystemLinks />

  </TabItem>

  <TabItem value="in-memory">

  <InMemoryLinks />

  </TabItem>

  <TabItem value="sql">

  <SqlLinks />

  </TabItem>


  <TabItem value="data assets">

  <DataAssetLinks />

  </TabItem>

</Tabs >