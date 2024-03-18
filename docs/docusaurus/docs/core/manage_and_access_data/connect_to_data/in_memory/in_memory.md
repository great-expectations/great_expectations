---
title: "Connect to in-memory data"
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PandasPrerequisites from './_pandas/_prerequisites.md'
import PandasDataSource from './_pandas/_add_data_source.md'
import PandasDataAsset from './_pandas/_add_data_asset.md'
import PandasNextSteps from './_pandas/_next_steps.md'

import SparkPrerequisites from './_spark/_prerequisites.md'
import SparkDataSource from './_spark/_add_data_source.md'
import SparkDataAsset from './_spark/_add_data_asset.md'
import SparkNextSteps from './_spark/_next_steps.md'

Use the information provided here to connect to Data Assets that have been loaded into memory as a pandas or Spark dataframe.

<Tabs
  queryString="data-source"
  groupId="in-memory-data-source"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>

  <TabItem value="pandas">
  </TabItem>

  <TabItem value="spark">
  </TabItem>

</Tabs>

## Prerequisites

<Tabs
  queryString="data-source"
  groupId="in-memory-data-source"
  defaultValue='pandas'
  values={[
  {label: 'pandas dataframe', value:'pandas'},
  {label: 'Spark dataframe', value:'spark'},
  ]}>

  <TabItem value="pandas">
<PandasPrerequisites/>
  </TabItem>

  <TabItem value="spark">
<SparkPrerequisites/>
  </TabItem>

</Tabs>

## Create a Data Source

<Tabs
  queryString="data-source"
  groupId="in-memory-data-source"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>

  <TabItem value="pandas">
<PandasDataSource/>
  </TabItem>

  <TabItem value="spark">
<SparkDataSource/>
  </TabItem>

</Tabs>

## Add a Data Asset to a Data Source

<Tabs
  queryString="data-source"
  groupId="in-memory-data-source"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>

  <TabItem value="pandas">
<PandasDataAsset/>
  </TabItem>

  <TabItem value="spark">
<SparkDataSource/>
  </TabItem>

</Tabs>

## Next steps

<Tabs
  queryString="data-source"
  groupId="in-memory-data-source"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>

  <TabItem value="pandas">
<PandasNextSteps/>
  </TabItem>

  <TabItem value="spark">
<SparkNextSteps/>
  </TabItem>

</Tabs>