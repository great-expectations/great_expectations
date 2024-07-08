import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PandasOrSparkDataSource from './_pandas_or_spark_data_source.md';
import PandasDefault from './_pandas_default.md';

GX can leverage either pandas or Spark to read local and networked data from files such as `.csv` or `.parquet`.  GX also deploys with a built in `pandas_default` Data Source that can be used as a shortcut to quickly retrieve some data when creating new Expectations or engaging in data exploration.

<Tabs queryString="data_source_type" groupId="data_source_type" defaultValue='pandas_filesystem'>

   <TabItem value="pandas_default" label="pandas_default">
   <PandasDefault/>
   </TabItem>

   <TabItem value="pandas_filesystem" label="pandas">
   <PandasOrSparkDataSource/>
   </TabItem>

   <TabItem value="spark" label="Spark">
   <PandasOrSparkDataSource/>
   </TabItem>

</Tabs>
