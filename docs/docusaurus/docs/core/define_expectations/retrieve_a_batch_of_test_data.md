---
title: Retrieve a Batch of sample data
description: Retrieve a Batch of data for testing Expectations or engaging in data exploration.
hide_table_of_contents: true
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import FromBatchDefinition from './_retrieve_a_batch_of_test_data/_from_a_batch_definition.md';
import FromPandasDefault from './_retrieve_a_batch_of_test_data/_from_pandas_default.md';

Expectations can be individually validated against a Batch of data.  This allows you to test newly created Expectations, or to create and validate Expectations to further your understanding of new data.  But first, you must retrieve a Batch of data to validate your Expectations against.

GX provides two methods of retrieving sample data for testing or data exploration.  The first is to request a Batch of data from any Batch Definition you have previously configured.  The second is to use the built in `pandas_default` Data Source to read in a Batch of data from a datafile such as a `.csv` or `.parquet` file without first defining a corresponding Data Source, Data Asset, and Batch Definition.


<Tabs queryString="method" groupId="method" defaultValue='from_a_batch_definition'>

   <TabItem value="from_a_batch_definition" label="Batch Definition">

      <FromBatchDefinition/>

   </TabItem>

   <TabItem value="from_pandas_default" label="pandas_default">

      <FromPandasDefault/>

   </TabItem>

</Tabs>