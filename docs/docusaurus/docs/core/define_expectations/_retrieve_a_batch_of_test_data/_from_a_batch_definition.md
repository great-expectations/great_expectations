import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPython from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqDataSourceAndAssetConnectedToData from '../../_core_components/prerequisites/_data_source_asset_and_batch_definition.md';

Batch Definitions both organize a Data Asset's records into Batches and provide a method for retrieving those records.  Any Batch Definition can be used to retrieve a Batch of records for use in testing Expectations or data exploration.

## Prerequisites

- <PrereqPython/>.
- <PrereqGxInstallation/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- <PrereqDataSourceAndAssetConnectedToData/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve your Batch Definition.

   Update the values of `data_source_name`, `data_asset_name`, and `batch_definition_name` in the following code and execute it to retrieve your Batch Definition from the Data Context:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve Batch Definition"
   ```

2. Optional. Specify the Batch to retrieve.

   Some Batch Definitions can only provide a single Batch.  Whole table batch definitions on SQL Data Assets, file path and whole directory Batch Definitions on filesystem Data Assets, and all Batch Definitions for dataframe Data Assets will provide all of the Data Asset's records as a single Batch.  For these Batch Definitions there is no need to specify which Batch to retrieve because there is only one available.

   Yearly, monthly, and daily Batch Definitions subdivide the Data Asset's records by date.  This allows you to retrieve the data corresponding to a specific date from the Data Asset.  If you do not specify a Batch to retireve, these Batch Definitions will return the first valid Batch they find.  By default, this will be the most recent Batch (sort ascending) or the oldest Batch if the Batch Definition has been configured to sort descending.

   :::note Sorting of records with invalid dates
   
   Records that are missing the date information necessary to be sorted into a Batch will be treated as the "oldest" records and will be returned first when a Batch Definition is set to sort descending.
   
   :::

   You are not limited to retrieving only the most recent (or oldest, if the Batch Definition is set to sort descending) Batch.  You can also request a specific Batch by providing a Batch Parameter dictionary.

   The Batch Parameter dictionary is a dictionary with keys indicating the `year`, `month`, and `day` of the data to retrieve and with values corresponding to those date components.  

   Which keys are valid Batch Parameters depends on the type of date the Batch Definition is configured for:

   - Yearly Batch Definition accept the key `year`.
   - Monthly Batch Definition accept the keys `year` and `month`.
   - Daily Batch Definition accept the keys `year`, `month`, and `day`.

   If a Batch Definition is missing a key, the returned Batch will be the first Batch (as determined by the Batch Definition's sort ascending or sort descending configuration) that matches the date components that were provided.

   The following are some sample Batch Parameter dictionaries for progressively more specific dates:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - example Batch Parameters"
   ```

3. Retrieve a Batch of data.

   The Batch Definition's `.get_batch(...)` method is used to retrieve a Batch of Data.  The Batch Parameters provided to this method will determine if the first valid Batch is returned, or a Batch for a specific date is returned.

   <Tabs queryString="use_batch_parameters" groupId="use_batch_parameters" defaultValue='false'>

   <TabItem value="false" label="First valid Batch">

   Execute the following code to retrieve the first available Batch from the Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve most recent Batch"
   ```

   </TabItem>

   <TabItem value="true" label="Specific Batch">

   Update the Batch Parameters in the following code and execute it to retrieve a specific Batch from the Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve specific Batch"
   ```

   </TabItem>

   </Tabs>

4. Optional. Verify that the returned Batch is populated with records.

   You can verify that your Batch Definition was able to read in data and return a populated Batch by printing the header and first few records of the returned Batch:

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - verify populated Batch"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - full example"
```

</TabItem>

</Tabs>