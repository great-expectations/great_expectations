import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPython from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstallation from '../../_core_components/prerequisites/_gx_installation.md';
import PrereqDataContext from '../../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqDataSourceAndAssetConnectedToData from '../../_core_components/prerequisites/_data_source_asset_and_batch_definition.md';


## Prerequisites

- <PrereqPython/>.
- <PrereqGxInstallation/>.
- <PrereqDataContext/>.  These examples assume the variable `context` contains your Data Context.
- <PrereqDataSourceAndAssetConnectedToData/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

1. Retrieve your Batch Definition.

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve Batch Definition"
   ```

3. Optional. Specify the Batch to retrieve.



3. Retrieve a Batch of data.

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve most recent Batch"
   ```

   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - retrieve specific Batch"
   ```

5. Optional. Verify that the returned Batch is populated with records.


   ```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - verify populated Batch"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python" name="docs/docusaurus/docs/core/define_expectations/_retrieve_a_batch_of_test_data/_from_a_batch_definition.py - full example"
```

</TabItem>

</Tabs>