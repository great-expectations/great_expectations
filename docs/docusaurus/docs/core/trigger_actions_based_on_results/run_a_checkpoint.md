---
title: Run a Checkpoint
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqCheckpoint from '../_core_components/prerequisites/_checkpoint.md';

<h2>Prerequisites</h2>
- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>.
- <PrereqCheckpoint/>.

<Tabs>

<TabItem value="procedure" label="Procedure">

In this procedure your Data Context is assumed to be stored in the variable `context` and your Checkpoint is assumed to be stored in the variable `checkpoint`.

1. Optional. Define Batch Parameters.

2. Optional. Define Expectation Parameters.

3. Run the Checkpoint.

   At runtime, a Checkpoint can take in dictionaries that filter the Batches in each Validation Definition and modify the parameters of the Expectations that will be validated against them. The parameters apply to every Validation Definition in the Checkpoint.  Therefore, the Validation Definitions grouped in a Checkpoint should have Batch Definitions that accept the same Batch filtering criteria.

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python title="Python"
import great_expectations as gx

context = gx.get_context()
checkpoint = context.checkpoints.get("my_checkpoint")

batch_parameters = {}
expectation_parameters = {}
 
```

</TabItem>

</Tabs>