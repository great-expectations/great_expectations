---
title: Create a Validation Definition
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import GxData from '../_core_components/_data.jsx'
import PrereqPythonInstalled from '../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../_core_components/prerequisites/_gx_installation.md';
import PrereqPreconfiguredDataContext from '../_core_components/prerequisites/_preconfigured_data_context.md';
import PrereqPreconfiguredDataSourceAndAssetAndBatchDefinition from '../_core_components/prerequisites/_data_source_asset_and_batch_definition.md';
import PrereqPreconfiguredExpectationSuiteAndExpectations from '../_core_components/prerequisites/_expectation_suite_with_expectations.md';

import StepRequestADataContext from '../_core_components/common_steps/_request_a_data_context.md';


A Validation Definition is a fixed reference that links a Batch of data to an Expectation Suite. It can be run by itself to validate the referenced data against the associated Expectations for testing or data exploration.  Multiple Validation Definitions can also be provided to a Checkpoint which, when run, executes Actions based on the Validation Results for each provided Validation Definition.

<h2>Prerequisites</h2>

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqPreconfiguredDataContext/>. In this guide the variable `context` is assumed to contain your Data Context.
- <PrereqPreconfiguredDataSourceAndAssetAndBatchDefinition/>.
- <PrereqPreconfiguredExpectationSuiteAndExpectations/>.

### Procedure

<Tabs 
   queryString="procedure"
   defaultValue="instructions"
   values={[
      {value: 'instructions', label: 'Instructions'},
      {value: 'sample_code', label: 'Sample code'}
   ]}
>

<TabItem value="instructions" label="Instructions">

1. Retrieve an Expectation Suite with Expectations.

   Update the value of `expectation_suite_name` in the following code with the name of your Expectation Suite.  Then execute the code to retrieve that Expectation Suite:

   ```python title="Python" name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - retrieve an Expectation Suite"
   ```

2. Retrieve the Batch Definition that describes the data to associate with the Expectation Suite.

   Update the values of `data_source_name`, `data_asset_name`, and `batch_definition_name` in the following code with the names of your previously defined Data Source, one of its Data Assets, and a Batch Definition for that Data Asset.  Then execute the code to retrieve the Batch Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - retrieve a Batch Definition"
   ```

3. Create a `ValidationDefinition` instance using the Batch Definition, Expectation Suite, and a unique name.

   Update the value of `definition_name` with a descriptive name that indicates the purpose of the Validation Definition.  Then execute the code to create your Validation Definition:

   ```python title="Python" name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - create a Validation Definition"
   ```

4. Optional. Save the Validation Definition to your Data Context.

   ```python title="Python" name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - save the Validation Definition to the Data Context"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

```python showLineNumbers title="Python" name="docs/docusaurus/docs/core/run_validations/_examples/create_a_validation_definition.py - full code example"
```

</TabItem>

</Tabs>