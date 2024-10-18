---
title: Configure Data Docs
description: Configure the location where static Data Docs webpages are created.
hide_feedback_survey: false
hide_title: false
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import PrereqPythonInstalled from '../../_core_components/prerequisites/_python_installation.md';
import PrereqGxInstalled from '../../_core_components/prerequisites/_gx_installation.md'
import PrereqFileDataContext from '../../_core_components/prerequisites/_file_data_context.md'

import EnvironmentFilesystem from './_backends/_local_or_networked.md'

Data Docs translate Expectations, Validation Results, and other metadata into human-readable documentation that is saved as static web pages. Automatically compiling your data documentation from your data tests in the form of Data Docs keeps your documentation current.  This guide covers how to configure additional locations where Data Docs should be created.

### Prerequisites:

- <PrereqPythonInstalled/>.
- <PrereqGxInstalled/>.
- <PrereqFileDataContext/>.  This guide assumes the variable `context` contains your Data Context.

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

1. Define a configuration dictionary for your new Data Docs site.

   GX writes Data Doc sites to a directory specified by the `base_directory` key of the configuration dictionary.
   Configuring other keys of the dictionary is not supported, and they may be removed in a future release.

   <EnvironmentFilesystem/>

2. Add your configuration to your Data Context.

   All Data Docs sites have a unique name within a Data Context. Once your Data Docs site configuration has been defined, add it to the Data Context by updating the value of `site_name` in the following to something more descriptive and then execute the code::

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - add data docs config to Data Context"
   ```

3. Optional. Build your Data Docs sites manually.

   You can manually build a Data Docs site by executing the following code:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - manually build Data Docs"
   ```

4. Optional. Automate Data Docs site updates with Checkpoint Actions.

   You can automate the creation and update of Data Docs sites by including the `UpdateDataDocsAction` in your Checkpoints.  This Action will automatically trigger a Data Docs site build whenever the Checkpoint it is included in completes its `run()` method.

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - automate data docs with a Checkpoint Action"
   ```

5. Optional. View your Data Docs.

   Once your Data Docs have been created, you can view them with:

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - view data docs"
   ```

</TabItem>

<TabItem value="sample_code" label="Sample code">

   ```python title="Python" name="docs/docusaurus/docs/core/configure_project_settings/configure_data_docs/_examples/data_docs_local_or_networked.py - full code example"
   ```

</TabItem>

</Tabs>

