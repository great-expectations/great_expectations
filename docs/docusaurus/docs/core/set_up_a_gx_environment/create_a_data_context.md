---
title: Create a Data Context
hide_table_of_contents: true
---
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

import GxData from '../_core_components/_data.jsx'

import QuickDataContext from './_create_a_data_context/_quick_start.md'
import FileDataContext from './_create_a_data_context/_file_data_context.md'
import EphemeralDataContext from './_create_a_data_context/_ephemeral_data_context.md'

A Data Context defines the storage location for metadata, such as your configurations for Data Sources, Expectation Suites, Checkpoints, and Data Docs. It also contains your Validation Results and the metrics associated with them, and it provides access to those objects in Python, along with other helper functions. 

All scripts that utilize GX Core should start with the creation of a Data Context.

`get_context()` also selects the process-wide current Data Context. An object that belongs to no Data Context resolves through the current Data Context. A process must obtain at least one Data Context through `get_context()`, or select one through `set_context()`, before it uses objects that belong to a Data Context, because a Data Context that you construct directly, such as `EphemeralDataContext(...)` or `FileDataContext(...)`, does not become the current Data Context and some operations still consult the current Data Context for settings that do not depend on which Data Context owns an object.

An object that you obtain from a Data Context, or add to one, resolves through that Data Context. One lookup goes through the current Data Context by design: when a query runs against a second Data Source, that Data Source is looked up by name in the current Data Context. This applies to the comparison Data Source of `ExpectQueryResultsToMatchComparison` and to the `data_source_name` of the `QueryDataSourceTable` metric.

The following are the available Data Context types:

- **File Data Context:** A persistent Data Context that stores metadata and configuration information as YAML files within a file system. File Data Contexts allow you to re-use previously configured Expectation Suites, Data Sources, and Checkpoints.

- **Ephemeral Data Context:** A temporary Data Context that stores metadata and configuration in memory and does not persist beyond the current Python session. Ephemeral Data Contexts are a good fit when you don't need your GX configuration to persist between runs, such as:
  - Exploring data without saving results
  - Running validations in CI where the environment is recreated each run
  - Working in read-only or disposable compute (such as containers or hosted notebooks) with no persistent file system

<Tabs queryString="context_type" groupId="context_type" defaultValue='quick' values={[{label: 'Quick Start', value:'quick'}, {label: 'File', value:'file'}, {label: 'Ephemeral', value:'ephemeral'}]}>

<TabItem value="quick" label="Quick Start">
<QuickDataContext/>
</TabItem>

<TabItem value="file" label="File">
<FileDataContext/>
</TabItem>

<TabItem value="ephemeral" label="Ephemeral">
<EphemeralDataContext/>
</TabItem>

</Tabs>