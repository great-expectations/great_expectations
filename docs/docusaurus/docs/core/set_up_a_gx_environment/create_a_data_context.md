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
import CloudDataContext from './_create_a_data_context/_cloud_data_context.md'

A Data Context defines the storage location for metadata, such as your configurations for Data Sources, Expectation Suites, Checkpoints, and Data Docs. It also contains your Validation Results and the metrics associated with them, and it provides access to those objects in Python, along with other helper functions for the GX Python API. 

All scripts that utilize GX Core should start with the creation of a Data Context.

The following are the available Data Context types:

- **File Data Context:** A persistent Data Context that stores metadata and configuration information as YAML files within a file system. File Data Contexts allow you to re-use previously configured Expectation Suites, Data Sources, and Checkpoints.

- **Ephemeral Data Context:** A temporary Data Context that stores metadata and configuration information in memory. This Data Context will not persist beyond the current Python session. Ephemeral Data Contexts are useful when you don’t have write permissions to a file system or if you are going to engage in data exploration without needing to save your results.

- **GX Cloud Data Context:** A Data Context that connects to a GX Cloud Account to retrieve and store GX Cloud metadata and configuration information. The GX Cloud Data Context lets you leverage GX Cloud to share your Expectation Suites, Data Sources, and Checkpoints with your organization.

<Tabs queryString="context_type" groupId="context_type" defaultValue='quick' values={[{label: 'Quick Start', value:'quick'}, {label: 'File', value:'file'}, {label: 'Ephemeral', value:'ephemeral'}, {label: 'GX Cloud', value:'gx_cloud'}]}>

<TabItem value="quick" label="Quick Start">
<QuickDataContext/>
</TabItem>

<TabItem value="file" label="File">
<FileDataContext/>
</TabItem>

<TabItem value="ephemeral" label="Ephemeral">
<EphemeralDataContext/>
</TabItem>

<TabItem value="gx_cloud" label="GX Cloud">
<CloudDataContext/>
</TabItem>

</Tabs>