---
title: "Setup: Connect to data"
---
# [![Connect to data icon](../../images/universal_map/Outlet-active.png)](setup_overview.md) Setup: Overview 

import UniversalMap from '/docs/images/universal_map/_universal_map.mdx';

<!--Use 'inactive' or 'active' to indicate which Universal Map steps this term has a use case within.-->

<UniversalMap setup='inactive' connect='active' create='inactive' validate='inactive'/>

<!-- Only keep one of the 'To best understand this document' lines.  For processes like the Universal Map steps, use the first one.  For processes like the Architecture Reviews, use the second one. -->

:::note Prerequisites
- Completing [Step 2: Connect to data](/docs/tutorials/getting_started/connect_to_data) of the Getting Started tutorial is recommended.
:::
	
Connecting to your data in Great Expectations is designed to be a painless process.  Once you have performed this step, you will have a unified API with which to interact with your data in Great Expectations, regardless of the source data system.

## The connect to data process

<!-- Brief outline of what the process entails.  -->

Connecting to your data is built around the Datasource object.  A Datasource provides a standard API for accessing and interacting with data from a wide variety of source systems.  It does this by providing an interface for a Data Connector and an Execution Engine.  In the connect to data process you will configure your Datasources' Data Connectors according to the requirements of the source data system that contains the data you will be working with, along with specifications that will alow you to determine what slice of data your Datasources will provide access to.  At the same time, you will specify the Execution Engine that will be used to work with the data.  From that point forward you will only need to use the Datasource API to access and interact with your data, regardless of the original source system that your data is stored in.

<!-- The following subsections should be repeated as necessary.  They should give a high level map of the things that need to be done or optionally can be done in this process, preferably in the order that they should be addressed (assuming there is one). If the process crosses multiple steps of the Universal Map, use the <SetupHeader> <ConnectHeader> <CreateHeader> and <ValidateHeader> tags to indicate which Universal Map step the subsections fall under. -->

## 1. Create a Datasource
## 2. Specify your Execution Engine
## 3. Choose your Data Connector

Great Expectations provides three types of `DataConnector` classes, which are useful in various situations.  Which Data Connector you will want to use will depend on the format of your source data systems.
- An InferredAssetDataConnector infers the `data_asset_name` by using a regex that takes advantage of patterns that exist in the filename or folder structure.  If your source data system is designed so that it can easily be parsed by regex, this will allow new data to be included by the Datasource automatically.
- A ConfiguredAssetDataConnector, which allows you to have the most fine-tuning by requiring an explicit listing of each Data Asset you want to connect to.  This Data Connector would be ideal
- A `RuntimeDataConnector` which enables you to use a `RuntimeBatchRequest` to wrap either an in-memory dataframe, filepath, or SQL query.


## 3. Configure your Data Connector for a source system
## 4. Configure your Data Connector for slices of data

## Accessing your Datasource from your Data Context

## Retrieving Batches of data from your Datasource
This is primarily done when running Profilers in the the Create Expectation step, or when running Checkpoints in the Validate Data step, and will be covered in more detail in those sections of the documentation.

## Wrapping up

<!-- This section is essentially a victory lap.  It should reiterate what they have accomplished/are now capable of doing.  If there is a next process (such as the universal map steps) this should state that the reader is now ready to move on to it. -->

From here you will move on to the next step of working with Great Expectations: Create Expectations.