---
title: 'GX Cloud overview'
id: gx_cloud_overview
description: Explore GX Cloud components and workflows.
---

GX Cloud is a fully managed SaaS platform that simplifies data quality management and monitoring. With GX Cloud, you and your organization can work collaboratively to define shared understanding of your data.

## GX Cloud components

The key GX Cloud components are the front end web UI, storage for your organization's 

| GX Cloud component | Description |
| :--- | :--- |
| ![GX Cloud web UI](https://placehold.co/200x200) | **GX Cloud web UI**<br/>A web interface that allows you to manage and validate your organization's data quality without running Python code and enables shared visibility into your organization's Validation Results and Checkpoint run history. It's browser- and platform-independent. |
| ![GX Cloud data storage](https://placehold.co/200x200) | **GX Cloud data storage**<br/>Stores the configurations for your organization's Data Sources, Data Assets, Expectations, and Validations alongside your organization's Validation Result histories and Data Asset descriptive metrics. |
| ![GX Cloud backend](https://placehold.co/200x200) | **GX Cloud backend**<br/>Handles data connections and queries based on your [GX Cloud deployment pattern](/cloud/deploy/deployment_patterns.md).|
| ![GX Core Python client](https://placehold.co/200x200) | **GX Core Python client**<br/>The GX Core Python client enables you to interact programmatically with your GX Cloud organization. It serves as a way to interact with your GX Cloud entities and can complement and extend your UI-created workflows.|


## GX Cloud terminology

Data Asset

Expectation

Validation

Validation Result


## GX Cloud workflow

The GX Cloud workflow is a sequence of tasks you complete to perform data validations. The basic GX Cloud workflow is:

![Basic GX Cloud workflow](./overview_images/gx_cloud_workflow.png)

1. Connect to data

2. Create a Data Asset

3. Create Expectations

4. Validate your data

5. Review and share Validation Results



There are a variety of GX Cloud features that support additional enhancements in your GX Cloud workflow.

* Schedule Validations. GX Cloud enables you to schedule validations, so that you can test and assess your data on a regular, ongoing cadence and monitor quality over time.

* Alerting. GX Cloud provides the ability send alerts when validations fail.

* Data Asset profiling. GX Cloud introspects your data schema by default on Asset creation, and also offers one-click fetching of additional descriptive metrics including column type and statistical summaries.

* Inviting users to your GX Cloud organization

* Share Validation Results

