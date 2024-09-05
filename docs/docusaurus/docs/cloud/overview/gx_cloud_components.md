---
title: 'GX Cloud components'
id: gx_cloud_components
description: Explore GX Cloud components.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

# GX Cloud components


GX Cloud simplifies data quality management and monitoring. Its easy-to-use web interface lets you quickly validate your data without provisioning and maintaining complex infrastructure. With GX Cloud, you can work collaboratively with your teammates to define and test reusable data queries that alert you to changes in your data.

GX Cloud includes the following features and functionality:

- An intuitive web-based interface makes it easier and faster to set up your Expectations and run your Validations.

- Data Asset metrics give you a head-start on understanding your data and creating Expectations.

- Validation run history shows you how your data has changed over time, and makes troubleshooting failed Expectations simple.

- Scheduling your Validations in GX Cloud helps you get set up with production data quailty checks within minutes.

- As a fully managed solution, GX Cloud makes connecting to your data straightforward and secure.

## GX Cloud architecture

The following diagram provides an overview of the key GX Cloud architecture components:

![GX Cloud Architecture and Components](../architecture_deployment_images/gx_cloud_architecture_components.png)

### GX Cloud components

- **GX Cloud data storage** - Stores your organization's Data Source, Data Asset, Expectation Suite, and Checkpoint configurations and your organization's Validation run histories and Data Asset descriptive metrics.

- **GX Cloud web UI** - A web interface that allows you to manage and validate your organization's data quality without running Python code and enables shared visibility into your organization's Validation Results and Checkpoint run history. It's browser- and platform-independent.

- **GX Cloud API** - Provides a REST API to programmatically access and manage GX Cloud data and configurations. Both the GX Core Python library and the GX Agent use the GX Cloud API to query data from and send data to GX Cloud.