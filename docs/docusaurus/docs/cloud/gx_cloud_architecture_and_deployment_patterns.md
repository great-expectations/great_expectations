---
sidebar_label: 'GX Cloud Architecture and Deployment Patterns'
title: 'GX Cloud Architecture and Deployment Patterns'
description: Learn more about the components of GX Cloud architecture and deployment patterns
toc_min_heading_level: 2
toc_max_heading_level: 2
---

GX Cloud provides a secure, fully-managed SaaS solution to monitor, manage, and share your data quality ecosystem. GX Cloud offers flexible deployment patterns that accommodate your environment and supported data sources.

## GX Cloud Architecture

GX Cloud's architecture is depicted in the below diagram and is comprised of the following key components:

![GX Cloud Architecture and Components](./architecture_deployment_images/gx_cloud_architecture_components.png)

- **Cloud storage for data quality configuration, metadata, and history.** GX Cloud provides cloud storage for the Data Source, Data Asset, Expectation Suite, and Checkpoint configurations that comprise your GX workflows. Additionally, GX Cloud stores your Validation run histories and Data Asset descriptive metrics.

- **GX Cloud Web App.** GX Cloud includes a web app that is browser- and platform-independent. The web app enables you to manage and validate your data without running Python code, and provides shared visibility into your Validation Results and Checkpoint run history.

- **GX Cloud REST API.** The GX REST API provides an interface to programmatically access and manage GX Cloud data and configurations. Both GX OSS and the GX Agent utilize the GX REST API to query data from and send data to GX Cloud. The API is not currently publicly documented.

- **GX Cloud AMQP Broker.** The GX Cloud Advanced Message Queuing Protocol (AMQP) broker enables connection between GX Cloud and your locally running GX Agent.

GX Cloud operates in tandem with two components that are run within your environment:

- **GX OSS.** GX open source software (OSS) is the Python library that powers all GX deployments, both Cloud and local. GX Core contains the logic needed to test and document your data. You can use GX Core to interact with your organization's GX Cloud entities.

- **GX Agent.** The GX Agent is a utility that runs locally in your environment. When running, the GX Agent can receive tasks generated from the GX Cloud Web App, such as running a Checkpoint or fetching Column Descriptive Metrics and execute these tasks against your data sources.


## Deployment Patterns

GX Cloud deployment is flexible and can be tailored to your organization's needs. The deployment pattern that you employ is driven by two factors:
-  Whether or not you want to interact with GX OSS locally, or just use the GX Cloud Web App
-  Where and how you want to run the GX Agent. The GX Agent can be run:
    - in your local environment, as a Python program
    - In your local environment, as a Docker container


### Run the GX Agent locally and interact with the GX Cloud Web App only

### Run the GX Agent locally and interact with both the GX Cloud Web App and GX OSS


![GX Cloud deployment with GX Agent and GX OSS running in a local Python environment](./architecture_deployment_images/deploy_gx_agent_python.png)

Alternatively, the GX Agent can be run as a Docker container in your environment:
![GX Cloud deployment with GX Agent running as a Docker container and GX OSS running in a local Python environment](./architecture_deployment_images/deploy_gx_agent_docker.png)


### Run GX OSS within an orchestrator
