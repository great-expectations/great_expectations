---
sidebar_label: 'GX Cloud deployment patterns and architecture'
title: 'GX Cloud deployment patterns and architecture'
description: Learn more about GX Cloud deployment patterns and architecture.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

GX Cloud simplifies data quality management and monitoring. Its easy-to-use web interface lets you quickly validate your data without provisioning and maintaining complex infrastructure. With GX Cloud, you can work collaboratively with your teammates to define and test reusable data queries that alert you to changes in your data.

GX Cloud includes the following features and functionality:

- An intuitive web-based interface makes it easier and faster to set up your Expectations and run your Validations.

- Data Asset metrics give you a head-start on understanding your data and creating Expectations.

- Validation run history shows you how your data has changed over time, and makes troubleshooting failed Expectations simple.

- Scheduling your Validations in GX Cloud helps you get set up with production data quailty checks within minutes.

- GX's fully managed Cloud solution makes connecting to your data straightforward and secure.

## GX Cloud architecture

The following diagram provides an overview of the key GX Cloud architecture components:

![GX Cloud Architecture and Components](./architecture_deployment_images/gx_cloud_architecture_components.png)

### GX Cloud components

- **GX Cloud data storage** - Stores your organization's Data Source, Data Asset, Expectation Suite, and Checkpoint configurations and your organization's Validation run histories and Data Asset descriptive metrics.

- **GX Cloud web UI** - A web interface that allows you to manage and validate your organization's data quality without running Python code and enables shared visibility into your organization's Validation Results and Checkpoint run history. It's browser- and platform-independent.

- **GX Cloud API** - Provides a REST API to programmatically access and manage GX Cloud data and configurations. Both the GX open source software (OSS) client and the GX Agent use the GX Cloud API to query data from and send data to GX Cloud.

### Deployment components

- **Fully hosted** - GX manages a fully hosted Cloud environment that requires no code to get started with. Connections to your data are read-only, and use secure, encypted methods.

- **GX OSS client** - The Python library that powers GX Cloud and provides a Python client for programmatic access to GX Cloud. GX OSS contains the logic needed to test and document your organization's data, and you can also use it to create, manage, and interact with GX Cloud components.

- **GX Agent** - A utility that runs in your organization's deployment environment. While running, the GX Agent can receive tasks generated from the GX Cloud web UI, such as running a Checkpoint or fetching Metrics, and execute these tasks against your Data Assets.

## GX Cloud deployment patterns

GX Cloud deployments can be tailored to meet your specific business requirements. How your data is accessed and how you want your users to interact with GX Cloud determine which deployment pattern and GX architectural components are suitable for your organization.

### Fully hosted
In a fully hosted deployment, you get started with GX Cloud by connecting directly to your organizations's cloud data stores via the GX Cloud UI. This is the fastest path to setup.

### Org-hosted GX Agent

In an org-hosted deployment, you run the GX Agent in your organization's deployment environment. The GX Agent is an intermediary between GX Cloud and your organization's deployment environment data stores. Organization users can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud org-hosted deployment](./architecture_deployment_images/org_hosted_deployment.png)

### Self-hosted GX Agent

In a self-hosted deployment, you use Docker to run the GX Agent in your local environment. The GX Agent is an intermediary between GX Cloud and your local data stores. You can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud self-hosted deployment](./architecture_deployment_images/self_hosted_deployment.png)

### API-only

Use Python scripts and an orchestrator to integrate GX Cloud into your existing data pipeline. The GX OSS client can be used to access your organization's data stores and to store job results in GX Cloud. Users can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud orchestrated deployment](./architecture_deployment_images/orchestrated_deployment.png)

### Custom deployment patterns

The deployment patterns described previously are not an exhaustive collection of the ways you can deploy GX Cloud. Instead, these patterns are meant to illustrate the building blocks of a GX Cloud deployment and how your organization can configure and connect those blocks to form a functioning data quality ecosystem.

For example, you might opt to run and interact with the GX OSS client locally to define your Data Sources, Data Assets, Expectation Suites, and Checkpoints, run the GX OSS client in your Airflow pipeline to execute Checkpoints on a regular schedule, run the GX Agent in your production environment to fetch Data Asset metrics from the GX Cloud web UI, and access the GX Cloud web UI to view Validation and Checkpoint run histories.

GX provides flexible, robust products that allow your organization to quickly deploy GX Cloud or GX OSS to fit your unique requirements.

## GX Cloud workflow

The GX Cloud workflow is a sequence of tasks you complete to perform Data Validations. 

You connect GX Cloud to a Data Asset stored on your Data Source, you define and create an Expectation, and then you run a Validation on the data defined in the Expectation. Knowing that your data meets your Expectations helps you ensure your data is accurate and reliable and alerts you to issues before they become problematic.

The following table lists the sequence of tasks that you complete in a typical GX Cloud workflow. A brief description of the task and links to the relevant topics are provided.

| Task                                     | Description                                                        |
|------------------------------------------|--------------------------------------------------------------------|
| [Create a Data Asset](./data_assets/manage_data_assets.md#create-a-data-asset) | The Data Asset contains the data you want to examine and validate, and the Data Source is where the Data Asset is located. | 
| [Invite users](./users/manage_users.md#invite-a-user)    | Invite users to your GX Cloud organization.                       |
| [Create an Expectation](./expectations/manage_expectations.md)    | Define verifiable assertions about your data.                       |  
| [Run Validations](./validations/manage_validations.md)                          | Run a Validation to determine if your data meets the assertions in the Expectation.                       |
| (Optional) [Add and run Checkpoints](./checkpoints/manage_checkpoints.md)       | Add and run Checkpoints to refine and confirm your Data Validations. |

## Roles and responsibilities

How you interact with GX Cloud is dependent on the role you're assigned by your Admin. The following table lists GX Cloud roles and responsibilities. 

| Role          | Responsibilities                                  |
|---------------|---------------------------------------------------|
| Viewer        | View Checkpoints and Validation Results           | 
| Editor        | Create Data Assets<br/>Create and edit Expectations<br/>Create access tokens<br/>Create and edit Checkpoints |
| Admin         | Full access<br/>Perform all GX Cloud administrative functions including user and role assignment |

## Supported browsers

The following browsers are supported by GX Cloud:

- [Google Chrome](https://www.google.com/chrome/) — the latest version is fully supported

- [Mozilla Firefox](https://www.mozilla.org/en-US/firefox/) — the latest version is fully supported

- [Apple Safari](https://www.apple.com/safari/) — the latest version is fully supported

- [Microsoft Edge](https://www.microsoft.com/en-us/edge?ep=82&form=MA13KI&es=24) — the latest version is fully supported

### Browser session duration

A session is the period of time that you’re signed in to your GX Cloud account from a browser. If you close the browser, your session ends, and you're signed out. You'll need to sign in again to access GX Cloud.
