---
sidebar_label: 'About GX Cloud'
title: 'About GX Cloud'
description: Learn more about the features and benefits of GX Cloud.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

GX Cloud builds on the innovative features of GX open source software (OSS). Its easy-to-use web interface lets you quickly validate your data without creating and running complex Python code. With GX Cloud you can work collaboratively with your teammates to define and test reusable data queries that alert you to changes in your data.

While it is possible to design and deploy a functioning data quality ecosystem with GX OSS, GX Cloud simplifies infrastructure and deployment significantly. With GX Cloud, you'll save time, money, and effort implementing your data quality management and monitoring solution.

Here are just a few of the benefits you'll get with GX Cloud:

- It's web-based and browser- and platform-independent.

- A hosted environment and intuitive interface makes it faster to set up and run your data Validations.

- Features such as Validation run histories, Data Asset metrics, and more.

- No more need to create, run, and maintain complex code for your data queries.

- No more need to host GX OSS Data Docs. GX Cloud takes care of hosting and allows your organization to focus on managing and monitoring data.

- Hosted backend storage for GX deployments. With GX OSS, you need to set up and maintain a collection of required backend stores, including stores for your GX component configurations, data quality metadata, and data validation history. GX Cloud removes the need to deploy and maintain this additional internal infrastructure&mdash;with GX Cloud, your backend stores are fully-managed.

## GX Cloud architecture

The following diagram provides an overview of the key GX Cloud architecture components:

![GX Cloud Architecture and Components](./architecture_deployment_images/gx_cloud_architecture_components.png)

### GX Cloud components

- **GX Cloud data storage** - Stores your organization's Data Source, Data Asset, Expectation Suite, and Checkpoint configurations and your organization's Validation run histories and Data Asset descriptive metrics.

- **GX Cloud web UI** - A web interface that allows you to manage and validate your organization's data quality without running Python code and enables shared visibility into your organization's Validation Results and Checkpoint run history. It's browser- and platform-independent.

- **GX Cloud API** - Provides a REST API to programmatically access and manage GX Cloud data and configurations. Both the GX open source software (OSS) client and the GX Agent use the GX Cloud API to query data from and send data to GX Cloud. Documentation for the GX Cloud API is not currently available, and it is not yet intended for use outside the GX OSS client and the GX Agent.

- **GX Cloud message broker** - Enables communication between GX Cloud and the GX Agent.

### Deployment components

- **GX OSS client** - The Python library that powers GX Cloud and provides a Python client for programmatic access to GX Cloud. GX OSS contains the logic needed to test and document your organization's data, and you can also use it to create, manage, and interact with GX Cloud components.

- **GX Agent** - A utility that runs in your organization's deployment environment. While running, the GX Agent can receive tasks generated from the GX Cloud web UI, such as running a Checkpoint or fetching Metrics, and execute these tasks against your Data Assets.

## GX Agent

The GX Agent is an intermediary between GX Cloud and your organization's data stores. GX Cloud does not connect directly to your data, all data access occurs within the GX Agent. GX Cloud sends jobs to the GX Agent, the GX Agent executes these jobs against your data, and then sends the job results to GX Cloud.

The GX Agent is typically deployed in your organization's deployment environment, for example, in a development, staging, or production cloud services environment. The GX Agent serves all GX Cloud users within your organization. It can be run as part of your development or production workflows. See [Connect GX Cloud](./connect/connect_lp.md) for setup instructions.

### GX Agent versioning

GX uses a date-based versioning format for its weekly GX Agent releases. For example, `YYYYMMMDD`. GX appends `stable` or `dev` to the GX Agent version to identify the release type. The `stable` identifier indicates the release is fully tested and ready for use. The `dev` identifier indicates the release is pre-production and untested. This documentation assumes you are using the latest `stable` GX Agent release.


## GX Cloud deployment patterns

GX Cloud deployments can be tailored to meet your specific business requirements. To determine which deployment pattern and GX architectural components are most suitable for your organization, determine how your data is accessed and how users interact with GX Cloud.

The following table shows the connection options.

| Deployment pattern | Environment | GX Agent | GX OSS client |
| :-- | :-- | :--: | :--: |
| Org-hosted | Deployment | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> | |
| Self-hosted | Local | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> | |
| Orchestrated | Deployment or local | | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> |

The following table shows how users can access GX Cloud.

| Access method | GX Cloud | GX OSS client |
| :-- | :--: | :--: |
| UI | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> | |
| Python API | | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div>  |
| UI and Python API | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> | <div align="center"><img alt="Yes" src="/img/simple_orange_circle.svg"/></div> |


### Org-hosted deployment pattern
In an org-hosted deployment, you run the GX Agent in your organization's deployment environment, for example, development or production. The GX Agent is an intermediary between GX Cloud and your organization's deployment environment data stores. Organization users can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud org-hosted deployment](./architecture_deployment_images/org_hosted_deployment.png)

### Self-hosted deployment pattern
In a self-hosted deployment, you use Docker to run the GX Agent in your local environment. The GX Agent is an intermediary between GX Cloud and your local data stores. You can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud self-hosted deployment](./architecture_deployment_images/self_hosted_deployment.png)

### Orchestrated deployment pattern
In an orchestrated deployment pattern, you use an orchestrator to integrate GX Cloud into your existing data pipeline. You use the GX OSS client to access your organization's data stores and to store job results in GX Cloud. Users can interact with GX Cloud using the web UI, the GX OSS client, or both.

![GX Cloud orchestrated deployment](./architecture_deployment_images/orchestrated_deployment.png)

### Custom deployment patterns

The deployment patterns described previously are not an exhaustive collection of the ways you can deploy GX Cloud. Instead, these patterns are meant to illustrate the building blocks of a GX Cloud deployment and how your organization can configure and connect those blocks to form a functioning data quality ecosystem.

For example, you might opt to run and interact with the GX OSS client locally to define your Data Sources, Data Assets, Expectation Suites, and Checkpoints, run the GX OSS client in your Airflow pipeline to execute Checkpoints on a regular schedule, run the GX Agent in your production environment to fetch Data Asset metrics from the GX Cloud web UI, and access the GX Cloud web UI to view Validation and Checkpoint run histories.

GX provides flexible, robust products that allow your organization to quickly deploy GX Cloud or GX OSS to fit your unique requirements.

### Versioning

GX uses a date-based versioning format for its weekly GX Agent Docker image releases. For example, `YYYYMMMDD.#` for stable releases or `YYYYMMDD.#.dev#` for pre-releases. GX uses the `stable` and `dev` Docker image tags to identify the release type. The `stable` tag indicates the image is fully tested and ready for use. The `dev` tag indicates a pre-release image. This documentation assumes you're using the latest `stable` GX Agent Docker image.


## GX Cloud workflow

The GX Cloud workflow is a sequence of tasks you complete to perform Data Validations. 

You connect GX Cloud to a Data Asset stored on your Data Source, you define and create an Expectation, and then you run a Validation on the data defined in the Expectation. Knowing that your data meets your Expectations helps you ensure your data is accurate and reliable and alerts you to issues before they become problematic.

The following table lists the sequence of tasks that you complete in a typical GX Cloud workflow. A brief description of the task and links to the relevant topics are provided.

| Task                                     | Description                                                        |
|------------------------------------------|--------------------------------------------------------------------|
|[Connect GX Cloud](./connect/connect_lp.md) | Set up your deployment environment to get the most out of GX Cloud. Do it once, and you don't need to do it again.                                   | 
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
