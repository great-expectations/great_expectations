---
sidebar_label: 'Deploy the GX Agent'
title: 'Deploy the GX Agent'
id: deploy_gx_agent
description: Deploy the GX Agent to use GX Cloud features and functionality.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

To use GX Cloud features and functionality in an org-hosted deployment, you need to deploy the GX Agent. If you're not ready to implement an org-hosted deployment, see [Try GX Cloud](../cloud/try_gx_cloud.md).

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud).

- You have a [Docker instance](https://docs.docker.com/get-docker/) or [kubectl](https://kubernetes.io/docs/tasks/tools/).


## Self-hosted and org-hosted deployments

To try GX Cloud, you use a [self-hosted deployment](./about_gx#self-hosted-deployment-pattern) to run the GX Agent with Docker, connect the GX Agent to your target Data Sources, and use the GX Cloud web UI to define your Data Assets, create Expectations, and run Validations. A self-hosted deployment is recommended when you want to test GX Cloud features and functionality, and it differs from the recommended [org-hosted deployment](./about_gx.md#org-hosted-deployment-pattern), in which the GX Agent runs in your organization's deployment environment.

## Get your user access token and copy your organization ID

You'll need your user access token and organization ID to deploy the GX Agent. Access tokens shouldn't be committed to version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

## Deploy the GX Agent

The GX Agent runs open source GX code in GX Cloud, and it allows you to securely access your data without connecting to it or interacting with it directly. To learn more about the GX Agent and deployment patterns, see [About GX Cloud](./about_gx.md).

If you're deploying and running the GX Agent within your deployment environment, you can deploy the GX Agent container in any environment where you can run Docker container images or create Kubernetes clusters.

To learn how to deploy a Docker container image in a specific deployment environment, see the following documentation:

- [Quickstart: Deploy a container instance in Azure using the Azure CLI](https://learn.microsoft.com/en-us/azure/container-instances/container-instances-quickstart)

- [Build and push a Docker image with Google Cloud Build](https://cloud.google.com/build/docs/build-push-docker-image)

- [Deploy Docker Containers on Amazon ECS](https://aws.amazon.com/getting-started/hands-on/deploy-docker-containers/)

You can deploy the GX Agent in any deployment environment in which you create Kubernetes clusters. For example:

- [Amazon Elastic Kubernetes Service (EKS)](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html)

- [Microsoft Azure Kubernetes Service (AKS)](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/containers/aks-start-here)

- [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine/docs)

- Any Kubernetes cluster version 1.21 or greater which uses standard Kubernetes

<Tabs
  groupId="deploy-gx-agent"
  defaultValue='docker'
  values={[
  {label: 'Docker', value:'docker'},
  {label: 'Kubernetes', value:'kubernetes'},
  ]}>
<TabItem value="docker">

1. Download the GX Agent Docker container image from [Docker Hub](https://hub.docker.com/r/greatexpectations/agent) to your local or deployment environment.

2. Run the following Docker command to start the GX Agent: 

   ```bash title="Terminal input"
   docker run -it \
   -e GX_CLOUD_ACCESS_TOKEN= YOUR_ACCESS_TOKEN \ 
   -e GX_CLOUD_ORGANIZATION_ID= YOUR_ORGANIZATION_ID \  
   greatexpectations/agent:latest
    ```
    Replace `YOUR_ACCESS_TOKEN` and `YOUR_ORGANIZATION_ID` with the values you copied previously.

3. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

4. Optional. Run the following command to use the GX Agent image as the base image and optionally add custom commands:

   ```bash title="Terminal input"
   FROM greatexpectations/agent
   RUN echo "custom_commands"
   ```
5. Optional. Run the following command to rebuild the Docker image:

   ```bash title="Terminal input"
   docker build -t myorg/agent
   ```
6. Optional. Run `docker ps` or open Docker Desktop to confirm the agent is running.

</TabItem>
<TabItem value="kubernetes">

1. Open kubectl and run the following command to provide the access credentials to the Kubernetes container:
    
   ```sh title="Terminal input"
   kubectl create secret generic gx-agent-secret \
   --from-literal=GX_CLOUD_ORGANIZATION_ID=YOUR_ORGANIZATION_ID \
   --from-literal=GX_CLOUD_ACCESS_TOKEN=YOUR_ACCESS_TOKEN \
   ```
    Replace `YOUR_ORGANIZATION_ID` and `YOUR_ACCESS_TOKEN` with the values you copied previously.

2. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

3. Create and save a file named `deployment.yaml`, with the following configuration:

   ```yaml title="deployment.yaml"
   apiVersion: apps/v1
   kind: Deployment
   metadata:
    name: gx-agent
    labels:
    app: gx-agent
    spec:
    replicas: 1
    selector:
    matchLabels:
    app: gx-agent
    template:
    metadata:
      labels:
        app: gx-agent
    spec:
      containers:
       name: gx-agent
        image: greatexpectations/agent:latest
        envFrom:
        secretRef:
         name: gx-agent-secret
   ```
5. Run the following command to use the `deployment.yaml`configuration file to deploy the GX Agent:

   ```sh title="Terminal input"
   kubectl apply -f deployment.yaml
   ```
6. Optional. Run the following command to confirm the agent is running:

   ```sh title="Terminal input"
   kubectl logs -l app=gx-agent
   ```
7. Optional. Run the following command to terminate running resources gracefully:

   ```sh title="Terminal input"
   kubectl delete -f deployment.yaml
   kubectl delete secret gx-agent-secret
   ```


</TabItem>
</Tabs>

## Next steps

- [Connect GX Cloud](../cloud/connect/connect_lp.md)

