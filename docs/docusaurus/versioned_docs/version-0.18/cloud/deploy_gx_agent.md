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

To use GX Cloud features and functionality, you need to deploy the GX Agent. The GX Agent is an intermediary between GX Cloud and your organization's data stores. GX Cloud does not connect directly to your data, all data access occurs within the GX Agent. GX Cloud sends jobs to the GX Agent, the GX Agent executes these jobs against your data, and then sends the job results to GX Cloud.

To test GX Cloud, you use a [self-hosted deployment](./about_gx#self-hosted-deployment-pattern) to run the GX Agent with Docker, connect the GX Agent to your target Data Sources, and use the GX Cloud web UI to define your Data Assets, create Expectations, and run Validations. When you've finished testing GX Cloud, you move to an [org-hosted deployment](./about_gx.md#org-hosted-deployment-pattern) and deploy the GX Agent to your organization's development, staging, or production cloud services environment. The GX Agent serves all GX Cloud users within your organization. It can be run as part of your development or production workflows. To learn more about the GX Agent and deployment patterns, see [About GX Cloud](./about_gx.md).

## Prerequisites

- You have a [GX Cloud account](https://greatexpectations.io/cloud).

## Get your access token and organization ID

You need your access token and organization ID to deploy the GX Agent. Access tokens shouldn't be committed to version control software. 

If you've used GX Cloud previously, you have your access token and organization ID, and you need to restart the GX Agent, see [Deploy the GX Agent](#deploy-the-gx-agent).

<Tabs
  groupId="copy-token"
  defaultValue='new'
  values={[
  {label: 'New GX Cloud account', value:'new'},
  {label: 'Existing GX Cloud account', value:'existing'},
  ]}>
<TabItem value="new">

1. Sign in to GX Cloud.

2. Complete the survey and then click **Continue to GX Cloud**.

3. Copy and then paste the **Access token** and **Organization ID** values into a temporary file. You'll need them to deploy the GX Agent.

4. Click **Deploy the GX Agent** and complete [Deploy the GX Agent](#deploy-the-gx-agent).

</TabItem>
<TabItem value="existing">

Use the information provided here to view your organization ID or create a new access token. This can be helpful if you've forgotten your organization ID or access token, and you need to restart the GX Agent.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your user access token and then save the file. 

    GX recommends deleting the temporary file after you set the environment variables.

8. [Deploy the GX Agent](#deploy-the-gx-agent).

</TabItem>
</Tabs>

## Deploy the GX Agent

The GX Agent allows you to securely access your data without connecting to it or interacting with it directly. To learn more about the GX Agent and deployment patterns, see [About GX Cloud](./about_gx.md).

### Prerequisites

- You have a GX Cloud access token and organization ID. See [Get your access token and organization ID](#get-your-access-token-and-organization-id). 

- You have a [Docker instance](https://docs.docker.com/get-docker/) or [kubectl](https://kubernetes.io/docs/tasks/tools/).

<Tabs
  groupId="deploy-agent"
  defaultValue='deployment'
  values={[
  {label: 'Org-hosted', value:'deployment'},
  {label: 'Self-hosted', value:'local'},
  ]}>
<TabItem value="deployment">

You can deploy the GX Agent container in any deployment environment where you can run Docker container images.

To learn how to deploy a Docker container image in a specific environment, see the following documentation:

- [Quickstart: Deploy a container instance in Azure using the Azure CLI](https://learn.microsoft.com/en-us/azure/container-instances/container-instances-quickstart)

- [Build and push a Docker image with Google Cloud Build](https://cloud.google.com/build/docs/build-push-docker-image)

- [Deploy Docker Containers on Amazon ECS](https://aws.amazon.com/getting-started/hands-on/deploy-docker-containers/)

You can deploy the GX Agent in any environment in which you create Kubernetes clusters. For example:

- [Amazon Elastic Kubernetes Service (EKS)](https://docs.aws.amazon.com/eks/latest/userguide/getting-started.html)

- [Microsoft Azure Kubernetes Service (AKS)](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/containers/aks-start-here)

- [Google Kubernetes Engine (GKE)](https://cloud.google.com/kubernetes-engine/docs)

- Any Kubernetes cluster version 1.21 or greater which uses standard Kubernetes

<Tabs
  groupId="connect-PostgreSQL"
  defaultValue='docker'
  values={[
  {label: 'Docker', value:'docker'},
  {label: 'Kubernetes', value:'kubernetes'},
  ]}>
<TabItem value="docker">

1. Download the GX Agent Docker container image from [Docker Hub](https://hub.docker.com/r/greatexpectations/agent).

2. After configuring your cloud service to run Docker containers, run the following Docker command to start the GX Agent: 

   ```bash title="Terminal input"
   docker run -it \
   -e GX_CLOUD_ACCESS_TOKEN= YOUR_ACCESS_TOKEN \ 
   -e GX_CLOUD_ORGANIZATION_ID= YOUR_ORGANIZATION_ID \  
   greatexpectations/agent:stable
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

1. Install kubectl. See [Install Tools](https://kubernetes.io/docs/tasks/tools/).

2. Run the following command to provide the access credentials to the Kubernetes container:
    
   ```sh
   kubectl create secret generic gx-agent-secret \
   --from-literal=GX_CLOUD_ORGANIZATION_ID=YOUR_ORGANIZATION_ID \
   --from-literal=GX_CLOUD_ACCESS_TOKEN=YOUR_ACCESS_TOKEN \
   ```
    Replace `YOUR_ORGANIZATION_ID` and `YOUR_ACCESS_TOKEN` with the values you copied previously.

3. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

4. Create and save a file named `deployment.yaml`, with the following configuration:

   ```yaml
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
        image: greatexpectations/agent:stable
        imagePullPolicy: Always
        envFrom:
        secretRef:
         name: gx-agent-secret
   ```
5. Run the following command to use the `deployment.yaml`configuration file to deploy the GX Agent:

   ```sh
   kubectl apply -f deployment.yaml
   ```
6. Optional. Run the following command to confirm the agent is running:

   ```sh
   kubectl logs -l app=gx-agent
   ```
7. Optional. Run the following command to terminate running resources gracefully:

   ```sh
   kubectl delete -f deployment.yaml
   kubectl delete secret gx-agent-secret
   ```

</TabItem>
</Tabs>
</TabItem>
<TabItem value="local">

1. Start the Docker Engine.

2. Run the following code to set the `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID` environment variables, install GX Cloud and its dependencies, and start the GX Agent:

    ```bash title="Terminal input"
    docker run --rm --pull=always -e GX_CLOUD_ACCESS_TOKEN="<user_access_token>" -e GX_CLOUD_ORGANIZATION_ID="<organization_id>" greatexpectations/agent
    ```
   Replace `user_access_token` and `organization_id` with the values you copied previously. 

3. In GX Cloud, confirm the GX Agent status is **Active Agent** and the icon is green. This indicates the GX Agent is active. If it isn't, repeat step 2 and confirm the `user_access_token` and `organization_id` values are correct.

    ![GX Agent status](/img/gx_agent_status.png)

4. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

5. Optional. Run `docker ps` or open Docker Desktop to confirm the agent is running.

    If you stop the GX Agent, close the terminal, and open a new terminal you'll need to set the environment variables again.

    To edit an environment variable, stop the GX Agent, edit the environment variable, save the change, and then restart the GX Agent.

</TabItem>
</Tabs>

## View GX Cloud logs

If you encounter an issue deploying the GX Agent or performing a GX Cloud task, review log information to troubleshoot the cause and determine a fix.

1. In GX Cloud, click **Logs**.

2. Click **Show log** next to a log entry to display additional log details.

3. Optional. Click **Hide log** to close the log details view.


## Next steps

Want to continue testing GX Cloud features and functionality?

- [Create a Data Asset](./data_assets/manage_data_assets.md#create-a-data-asset)

Ready to connect GX Cloud to your deployment environment?

- [Connect GX Cloud](./connect/connect_lp.md)
