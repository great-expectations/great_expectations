---
sidebar_label: 'Connect GX Cloud to PostgreSQL'
title: 'Connect GX Cloud to PostgreSQL'
description: Connect GX Cloud to a PostgreSQL Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

To validate data stored in a PostgreSQL database from GX Cloud, you must add the GX Agent to your deployment environment. The GX Agent acts as an intermediary between GX Cloud and PostgreSQL and allows you to securely access and validate your data in GX Cloud.

New to GX Cloud and not sure that it's the right solution for your organization? See [Try GX Cloud](../try_gx_cloud.md).

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud) with [Admin or Editor permissions](../about_gx.md#roles-and-responsibilities).

- You have a PostgreSQL database, schema, and table.

- To improve data security, GX recommends creating a separate PostgreSQL user for your GX Cloud connection.

- You know your PostgreSQL access credentials.

- You have stopped all local running instances of the GX Agent.

## Prepare your PostgreSQL environment

Run the following code to create and assign the `gx_ro` role and allow GX Cloud to access to all `public` schemas and tables on a specific database:

```sql
   CREATE ROLE gx_ro WITH LOGIN PASSWORD <your_password>;
   GRANT CONNECT ON DATABASE <your_database> TO gx_ro;
   GRANT USAGE ON SCHEMA public TO gx_ro;
   GRANT SELECT ON ALL TABLES in SCHEMA public TO gx_ro;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gx_ro;
```

Replace `your_password` and `your_database` with your own values. `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gx_ro;` is optional and gives the `gx_ro` user access to all future tables in the defined schema.

## Get your GX Cloud access token and organization ID

You'll need your access token and organization ID to set your access credentials. Don't commit your access credentials to your version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy and then paste the user access token into a temporary file. The token can't be retrieved after you close the dialog.

6. Click **Close**.

7. Copy the value in the **Organization ID** field into the temporary file with your access token and then save the file. 

   GX recommends deleting the temporary file after you set the environment variables.

## Deploy the GX Agent

You deploy and run the GX Agent within your deployment environment. You can deploy the GX Agent container in any environment where you can run Docker container images.

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
        image: greatexpectations/agent:latest
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


## Next steps

- [Create a Data Asset](../data_assets/manage_data_assets.md#create-a-data-asset)

- [Invite users](../users/manage_users.md#invite-a-user)

