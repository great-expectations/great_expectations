---
sidebar_label: 'Connect GX Cloud to Snowflake'
title: 'Connect GX Cloud to Snowflake'
description: Connect GX cloud to a Snowflake Data Source.
---

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

To validate data stored in a Snowflake data warehouse, you must add the GX Agent to your production environment. The GX Agent is a containerized image that runs open source and proprietary GX code. The GX Agent acts as an intermediary between GX Cloud and Snowflake and allows you to securely access and validate your data in GX Cloud without connecting or interacting with it directly.

## Prerequisites

- You have a [GX Cloud Beta account](https://greatexpectations.io/cloud).

- You have a Snowflake database, schema, and table.

- You have a [Snowflake account](https://docs.snowflake.com/en/user-guide-admin) with USAGE privileges on the table, database, and schema you are validating, and you have SELECT privileges on the table you are validating. To improve data security, GX recommends using a separate Snowflake user service account to connect to GX Cloud.

- You know your Snowflake password.

## Prepare your Snowflake environment

You can use an existing Snowflake warehouse, but GX recommends creating a separate warehouse for GX Cloud to simplify cost management and optimize performance.

1. In Snowflake Snowinsight, click **Worksheets** > **Add** > **SQL Worksheet**.

2. Copy and paste the following code into the SQL worksheet:

   ```sh
   use role accountadmin;
   create user gx_user password="secure_password";
   create role gx_role;
   grant role gx_role to user gx_user;
   ```
3. Replace `secure_password` with your value.

4. Select **Run All** to define your user password, create a new GX role (`gx_role`), and assign the password and role to a new user (`gx_user`).

    ![Snowflake Run All](/img/run_all.png)

 5. Copy the following code and paste it into the SQL worksheet:

   ```sh
   create warehouse gx_wh
   warehouse_size=xsmall 
   auto_suspend=10  
   auto_resume=true
   initially_suspended=true;
   ```
    The settings in the code example optimize cost and performance. Adjust them to suit your business requirements.

6. Select **Run All** to create a new warehouse (`gx_wh`) for the GX Agent.

7. Copy the following code and paste it into the SQL worksheet:

   ```sh
   grant usage, operate on warehouse gx_wh to role gx_role;
   grant usage on database "database_name" to role gx_role;
   grant usage on schema "database_name.schema_name" to role gx_role;
   grant select on all tables in schema "database_name.schema_name" to role gx_role;
   grant select on future tables in schema "database_name.schema_name" to role gx_role; 
   ```
8. Replace `database_name` and `schema_name` with the names of the database and schema you want to access in GX Cloud.

9. Select **Run All** to allow the user with the `gx_role` role to access data on the Snowflake database and schema.

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

You deploy and run the GX Agent within your production cloud services environment. You can deploy the GX Agent container in any environment where you can run Docker container images or create Kubernetes clusters.

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
  groupId="connect-snowflake"
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
    Replace `YOUR_ACCESS_TOKEN` and `YOUR_ORGANIZATION_ID` with the values you copied previously. To store your Snowflake password as an environment variable, add `-e GX_CLOUD_SNOWFLAKE_PW= YOUR_SNOWFLAKE_PASSWORD \` to the Docker command.

3. Run the following command to use the GX Agent image as the base image and optionally add custom commands:

   ```bash title="Terminal input"
   FROM greatexpectations/agent
   RUN echo "custom_commands"
   ```
4. Run the following command to rebuild the Docker image:

   ```bash title="Terminal input"
   docker build -t myorg/agent
   ```

</TabItem>
<TabItem value="kubernetes">

1. Install kubectl. See [Install Tools](https://kubernetes.io/docs/tasks/tools/).

2. Run the following command to provide the access credentials to the Kubernetes container:
    
   ```sh
   kubectl create secret generic gx-agent-secret \
   --from-literal=GX_CLOUD_ORGANIZATION_ID=YOUR_ORGANIZATION_ID \
   --from-literal=GX_CLOUD_ACCESS_TOKEN=YOUR_ACCESS_TOKEN \
   ```
    Replace `YOUR_ORGANIZATION_ID` and `YOUR_ACCESS_TOKEN` with the values you copied previously. To include your Snowflake password as an access credential, add `--from-literal=GX_CLOUD_SNOWFLAKE_PW=YOUR_SNOWFLAKE_PASSWORD` to the command.


3. Create and save a file named `deployment.yaml`, with the following configuration:

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
4. Run the following command to use the `deployment.yaml`configuration file to deploy the GX Agent:

   ```sh
   kubectl apply -f deployment.yaml
   ```
5. Run the following command to terminate running resources gracefully:

   ```sh
   kubectl delete -f deployment.yaml
   kubectl delete secret gx-agent-secret
   ```


</TabItem>
</Tabs>


## Next steps

- [Create a Data Asset](../data_assets/manage_data_assets.md#create-a-data-asset)

- [Invite users](../users/manage_users.md#invite-a-user)

