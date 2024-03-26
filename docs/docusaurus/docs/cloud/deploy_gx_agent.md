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

To use GX Cloud features and functionality in an org-hosted deployment, you need to deploy the GX Agent. 

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

Environment variables securely store your GX Cloud access credentials. The GX Agent runs open source GX code in GX Cloud, and it allows you to securely access your data without connecting to it or interacting with it directly. To learn more about the GX Agent and deployment patterns, see [About GX Cloud](./about_gx.md).

1. Start the Docker Engine.

2. Run the following code to set the `GX_CLOUD_ACCESS_TOKEN` and `GX_CLOUD_ORGANIZATION_ID` environment variables, install GX Cloud and its dependencies, and start the GX Agent:

    ```bash title="Terminal input"
    docker run --rm --pull=always -e GX_CLOUD_ACCESS_TOKEN="<user_access_token>" -e GX_CLOUD_ORGANIZATION_ID="<organization_id>" greatexpectations/agent
    ```
   Replace `user_access_token` and `organization_id` with the values you copied previously. 

3. In GX Cloud, confirm the GX Agent status icon is green. This indicates the GX Agent is running. If it isn't, repeat step 2 and confirm the `user_access_token` and `organization_id` values are correct.

    ![GX Agent status](/img/gx_agent_status.png)

4. Optional. If you created a temporary file to record your user access token and Organization ID, delete it.

5. Optional. Run `docker ps` or open Docker Desktop to confirm the agent is running.

    If you stop the GX Agent, close the terminal, and open a new terminal you'll need to set the environment variables again.

    To edit an environment variable, stop the GX Agent, edit the environment variable, save the change, and then restart the GX Agent.

## Next steps

- [Connect GX Cloud](../cloud/connect/connect_lp.md)

