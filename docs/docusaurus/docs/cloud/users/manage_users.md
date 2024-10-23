---
sidebar_label: 'Manage users and access tokens'
title: 'Manage users and access tokens'
description: Manage GX Cloud users and access tokens.
---

With Admin permissions, you can add users, edit organization roles, and delete users. You can also manage user and organization access tokens.

## Roles and responsibilities

The following table lists GX Cloud roles and responsibilities.

| Role          | Responsibilities                                  |
|---------------|---------------------------------------------------|
| Viewer        | View Validation Results           |
| Editor        | Create Data Assets<br/>Create and edit Expectations<br/>Create access tokens |
| Admin         | Full access<br/>Perform all GX Cloud administrative functions including user and role assignment |

## Invite a user

1. In GX Cloud, click **Settings** > **Users**.

2. Click **Invite User** and complete the following fields:

    - **Email** - Enter the user's email address.

    - **Organization Role** - Select **Viewer**, **Editor**, or **Admin**. Viewers can view Validation Results, Editors can create and edit Expectations and can create access tokens, and Admins can perform all GX Cloud administrative functions.

3. Click **Invite**.

    An email invitation is sent to the user.

## Edit a user role

1. In GX Cloud, click **Settings** > **Users**.

2. Click the options menu for a user and select **Edit**.

3. Select an organization role and then click **Update User**. 

## Delete a user

1. In GX Cloud, click **Settings** > **Users**.

2. Click the options menu for a user and select **Delete**.

3. Click **Yes, Remove This User**.

## Create a user access token

You'll need your user access token and organization ID when you set your environment variables.

Access tokens shouldn't be committed to version control software.

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** pane, click **Create user access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy, paste, and then save the user access token as a text file or similar. The token can't be retrieved after you close the dialog.

6. Click **Close**.

## Create an organization access token

Organization access tokens are typically required for external application authentication. These external applications complete tasks such as scheduled pipeline runs on behalf of your organization. 

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **Organization access tokens** pane, click **Create organization access token**.

3. In the **Token name** field, enter a name for the token that will help you quickly identify it.

4. Click **Create**.

5. Copy, paste, and then save the organization access token as a text file or similar. The token can't be retrieved after you close the dialog.

6. Click **Close**.

## Delete a user or organization access token

1. In GX Cloud, click **Settings** > **Tokens**.

2. In the **User access tokens** or **Organization access tokens** panes, click **Delete Token**.

3. Click **Delete**.

