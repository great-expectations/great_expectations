---
sidebar_label: 'Manage Expectation Suites'
title: 'Manage Expectation Suites'
description: Create and manage Expectation Suites in GX Cloud.
---

Expectation Suites contain multiple Expectations. Like Expectations, they can help you better understand your data and help you improve data quality. A default Expectation Suite is created when you create a Data Asset that defines the data you want GX Cloud to access. 

To learn more about Expectation Suites, see [Expectation Suites](../../terms/expectation_suite.md).

## Create an Expectation Suite with the Onboarding Data Assistant

Use the Onboarding Data Assistant to profile your data and automate the generation of an Expectation Suite.

1. In GX Cloud, click **Data Assets** and select a Data Asset in the **Data Assets** list.

2. Click **Create New Suite** in the **Expectation Suites** pane.

3. Click **Onboarding** and then enter a name for the expectation Suite in the **Suite name** field.

4. Click **Generate Expectations**. 

    A new Expectation Suite appears in the Expectation Suites pane.

## Create an Expectation Suite with the Missingness Data Assistant

Use the Missingness Data Assistant to profile your data and automate the generation of an Expectation Suite.

1. In GX Cloud, click **Data Assets** and select a Data Asset in the **Data Assets** list.

2. Click **Create New Suite** in the **Expectation Suites** pane.

3. Click **Missingness** and then enter a name for the expectation Suite in the **Suite name** field.

4. Click **Generate Expectations**. 

    A new Expectation Suite appears in the Expectation Suites pane.

## Delete an Expectation Suite

1. In GX Cloud, delete all Checkpoints associated with the Expectation Suite. See [Delete a Checkpoint](/docs/cloud/checkpoints/manage_checkpoints#delete-a-checkpoint). 

2. Click **Expectation Suites**.

3. Click **Delete** for the Expectation Suite you want to delete.

4. Click **Delete**.

## Related documentation

- [Manage Expectations](../expectations/manage_expectations.md)