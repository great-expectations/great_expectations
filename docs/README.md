# Contribute to Great Expectations documentation

This repository contains all the source files used to create Great Expectations documentation. Use the information here to complete the following:

- [Request a documentation change](#request-a-documentation-change)

- [Create a how-to guide](#create-a-how-to-guide)

- [Create an integrations guide](#create-an-integration-guide)

To submit a code change to Great Expectations for consideration, see [CONTRIBUTING_CODE](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) in the `great_expectations` repository.

To create and submit a custom Expectation to Great Expectations for consideration, see [CONTRIBUTING_EXPECTATIONS](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_EXPECTATIONS.md) in the `great_expectations` repository.

To submit a custom package to Great Expectations for consideration, see [CONTRIBUTING_PACKAGES](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_PACKAGES.md) in the `great_expectations` repository.

## Request a documentation change

If you’ve noticed an issue in the documentation, or have recommendations for improving the content, you can submit your request as a GitHub issue in this repository. To avoid forking the repository, Great Expectations recommends using the GitHub Markdown editor to edit documentation.

1. Go to the topic file in the [GitHub Great Expectations docs repository](https://github.com/great-expectations/great_expectations/tree/develop/docs) you want to edit. 

    The topic URL includes the topic file name. For example, the URL for the Great Expectations Quickstart is <https://docs.greatexpectations.io/docs/tutorials/quickstart/>. The URL indicates the topic is located in the `tutorials` folder, and it's named `quickstart`.

2. Click the file and then click **Edit this file**.

3. Add your edits.

4. Optional. Click the **Preview** tab to preview your changes.

5. When you’ve completed your edits, scroll down to the **Propose changes** section and add a meaningful commit message and an explanation of what you changed and why.  To help identify the type of issue you’re submitting, add the [DOCS] identifier to the pull request (PR). For example, [DOCS] Update Version Reference. 

6. Select **Create a new branch for this commit and start a pull request**. Accept the default name for the branch, or enter a new one.

7. Click **Propose changes**.

    If this is your first Great Expectations documentation contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the pull request (PR) to indicate that you’ve completed it.

8. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments or when your changes are successfully merged.

## Create a how-to guide

The objective of a how-to guide is to help users successfully replicate a specific process. You should include important information about how Great Expectations functions with other applications if it isn't obvious or documented elsewhere.

When you create your how-to guide, you can make the following assumptions:

- The user has a working deployment of Great Expectations, whether it is set up on the file system by running `great_expectations init` or configured in-memory.

- The user is already familiar with Great Expectations core concepts including Expectations, Data Contexts, Validation, and Datasources.

- The user is familiar with the application being integrated with Great Expectations. For example, if you’re creating a how-to for configuring a Snowflake Datasource, you don’t need to provide a detailed explanation of Snowflake or its core concepts.

### Fork and clone the Great Expectations repository

1. Open a browser and go to the [Great Expectations repository](https://github.com/great-expectations/great_expectations).

2. Click **Fork** and then **Create Fork**.

3. Click **Code** and then select the **HTTPS** or **SSH** tabs.

4. Copy the URL, open a Git terminal, and then run the following command:

    ```sh
    git clone <url>
    ```
5. Run the following command to specify a new remote upstream repository that will be synced with the fork:

    ```sh
    git remote add upstream git@github.com:great-expectations/great_expectations.git
    ```
6. Run the following command to create a branch for your changes:

    ```sh
    git checkout -b <how-to-branch-name>
    ```

### Add content to your how-to guide

1. Copy the `how_to_template` in the `templates` folder.

2. Rename the `how_to_template` using underscores as delimiters. For example, `use_great_expectations_with_airflow`. It's not necessary to include `how-to` in the filename. Keep your how-to in the `templates` folder.

3. Add content to your how-to using the instructions provided in the template.

### Submit a pull request

1. Push your changes to the remote fork of your repository.

2. Create a pull request from your fork. See [Creating a pull request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

3. Add a meaningful title and description for your pull request (PR). Add the [HOW-TO] identifier to the PR title. The description should include the purpose of your how-to and why it is necessary. For example, [HOW-TO] Contribute to Great Expectations Documentation. 

4. If this is your first Great Expectations contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the PR to indicate that you’ve completed it.

5. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments, your input is required, or when your changes are successfully merged.

## Create an integration guide

The objective of an integrations guide is to help users successfully integrate an application with Great Expectations. 

### Contact the Great Expectations Developer Relations team

Before you create your integration guide, notify the Great Expectations Developer Relations team of your plans in the [Great Expectations #integrations Slack channel](https://greatexpectationstalk.slack.com/archives/C037YCYNF1Q). A member of the team will discuss your requirements and support you through the review and publication process.

### Fork and clone the Great Expectations repository

1. Open a browser and go to the [Great Expectations repository](https://github.com/great-expectations/great_expectations).

2. Click **Fork** and then **Create Fork**.

3. Click **Code** and then select the **HTTPS** or **SSH** tabs.

4. Copy the URL, open a Git terminal, and then run the following command:

    ```sh
    git clone <url>
    ```
5. Run the following command to specify a new remote upstream repository that will be synced with the fork:

    ```sh
    git remote add upstream git@github.com:great-expectations/great_expectations.git
    ```
6. Run the following command to create a branch for your changes:

    ```sh
    git checkout -b <how-to-branch-name>
    ```

### Add content to your how-to guide

1. Copy the `integration_template` in the `templates` folder.

2. Rename the `integration_template` using underscores as delimiters. For example, `use_great_expectations_with_airflow`. It's not necessary to include `integration` in the filename. Keep your integration guide in the `templates` folder.

3. Add content to your integration guide using the instructions provided in the template.

### Submit a pull request

1. Push your changes to the remote fork of your repository.

2. Create a pull request (PR) from your fork. See [Creating a pull request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

3. Add a meaningful title and description for your PR. The description should include the purpose of your integration. Add the [INTEGRATE] identifier to the PR title. For example, [INTEGRATE] Great Expectations with Snowflake.

4. If this is your first Great Expectations contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the PR to indicate that you’ve completed it.

5. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments, your input is required, or when your changes are successfully merged.