# Contribute Custom Expectations

To submit a Custom Expectation to Great Expectations for consideration, you complete the following tasks:

- [Fork and clone the Great Expectations repository](#fork-and-clone-the-great-expectations-repository)

- [Generate the Expectation validation checklist](#generate-the-expectation-validation-checklist)

- [Verify Expectation metadata](#verify-expectation-metadata)

- [Submit a pull request](#submit-a-pull-request)

To request a documentation change, or a change that doesn't require local testing, see the [README](https://github.com/great-expectations/great_expectations/blob/develop/docs/README.md) in the `docs` repository.

To create and submit a custom package to Great Expectations for consideration, see [CONTRIBUTING_PACKAGES](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_PACKAGES.md) in the `great_expectations` repository.

To submit a code change to Great Expectations for consideration, see [CONTRIBUTING_CODE](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING_CODE.md) in the `great_expectations` repository.

## Prerequisites

- Great Expectations installed and configured for your environment. See [Great Expectations Quickstart](https://docs.greatexpectations.io/docs/tutorials/quickstart/).

- A Custom Expectation. See [Creating Custom Expectations](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview/).

- A GitHub account.

- A working version of Git on your computer. See [Getting Started - Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

- A new SSH (Secure Shell Protocol) key. See [Generating a new SSH key and adding it to the ssh-agent](https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).

- The latest Python version installed and configured. See [Python downloads](https://www.python.org/downloads/).

## Fork and clone the Great Expectations repository

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
    git checkout -b <branch-name>
    ```

## Generate the Expectation validation checklist

Before you submit your Custom Expectation, you need to verify it meets the submission requirements. Great Expectations provides a checklist to help you determine if your Custom Expectation meets the minimum requirements. Your Custom Expectation must meet the first five of the listed requirements to qualify for submission.

To generate the Expectation checklist, add the `print_diagnostic_checklist()` instance method to your Custom Expectation. When the instance method runs, it returns results similar to the following:

```console
✔ Has a valid library_metadata object
✔ Has a docstring, including a one-line short description
  ...
✔ Has at least one positive and negative example case, and all test cases pass
✔ Has core logic and passes tests on at least one Execution Engine
  ...
✔ Passes all linting checks
✔ Has basic input validation and type checking
✔ Has both statement Renderers: prescriptive and diagnostic
✔ Has core logic that passes tests for all applicable Execution Engines and SQL dialects
  ...
  Has a full suite of tests, as determined by project code standards
  Has passed a manual review by a code owner for code standards and style guides
```

## Verify Expectation metadata

Verifying your Custom Expectation metadata ensures that it is accredited to you and includes an accurate description.

Great Expectations maintains a number of Custom Expectation packages, that contain thematically related Custom Expectations. These packages are located in the [Great Expectations contrib directory](https://github.com/great-expectations/great_expectations/tree/develop/contrib) and on [PyPI](https://pypi.org/).
If your Custom Expectation fits within one of these packages, you're encouraged to contribute your Custom Expectation directly to one of these packages.

If you're not contributing to a specific package, your Custom Expectation is automatically published in the [PyPI great-expectations-experimental package](https://pypi.org/project/great-expectations-experimental/). This package contains all Great Expectations experimental community-contributed Custom Expectations that have not been submitted to other packages.

Confirm the `library_metadata` object for your Custom Expectation includes the following information:

- `contributors`: Identifies the creators of the Custom Expectation.
- `tags`: Identifies the Custom Expectation functionality and domain. For example, `statistics`, `flexible comparisons`, `geography`, and so on.
- `requirements`: Identifies if your Custom Expectation relies on third-party packages.

## Submit a pull request

1. Push your changes to the remote fork of your repository.

2. Create a pull request from your fork. See [Creating a pull request from a fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

3. Add a meaningful title and description for your pull request (PR). Provide a detailed explanation of what you changed and why. To help identify the type of issue you’re submitting, add one of the following identifiers to the pull request (PR) title:

    - [BUGFIX] for PRs that address minor bugs without changing behavior.

    - [FEATURE] for significant PRs that add a new feature likely to require being added to our feature maturity matrix.

    - [MAINTENANCE] for PRs that focus on updating repository settings or related changes.
		
    - [CONTRIB] for the contribution of Custom Expectations and supporting work into the `contrib/` directory.
      
    - [HACKATHON] for submissions to an active Great Expectations Hackathon.

    In the section for design review, include a description of any prior discussion or coordination on the features in the PR, such as mentioning the number of the issue where discussion has taken place. For example: Closes #123”, linking to a relevant discuss or slack article, citing a team meeting, or even noting that no discussion is relevant because the issue is small.

4. If this is your first Great Expectations contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the pull request (PR) to indicate that you’ve completed it.

5. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments or when your changes are successfully merged.

    If your Custom Expectation doesn't meet the minimum requirements in the validation checklist, it is failing testing, or there is a functionality error, you'll be asked to resolve the issues before your Custom Expectation can move forward.

    If you are submitting a production Custom Expectation, Great Expectations requires that your Custom Expectation meet or exceed Great Expectation standards for testing and coding.

    When your Custom Expectation has successfully passed testing and received approval from a code owner, your contribution is complete. Your custom Expectation will be included in the next release of Great Expectations and an announcement will appear in the release notes

## Issue tags

Great Expectations uses a `stalebot` to automatically tag issues without activity as `stale`, and closes them when a response is not received within a week. To prevent `stalebot` from closing an issue, you can add the `stalebot-exempt` tag.

Additionally, Great Expectations adds the following tags to indicate issue status:

- The`help wanted` tag identifies useful issues that require help from community contributors to accelerate development.

- The `enhacement` and `expectation-request` tags identify new Great Expectations features that require additional investigation and discussion. 

- The `good first issue` tag identifies issues that provide an introduction to the Great Expectations contribution process.
