# Contributing to Great Expectations documentation

This repository contains all the source files used to create Great Expectations documentation. 

If you want to modify existing Great Expectations code, you want to submit a new feature, or you want to submit a custom Expectation, see [CONTRIBUTING](https://github.com/great-expectations/great_expectations/blob/develop/CONTRIBUTING.md) in the Great Expectations repository.

## Request a documentation change

If you’ve noticed an issue in the documentation, or have recommendations for improving the content, you can submit your request as a GitHub issue in this repository. To avoid forking the repository, Great Expectations recommends using the GitHub Markdown editor to edit documentation.

1. Open a browser and go to the [GitHub Great Expectations docs repository](https://github.com/great-expectations/great_expectations/tree/develop/docs).

2. Go to the topic file you want to edit. The topic URL contains the path to each topic file. For example, the path for this topic is <https://docs.greatexpectations.io/docs/contributing/contributing_github>. The URL indicates the topic is located in the `contributing` folder, and it's named `contributing_github`.

3. Click the file and then click **Edit this file**.

4. Add your edits.

5. Optional. Click the **Preview** tab to preview your changes.

6. When you’ve completed your edits, scroll down to the **Propose changes** section and add a meaningful commit message and an explanation of what you changed and why.  To help identify the type of issue you’re submitting, add one of the following identifiers to the pull request (PR):

    - [BUGFIX] for PRs that address minor bugs without changing behavior.

    - [FEATURE] for significant PRs that add a new feature likely to require being added to our feature maturity matrix.

    - [DOCS] for PRs that focus on documentation improvements.

    - [MAINTENANCE] for PRs that focus on updating repository settings or related changes.
		
    - [CONTRIB] for the contribution of custom Expectations and supporting work into the `contrib/` directory.
      
    - [HACKATHON] for submissions to an active Great Expectations Hackathon.

7. Select **Create a new branch for this commit and start a pull request**. Accept the default name for the branch, or enter a new one.

8. Click **Propose changes**.

    If this is your first Great Expectations documentation contribution, you'll be prompted to complete the Contributor License Agreement (CLA). Complete the CLA and add `@cla-bot check` as a comment to the pull request (PR) to indicate that you’ve completed it.

9. Wait for the Continuous Integration (CI) checks to complete and then correct any syntax or formatting issues.

    A Great Expectations team member reviews, approves, and merges your PR. Depending on your GitHub notification settings, you'll be notified when there are comments or when your changes are successfully merged.
