---
title: Contribution Checklist
---

Following these instructions helps us make sure the code review and merge process go smoothly.

### Before submitting a pull request

Once your code is ready, please go through the following checklist before submitting a pull request.

#### 1. Have you signed the CLA?

* A Contributor License Agreement helps guarantee that contributions to Great Expectations will always remain free and open.

* Please see [Contributor license agreement](https://docs.greatexpectations.io/docs/contributing/contributing_misc/) (CLA) for more information and instructions for how to sign the CLA the first time you contribute to Great Expectations.

* If you’ve included your (physical) mailing address in the CLA, we’ll send you a personalized Great Expectations mug once your first PR is merged!

#### 2. Have you followed the Style Guide for code and comments?

* The [Style Guide](/docs/contributing/style_guides/code_style) is here.

* Thanks for helping us keep the codebase and documentation clean and consistent, so that it’s easier to maintain it as a community!

* If your PR contains only changes to `contrib` directory (community contributed Expectations), review your PR against this Acceptance Checklist

#### 3. Is your branch up to date with upstream/develop?

* Update your local repository with the most recent code from the main Great Expectations repository.

* For changes with few or no merge conflicts, you can do this by creating a draft pull request in GitHub and clicking `Update branch`.

* You can also rebase your branch from upstream/develop. In general, the steps are:

	* Run `git fetch upstream` then `git rebase upstream/develop`.

	* Fix any merge conflicts that arise from the rebase.

	* Make sure to add and commit all your changes in this step.

	* Re-run tests to ensure the rebase did not introduce any new issues.

* Atlassian and GitHub both have good tutorials for rebasing: [Atlassian’s tutorial](https://www.atlassian.com/git/tutorials/git-forks-and-upstreams), [GitHub’s tutorial](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork).

#### 4. Have you written and run all the tests you need?

:::note
If your PR contains **only** changes to `contrib` directory (community contributed Expectations), please skip this step - the tests that you included in the Expectation itself are sufficient and you do not need to run pytest.
:::

* See [Writing unit and integration tests](/docs/contributing/contributing_test) for details on how to write unit tests in Great Expectations.

* Please make certain to run `pytest` to verify that all tests pass locally. See [Running tests](/docs/contributing/contributing_test) for details.

#### 5. Have you documented all the changes in your PR?

:::note
If your PR contains **only** changes to `contrib` directory (community contributed Expectations), please skip this step.
:::

* Please add a bullet point to `docs/changelog.md`, in the develop section.
* Please group in the following order:  [FEATURE], [BUGFIX], [DOCS], [MAINTENANCE]

* You can see the [Newer Changelog](../changelog.md) here:
* The [Archived Changelog](https://github.com/great-expectations/great_expectations/blob/develop/docs_rtd/changelog.rst) here:

If you’ve checked off all these items, you’re now ready to submit a pull request!

### How to submit a pull request
When you’re done with your work…

#### 1. Create a PR

* Push to the remote fork of your repo.

* Follow these [instructions to create a PR](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) from your commit.

* Provide background for reviewers so they can understand and approve your PR more quickly:

	* Choose a short title which sums up the changes that you have made.

	* Add a tag to help categorize the PR:

		* [BUGFIX] for PRs that address minor bugs without changing behavior,

		* [FEATURE] for significant PRs that add a new feature likely to require being added to our feature maturity matrix,

		* [DOCS] for PRs that focus on improving documentation, or

		* [MAINTENANCE] for PRs that focus on updating repository settings or related chores.

		* Summarize your changes using a few clear sentences (sometimes screenshots are nice too!). A good guide is to aim for a collection of commit message summaries that provide more details about what your changes do, like “Fixed handling of malformed datasource configuration” or “Improved docstrings for store module”

		* Finally, in the section for design review, include a description of any prior discussion or coordination on the features in the PR, such as mentioning the number of the issue where discussion has taken place, e.g. “Closes #123”, linking to a relevant discuss or slack article, citing a team meeting, or even noting that no discussion is relevant because the issue is small.

#### 2. Confirm the contributor license agreement (CLA)

* If you’ve followed the checklist above, you will have already signed the CLA and won’t see the CLA bot.

* Otherwise, you will see a comment from the “CLA Bot” on the PR that asks you to complete the CLA form. Please do so.

* Once you’ve signed the form, add a new comment to the PR with the line @cla-bot check. This will trigger the CLA bot to refresh.

#### 3. Verify continuous integration checks

* Wait for the other continuous integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.

* Fix any issues that are flagged.

#### 4. Wait for a core team member to approve and merge your PR

* Once all checks pass, a Great Expectations team member will approve your PR and merge it.

* GitHub will notify you of comments or a successful merge according to your notification settings.

#### 5. Resolve any issues

* There will probably be discussion about the pull request. It’s normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!

Congratulations! You’ve just contributed to Great Expectations!
