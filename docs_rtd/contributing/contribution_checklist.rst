.. _contributing_contribution_checklist:


Contribution checklist
=======================

Following these instructions helps us make sure the code review and merge process go smoothly.

.. _contributing_before_submitting_a_pr:

Before submitting a pull request
--------------------------------

Once your code is ready, please go through the following checklist before submitting a pull request. 


**1. Have you signed the CLA?**

    * A Contributor License Agreement helps guarantee that contributions to Great Expectations will always remain free and open.
    * Please see :ref:`contributing_cla` for more information and instructions for how to sign the CLA the first time you contribute to Great Expectations.
    * If you’ve included your (physical) mailing address in the CLA, we’ll send you a personalized Great Expectations mug once your first PR is merged!

**2. Have you followed the Style Guide for code and comments?**

    * The :ref:`contributing__style_guide` is here.
    * Thanks for helping us keep the codebase and documentation clean and consistent, so that it's easier to maintain it as a community!
    * If your PR contains **only** changes to ``contrib`` directory (community contributed Expectations), review your PR against this :ref:`Acceptance Checklist <contrib_pull_request_acceptance_checklist>`

**3. Is your branch up to date with upstream/develop?**

    * Update your local repository with the most recent code from the main Great Expectations repository.
    * For changes with few or no merge conflicts, you can do this by creating a draft pull request in GitHub and clicking ``Update branch``.
    * You can also rebase your branch from ``upstream/develop``. In general, the steps are:

        1. Run ``git fetch upstream`` then ``git rebase upstream/develop``.
        2. Fix any merge conflicts that arise from the rebase.
        3. Make sure to add and commit all your changes in this step.
        4. Re-run tests to ensure the rebase did not introduce any new issues.

    * Atlassian and Github both have good tutorials for rebasing: `Atlassian's tutorial <https://www.atlassian.com/git/tutorials/git-forks-and-upstreams>`__, `Github's tutorial <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork>`__.

**4. Have you written and run all the tests you need?**

    .. admonition:: Note:

        If your PR contains **only** changes to ``contrib`` directory (community contributed Expectations), please skip this step - the tests that you included in the Expectation itself are sufficient and you do not need to run ``pytest``.

    * See :ref:`contributing_testing__writing_unit_tests` for details on how to write unit tests in Great Expectations.
    * Please make certain to run ``pytest`` to verify that all tests pass locally. See :ref:`contributing_testing__running_tests` for details.

**5. Have you documented all the changes in your PR?**

    .. admonition:: Note:

        If your PR contains **only** changes to ``contrib`` directory (community contributed Expectations), please skip this step.

    * Please add a bullet point to ``docs_rtd/changelog.rst``, in the ``develop`` section.
        * Please group in the following order: [BREAKING], [FEATURE], [BUGFIX], [DOCS], [MAINTENANCE]
    * You can see the past Changelog here: :ref:`changelog`


If you’ve checked off all these items, you’re now ready to submit a pull request!


.. _contributing_submitting_a_pr:

How to submit a pull request
----------------------------

When you’re done with your work...

**1. Create a PR**

    * Push to the remote fork of your repo.
    * Follow `these instructions <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__ to create a PR from your commit.
    * Provide background for reviewers so they can understand and approve your PR more quickly:

        * Choose a short title which sums up the changes that you have made. 
        * Add a tag to help categorize the PR:

            * [BUGFIX] for PRs that address minor bugs without changing behavior, 
            * [FEATURE] for significant PRs that add a new feature likely to require being added to our feature maturity matrix,
            * [DOCS] for PRs that focus on improving documentation, or 
            * [MAINTENANCE] for PRs that focus on updating repository settings or related chores. This status is used internally.

        * Summarize your changes using a few clear sentences (sometimes screenshots are nice too!). A good guide is to aim for a collection of commit message summaries that provide more details about what your changes do, like "Fixed handling of malformed datasource configuration" or "Improved docstrings for store module"
        * Finally, in the section for design review, include a description of any prior discussion or coordination on the features in the PR, such as mentioning the number of the issue where discussion has taken place, e.g. "Closes #123", linking to a relevant discuss or slack article, citing a team meeting, or even noting that no discussion is relevant because the issue is small.

**2. Confirm the contributor license agreement (CLA)**

    * If you've followed the checklist above, you will have already signed the CLA and won't see the CLA bot.
    * Otherwise, you will see a comment from the "CLA Bot" on the PR that asks you to complete the CLA form. Please do so.
    * Once you've signed the form, add a new comment to the PR with the line ``@cla-bot check``. This will trigger the CLA bot to refresh.

**3. Verify continuous integration checks**

    * Wait for the other continuous integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.

**4. Wait for a core team member to approve and merge your PR**

    * Once all checks pass, a Great Expectations team member will approve your PR and merge it.
    * GitHub will notify you of comments or a successful merge according to your notification settings.

**5. Resolve any issues**

    * There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!

**6. Do a victory dance**

    * Congratulations! You’ve just contributed to Great Expectations!

        .. image:: great_expectations_happy.gif
