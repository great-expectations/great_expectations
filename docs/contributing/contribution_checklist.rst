.. _contributing_contribution_checklist:


Contribution checklist
=======================


.. _contributing_before_submitting_a_pr:

Before submitting a pull request
------------

Once your code is ready, please go through the following checklist before submitting a pull request. to make sure the code review and merge process go smoothly:

We prefer small, incremental commits, because it makes the thought process behind changes easier to review.


**1. Have you signed the CLA?**

    * :ref:`contributing_cla`
    * If you’ve included your (physical) mailing address in the CLA, we’ll send you a personalized Great Expectations mug once your first PR is merged!

**2. Have you followed the Style Guide for code and comments?**

    * :ref:`contributing_style_guide`

**3. Is your branch up to date with upstream/develop?**

    * Update your local repository with the most recent code from the main Great Expectations repository.
    * For changes with few or no merge conflicts, you can do this by creating a draft pull request in GitHub and clicking `Update branch`.
    * You can also rebase your branch from ``upstream/develop`` branch. Atlassian and Github both have good tutorials for rebasing: `Atlassian's tutorial <https://www.atlassian.com/git/tutorials/git-forks-and-upstreams>`__, `Github's tutorial <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork>`__.
    * In general, the steps are:

        1. Run ``git fetch upstream`` then ``git rebase upstream/develop``.
        2. Fix any merge conflicts that arise from the rebase.
        3. Make sure to add and commit all your changes in this step.
        4. Re-run tests to ensure the rebase did not introduce any new issues.

**4. Have you written and run all the tests you need?**

    * :ref:`contributing_testing`.
    * ...meaning you have run all existing tests locally and you have added new tests where appropriate.

**5. Have you added a bullet with your changes under the "develop" heading in the Changelog?**

    * :ref:`changelog`

If you’ve checked off all these items, you’re now ready to submit a pull request! Check out the next section :ref:`contributing_submitting_a_pr` for step-by-step instructions.


.. _contributing_submitting_a_pr:

How to submit a pull request
--------------

When you’re done with your work...

**2. Create a PR**

    * Push to the remote fork of your repo.
    * Follow [these instructions](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to create a PR from your commit.
    *  In the PR, choose a short title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, e.g. "Closes #123".

**3. Confirm the contributor license agreement (CLA)**

    * If you've followed the checklist above, you will have already signed the CLA and won't see the CLA bot.
    * Otherwise, you will see a comment from the "CLA Bot" on the PR that asks you to complete the CLA form. Please do so.
    * Once you've signed the form, add a new comment to the PR with the line ``@cla-bot check``. This will trigger the CLA bot to refresh.

**4. Verify continuous integration checks**

    * Wait for the other continuous integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.

**5. Wait for a core team member to approve and merge your PR**

    * Once all checks pass, a GE team member will approve your PR and merge it.
    * GitHub will notify you of comments or a successful merge according to your notification settings.

**6. Resolve any issues**

    * There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!

**7. Do a victory dance**

    * Congratulations! You’ve just contributed to Great Expectations!

        .. image:: great_expectations_happy.gif
