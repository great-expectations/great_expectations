.. _contributing_submitting_your_contribution:

Submitting your contribution
================================

#FIXME: Rev once more on text
#FIXME: Verify that links exist and work.

When you’re done with your work...

1. Make sure that your branch is up to date with `develop`
--------------------------------------------------------

    * Update your local repository with the most recent code from the main Great Expectations repository, and rebase your branch on top of the latest ``develop`` branch. {#FIXME: Is develop correct here?}
    * We prefer small, incremental commits, because it makes the thought process behind changes easier to review. [Here's some more info on how to keep your forks up-to-date](https://www.atlassian.com/git/tutorials/git-forks-and-upstreams).
    * Fix any merge conflicts that arise from the rebase.
    * Make sure to add and commit all your changes in this step.
    * Re-run tests to ensure the rebase did not introduce any new issues.

2. Create a PR
------------------

    * Push to the remote fork of your repo.
    * Follow [these instructions](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to create a PR from your commit.
    *  In the PR, choose a short title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, e.g. "Closes #123".

3. Confirm the contributor license agreement (CLA)
------------------------

    * If you've followed the :ref:`contributing_contribution_checklist`, you will have already signed the CLA and won't see the CLA bot.
    * Otherwise, you will see a comment from the "CLA Bot" on the PR that asks you to complete the CLA form. Please do so.
    * Once you've signed the form, add a new comment to the PR with the line ``@cla-bot check``. This will trigger the CLA bot to refresh.

4. Verify continuous integration checks
------------------------------------------

    * Wait for the other continuous integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.

5. Wait for a core team member to approve and merge your PR
----------------------------------------------------------------

    * Once all checks pass, a GE team member will approve your PR and merge it. 
    * GitHub will notify you of comments or a successful merge according to your notification settings.

6. Resolve any issues
-----------------------

    * There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!

7. Do a victory dance
------------------------

    * Congratulations! You’ve just contributed to Great Expectations!

*last updated*: |lastupdate|
