.. _contributing_contribution_checklist:



Contribution checklist
=======================



Sam's instructions from Contributing.md
---------------------------------------------

# How to contribute to Great Expectations code

The following instructions provide a step-by-step overview of how to contribute code to Great Expectations.

* Fork the Great Expectations repo:
    * Go to the [Great Expectations repo on GitHub](https://github.com/great-expectations/great_expectations).
    * Click the "Fork" button in the top right. This will make a copy of the repo in your own GitHub account.
    * GitHub will take you to your forked version of the repository.
* Clone your fork: Click the green "Clone" button and choose the SSH or HTTPS URL depending on your setup. 
    * Copy the URL and run `git clone <url>` in your local terminal.
    * Note: This will clone the `develop` branch of the great_expectations repo by default, not `master`.
* Add the upstream remote:
    * On your local machine, cd into the great_expectations repo you cloned in the previous step.
    * Run: `git remote add upstream git@github.com:great-expectations/great_expectations.git`
    * This sets up a remote called `upstream` to track changes to the main branch.
* Install all relevant libraries:
    * Make a new virtual environment (e.g. using virtualenv or conda), name it "great_expectations_dev" or similar.
    * Install dependencies from requirements-dev.txt to make sure you have the right libraries, then install great_expectations from the version you just forked:
        * `pip install -r requirements-dev.txt`
        * `pip install .`
    * Make sure you have GCC and Java installed and working on your machine.
* Create a feature branch to start working on your changes.
    * Make sure to check out the *Conventions* sections below to stick with the project conventions.
* When you’re done with the work:
    *  Update your local repository with the most recent code from the main Great Expectations repository, and rebase your branch on top of the latest `develop` branch. We prefer small, incremental commits, because it makes the thought process behind changes easier to review. [Here's some more info on how to keep your forks up-to-date](https://www.atlassian.com/git/tutorials/git-forks-and-upstreams).
    * Fix any merge conflicts that arise from the rebase.
    * Run the tests -- refer to the section below on *Testing* for details
    * Please add a bullet on your changes to the [changelog](https://github.com/great-expectations/great_expectations/blob/develop/docs/changelog/changelog.rst) under the **Develop** heading.
    * Make sure to add and commit all your changes in this step.
* Create a PR:
    * Push to the remote fork of your repo
    * Follow [these instructions](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) to create a PR from your commit.
    *  In the PR, choose a short title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, e.g. "Closes #123".
* You will see a comment from the "CLA Bot" that asks you to complete the Contributor Licence Agreement form. See details in the section below if you want to learn more.
    * Please complete the form and comment on the PR to say that you’ve signed the form.
* Wait for the other Continuous Integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.
* Once all checks pass, a GE team member will approve your PR and merge it. 
    * GitHub will notify you of comments or a successful merge according to your notification settings.
    * There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!
* Congratulations! You’ve just contributed to Great Expectations!


Testing
--------------------

In general, you can run all tests by simply running `pytest` in the great_expectations directory root. Before submitting any PR, please ensure there are no tests that fail with an error, and review any warnings.

If you would like to run the tests without the need for local backend setups (e.g. setting up a local postgres database), adding the following flags will skip the respective backends:
- `--no-postgresql` will skip postgres tests
- `--no-spark` will skip spark tests 
- `--no-sqlalchemy` will skip all tests using sqlalchemy (i.e. all database backends)

For example, you can run

`pytest --no-spark --no-sqlalchemy` 

to skip all local backend tests (with the exception of the pandas backend). Please note that these tests will still be run by the CI as soon as you open a PR, so some tests might fail there if your code changes affected them.


In addition to running existing tests to make sure your code change works, please also write relevant tests for any change that you're making. Our tests live in the `tests` subdirectory of the great_expectations repo.


*last updated*: |lastupdate|
