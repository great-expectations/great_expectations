# How to contribute
We're excited for contributions to Great Expectations. If you see places where the code or documentation could be improved, please get involved! If you encounter any issues or have questions about how to contribute, please hop into the [Great Expectations Slack channel](https://greatexpectations.io/slack) and our team or other friendly community members will be able to help!

# Pre-requisites

In order to contribute to Great Expectations, you will need the following:

* A GitHub account - this is sufficient if you only want to contribute to the documentation.
* If you want to contribute code, you will also need a working version of Git on your computer. Please refer to the [Git setup instructions](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) for your environment.
* We also recommend going through the [SSH key setup process on GitHub](https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent) for easier authentication.

# How to modify the documentation

We suggest simply editing the docs using the GitHub markdown editor, which means you don’t have to fork the repo at all. Here’s how you do this:

* Go to the [Great Expectations (GE) docs](http://docs.greatexpectations.io/en/latest/).
* On each page, you’ll see an "Edit on GitHub" button in the top right - click this to go to the source file in the GE GitHub repo.
    * Or if you’re already on GitHub, the docs are located in great_expecations > docs. You can directly navigate to the respective page you want to edit (but getting there from the docs is a little easier).
* In the top right of the grey header bar of the actual file, click the pencil icon to get into edit mode on GitHub.
* Make your edits and use the Preview tab to preview changes.
* When you’re done, add a meaningful commit message at the bottom. Use a short title and a meaningful explanation of what you changed and why.
* Click the "Propose File Change" button at the bottom of the page.
* Click the "Create Pull Request" button.
    * Optionally: Add comment to explain your change, if it’s not already in the commit message.
* Click the next "Create Pull Request" button to create the actual PR.
* You will see a comment from the "CLA Bot" that asks you to complete the Contributor License Agreement form.
    * Please complete the form and comment on the PR to say that you’ve signed the form.
* Wait for the other Continuous Integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.
* Once all checks pass, a GE team member will approve your PR and merge it. 
    * GitHub will notify you of comments or a successful merge according to your notification settings.
    * If there are any issues, please address them promptly.
* Congratulations! You’ve just contributed to Great Expectations!

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
    * Make a new virtual environment (e.g. using virtualenv or conda), name it "great_expectations_dev" or similar, and then activate it, e.g.:
        * `source great_expectations_dev/bin/activate`
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
* You will see a comment from the "CLA Bot" that asks you to complete the Contributor License Agreement form. See details in the section below if you want to learn more.
    * Please complete the form and comment on the PR to say that you’ve signed the form.
* Wait for the other Continuous Integration (CI) checks to go green and watch out for a comment from the automated linter that checks for syntax and formatting issues.
    * Fix any issues that are flagged.
* Once all checks pass, a GE team member will approve your PR and merge it. 
    * GitHub will notify you of comments or a successful merge according to your notification settings.
    * There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to help them get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code!
* Congratulations! You’ve just contributed to Great Expectations!

## About the Contributor License Agreement

*When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.*

Please make sure you have signed our Contributor License Agreement (either [Individual Contributor License Agreement v1.0](https://docs.google.com/forms/d/e/1FAIpQLSdA-aWKQ15yBzp8wKcFPpuxIyGwohGU1Hx-6Pa4hfaEbbb3fg/viewform?usp=sf_link) or [Software Grant and Corporate Contributor License Agreement ("Agreement") v1.0](https://docs.google.com/forms/d/e/1FAIpQLSf3RZ_ZRWOdymT8OnTxRh5FeIadfANLWUrhaSHadg_E20zBAQ/viewform?usp=sf_link)). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

## Testing

### Running tests
In general, you can run all tests by simply running `pytest` in the great_expectations directory root. Before submitting any PR, please ensure there are no tests that fail with an error, and review any warnings.

If you would like to run the tests without the need for local backend setups (e.g. setting up a local postgres database), adding the following flags will skip the respective backends:
- `--no-postgresql` will skip postgres tests
- `--no-spark` will skip spark tests 
- `--no-sqlalchemy` will skip all tests using sqlalchemy (i.e. all database backends)

For example, you can run

`pytest --no-spark --no-sqlalchemy` 

to skip all local backend tests (with the exception of the pandas backend). Please note that these tests will still be run by the CI as soon as you open a PR, so some tests might fail there if your code changes affected them.

### Writing tests

In addition to running existing tests to make sure your code change works, please also write relevant tests for any change that you're making. Our tests live in the `tests` subdirectory of the great_expectations repo.

## Conventions and style

* Ensure any new features or behavioral differences introduced by your changes are documented in the docs, and ensure you have docstrings on your contributions. We use the Sphinx's Napoleon extension to build documentation from Google-style docstrings (see http://www.sphinx-doc.org/en/master/ext/napoleon.html).
* Avoid abbreviations, e.g. use `column_index` instead of `column_idx`.
* Use unambiguous expectation names, even if they're a bit longer, e.g. use `expect_columns_to_match_ordered_list` instead of `expect_columns_to_be`.
* Expectation names should reflect their decorators:
    * `expect_table_...` for methods decorated directly with `@expectation`
    * `expect_column_values_...` for `@column_map_expectation`
    * `expect_column_...` for `@column_aggregate_expectation`
    * `expect_column_pair_values...` for `@column_pair_map_expectation`

These guidelines should be followed consistently for methods and variables exposed in the API. They aren't intended to be strict rules for every internal line of code in every function.

## Release Checklist

GE core team members use this checklist to ship releases.

- [ ] merge all approved PRs into `develop`
- [ ] make a new branch from `develop` called something like `release-prep`
- [ ] in this branch update the version number in the `.travis.yml` file (look in the deploy section)
    - This sed snippet is handy if you change the numbers `sed -i '' 's/0\.9\.6/0\.9\.7/g' .travis.yml  `
- [ ] update the changelog.rst: move all things under `develop` under a new heading w/ the new release number.
- [ ] Submit this as a PR against `develop`
- [ ] After successful checks, get it approved and merged.
- [ ] Update your local branches and switch to master: `git fetch --all; git checkout master; git pull`. 
- [ ] Merge the now-updated `develop` branch into `master` and trigger the release: `git merge origin/develop; git push`
- [ ] Wait for all the builds to complete (including the deploy job)
- [ ] Check [PyPI](https://pypi.org/project/great-expectations/#history) for the new release
- [ ] Create an annotated git tag by
    - [ ] run `git tag -a <<VERSION>> -m "<<VERSION>>"` with the correct new version
    - [ ] push the tag up by running `git push origin <<VERSION>>` with the correct new version
    - [ ] merge `master` into `develop` so that the tagged commit becomes part of the history for `develop`: `git checkout develop; git pull; git merge master`
    - [ ] On develop, add a new "develop" section header to changelog.rst, and push the updated file with message "Update changelog for develop"
- [ ] [Create the release on GitHub](https://github.com/great-expectations/great_expectations/releases) with the version number. Copy the changelog notes into the release notes, and update any rst-specific links to use github issue numbers.
- [ ] Notify kyle@superconductivehealth.com about any community-contributed PRs that should be celebrated.
- [ ] Socialize the relase on GE slack by copying the changelog with an optional nice personal message (thank people if you can)
