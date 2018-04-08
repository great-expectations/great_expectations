
## How to contribute
We're excited for contributions to Great Expectations. If you see places where the code or documentation could be improved, please get involved!

Submitting your changes
Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken. See the the section on testing below for help running tests. (Hint: `pytest` from the great_expectations root.)

2. Update the documentation
    
    Ensure any new features or behavioral differences introduced by your changes are documented in the docs, and ensure you have docstrings on your contributions. We use the Sphinx's Napoleon extension to build documentation from Google-style docstrings (see http://www.sphinx-doc.org/en/master/ext/napoleon.html).

3. Sign the Contributor License Agreement

    **When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.**

    {Aspirational:} Please make sure you have signed our Contributor License Agreement. We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.
    
4. Rebase your changes

    Update your local repository with the most recent code from the main Great Expectations repository, and rebase your branch on top of the latest `develop` branch. We prefer small, incremental commits, because it makes the thought process behind changes easier to review.

5. Submit a pull request

    Push your local changes to your forked copy of the repository and submit a pull request. In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

6. Participate in review

    There will probably be discussion about the pull request. It's normal for a request to require some changes before merging it into the main Great Expectations project. We enjoy working with contributors to get their code accepted. There are many approaches to fixing a problem and it is important to find the best approach before writing too much code.

## Testing
We are actively migrating many of our tests to a new format to support testing across different dataset types. Consolidating them is an important next step. That means two things for contributors:

For now, write tests in whatever style suits your fancy. We (the core contributors) will worry about refactoring them later. As long as your thing works and is well-tested, you're good.

(This is **not** an excuse to avoid writing tests. All contributions must be under test. We're just not dogmatic about the style of those tests today.)

Second, if you have opinions on the testing framework, we'd love to hear them! Feedback based on your perspective and experience is very welcome.

Most of the discussion to date is encapsulated here: https://github.com/great-expectations/great_expectations/issues/167. The `refactor_tests` branch is intended as a pilot implementation.

## Conventions and Style

* Avoid abbreviations (`column_idx` < `column_index`)
* Use unambiguous expectation names, even if they're a bit longer. (`expect_columns_to_be` < `expect_columns_to_match_ordered_list`)

Expectations aren't just tests---they're also a kind of data documentation. Because we want expectations to be easy to interpret, we're avoiding abbreviations almost everywhere. We're not entirely consistent about this yet, but there's pretty strong consensus among early team and users that we should be heading in that direction.

These guidelines should be followed consistently for methods and variables exposed in the API. They aren't intended to be strict rules for every internal line of code in every function.

* Expectation names should reflect their decorators.

`expect_table_...` for methods decorated directly with `@expectation`
`expect_column_values_...` for `@column_map_expectation`
`expect_column_...` for `@column_aggregate_expectation`
`expect_column_pair_values...` for `@column_pair_map_expectation`
