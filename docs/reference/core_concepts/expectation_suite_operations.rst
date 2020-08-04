.. _expectation_suite_operations:

############################
Expectation Suite Operations
############################

GE's ExpectationSuite objects enable you to perform CRUD operations on a suite's expectations without needing to re-run
them. There are functions for finding expectations, removing expectations, and adding/updating expectations. Each of
these functions use two main parameters - `expectation_configuration` and `match_type`.

* **expectation_configuration** - an ExpectationConfiguration object that is used to determine whether and where this expectation already exists within the suite. It can be a complete or a partial ExpectationConfiguration - more information on this below.

* **match_type** - one of `domain_kwargs`, `success_kwargs`, or `runtime_kwargs` which determine the criteria used for matching.

  - `domain_kwargs` are the loosest, and can use the least complete ExpectationConfiguration object - `domain_kwargs` consist of expectation_type and location (the specific column for columnar expectations, or table if the expectation is about the table). These two pieces of information comprise the minimum requirements for matching an ExpectationConfiguration
  - `success_kwargs` criteria are more exacting - in addition to the `domain_kwargs`, these include those kwargs used when evaluating the success of an expectation, like `mostly`, `max`, or `value_set`
  - `runtime_kwargs` are the most specific - in addition to `domain_kwargs` and `success_kwargs`, these include kwargs used for configuration. Currently these include `result_format`, `include_config`, and `catch_exceptions`

**Adding or Updating Expectations**

To add an expectation to a suite, you can use
`suite.add_expectation(expectation_configuration, match_type, overwrite_existing)`. If the expectation is not
found on the suite, this function will add the expectation to the suite. If the expectation *is* found on the suite,
this will throw an error, unless `overwrite_existing` is set to `True`, in which case the found expectation will be
updated with `expectation_configuration`. If more than one expectation is found, this will throw an error, and you will
be prompted to be more specific with your matching criteria.

**Removing Expectations**

To remove an expectation from a suite, you can use
`suite.remove_expectation(expectation_configuration, match_type, remove_multiple_matches)`. If this finds one matching
expectation, it will remove it. If it finds more than one matching expectation, it will throw an error, unless
`remove_multiple_matches` is set to True, in which case it will remove all matching expectations. If this finds no
matching expectations, it will throw an error.