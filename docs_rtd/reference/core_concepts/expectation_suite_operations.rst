.. _expectation_suite_operations:

############################
Expectation Suite Operations
############################

A Great Expectations Expectation Suite enables you to perform Create, Read, Update, and Delete (CRUD) operations on a Suite's expectations without needing to re-run them. Each of the methods that support a CRUD operation relies on two main parameters - ``expectation_configuration`` and ``match_type``.

* **expectation_configuration** - an ExpectationConfiguration object that is used to determine whether and where this expectation already exists within the suite. It can be a complete or a partial ExpectationConfiguration.

* **match_type** - a string with the value of ``domain``, ``success``, or ``runtime`` which determines the criteria used for matching.

  - ``domain``checks whether two Expectation Configurations apply to the same data. It results in the loosest match, and can use the least complete ExpectationConfiguration object. For example, for a column map Expectation, ``domain_kwargs`` will include the expectation_type, the column, and any row_conditions that may affect which rows are evaluated by the Expectation.
  - ``success`` criteria are more exacting - in addition to the ``domain`` kwargs, these include those kwargs used when evaluating the success of an expectation, like `mostly`, `max`, or `value_set`.
  - ``runtime`` are the most specific - in addition to ``domain_kwargs`` and ``success_kwargs``, these include kwargs used for runtime configuration. Currently these include ``result_format``, ``include_config``, and ``catch_exceptions``

**Adding or Updating Expectations**

To add an expectation to a suite, you can use
``suite.add_expectation(expectation_configuration, match_type, overwrite_existing)``. If a matching expectation is not found on the suite, this function will add the expectation to the suite. If a matching expectation *is* found on the suite, ``add_expectation`` will throw an error, unless ``overwrite_existing`` is set to ``True``, in which case the found expectation will be updated with ``expectation_configuration``. If more than one expectation is found, this will throw an error, and you will be prompted to be more specific with your matching criteria.

**Removing Expectations**

To remove an expectation from a suite, you can use ``suite.remove_expectation(expectation_configuration, match_type, remove_multiple_matches)``. If this finds one matching expectation, it will remove it. If it finds more than one matching expectation, it will throw an error, unless ``remove_multiple_matches`` is set to True, in which case it will remove all matching expectations. If this finds no matching expectations, it will throw an error.