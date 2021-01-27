.. _contrib_pull_request_acceptance_checklist:


Acceptance Checklist for "contrib" Pull Requests
================================================

This acceptance checklist applies to "contrib" Pull Requests - PRs that make modifications only in ``/contrib`` directory of the repo which contains community contributed Expectations.


Both contributors and reviewers should follow this checklist:

* Scope of the PR.
    * A PR may introduce a new Expectation. The minimal acceptable version of a new Expectation is a module that contains a new class with the name of the Expectation, a Docstring describing the behavior, and examples/tests. The class may leave all methods unimplemented. Metric classes required by the new Expectation may be left undefined/unimplemented. This minimal contribution can be viewed as a "RFE" - Request for Expectation.
    * If the PR modifies existing Expectations and/or Metrics, check that the changes are an improvement and are not destructive. The contributor should not remove someone else' prior work without a clear reason.

* Basic class/module structure and naming of new Expectation and Metric classes
    * One new Expectation class per module.
    * The modules are placed in a subdirectory of `/contrib/experimental/expectations` that makes sense for the new Expectation
    * The new Expectation classes follow CamelCase convention. Module names are snake_case versions of the Expectation class names.
    * Metric classes that implement the new Metrics the Expectation depends on are implemented in the same module.
    * Expectation class names summarize the nature of the expectation - this is a judgement call.
    * New Expectation and Metric classes extend the appropriate core classes (e.g., new column map expectations extend ColumnMapExpectation)

* Docstring
    * Docstring must contain a legible description of the functionality of the new Expectations.

* Library metatada
    * this is the information required to add an Expectation to the Public Expectation Gallery. We expect the contributor to fill out this section, but we will "pick up the slack" if needed. The module containing a new Expectation must have a comment that looks as follows:

    .. code-block:: python

        library_metadata = {
            "maturity": "experimental", # "experimental", "beta", or "production"
            "tags": [  # Tags for this Expectation in the gallery
        #         "experimental"
            ],
            "contributors": [ # Github handles for all contributors to this Expectation.
        #         "@your_name_here", # Don't forget to add your github handle here!
            ],
            "package": "experimental_expectations",
        }



* Examples/tests
    * The module that contains a wew Expectations must have ``examples`` variable defined. The variable's value is a structure that defines inputs and outputs that serve both as examples and as tests. At least one positive and at least one negative example must be present. Below is an example of this structure for an imaginary expectation ``expect_column_values_to_begin_with_specific_letter``:

    .. code-block:: python

        examples = [{
            "data": {
                "myvalues": ["apple", "pear", "acyclical graph", "aardvardk", "mouse", "angular"],

            },
            "tests": [
                {
                    "title": "maintest",
                    "exact_match_out": False,
                    "in": {"column": "myvalues", "letter": "a", "mostly": 0.4},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [1, 4],
                        "unexpected_list": ["pear", "mouse"]
                    },
                }
            ],
        }]

    * Test titles must contain no spaces so that they can be individually selected by pytest.

    * If a test should be rendered as an example, it should be marked up with ``gallery_example: True``

    * If new Expectation class in the PR goes beyond "RFE" (it does not just define the name and the Docstring of the new Expectation, but has elements of implementation), the tests included in the module must pass for the PR to be approved.

 * If the PR contains changes to existing renderers (or new renderers), the wording must "make sense" - this is a judgement call.

* And finally, "Does it run?"
    * Check out the PR branch and run the ``run_diagnostics`` method of the Expectation. This :ref:`how-to guide <how_to_guides__creating_and_editing_expectations__how_to_template>` shows how to do it. Check the output and make sure everything ran without errors.