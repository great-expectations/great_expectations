.. _contrib_pull_request_acceptance_checklist:


Acceptance Checklist for "contrib" Pull Requests
================================================

This acceptance checklist applies to "contrib" Pull Requests - PRs that make modifications only in ``/contrib`` directory of the repo which contains community contributed Expectations.


Both contributors and reviewers should follow this checklist:

* Scope of the PR.
    * A PR may introduce a new Expectation. The minimal acceptable version of a new Expectation is a module that contains a new class with the name of the Expectation, a Docstring describing the behavior, and examples/tests. The class may leave all methods unimplemented. Metric classes required by the new Expectation may be left undefined/unimplemented. This minimal contribution can be viewed as a "RFE" - Request for Expectation.
    * If the PR modifies existing Expectations and/or Metrics, check that the changes are an improvement and are not destructive. The contributor should not remove someone else's prior work without a clear reason.

* Basic class/module structure and naming of new Expectation and Metric classes
    * One new Expectation class per module (file).
    * The modules are placed in a subdirectory of `/contrib/experimental/great_expectations_experimental/expectations` that makes sense for the new Expectation
    * The new Expectation classes follow CamelCase convention. Module names are snake_case versions of the Expectation class names.
    * Metric classes that implement the new Metrics the Expectation depends on are implemented in the same module.
    * Expectation class names summarize the nature of the expectation.
    * New Expectation and Metric classes extend the appropriate core classes (e.g., new column map expectations extend ColumnMapExpectation)

* Docstring
    * Docstring must contain a legible description of the functionality of the new Expectations.

* Library metatada
    * The `library_metadata` is the information required to add an Expectation to the Public Expectation Gallery. Contributors, please complete as much of the section as possible. Reviewers, please complete any remaining fields to avoid extra asks to contributors. Adapt the metadata example below as appropriate for the new contribution.

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
    * Any module that contains a new Expectation must have ``examples`` defined. The examples variable defines data, inputs, and outputs that serve both as examples and as tests. At least one positive and at least one negative example must be present. Below is an example of this structure for an imaginary expectation ``expect_column_values_to_begin_with_specific_letter``:

    .. code-block:: python

        examples = [{
            "data": {
                "myvalues": ["apple", "pear", "acyclical graph", "aardvardk", "mouse", "angular"],

            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "in": {"column": "myvalues", "letter": "a", "mostly": 0.4},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [1, 4],
                        "unexpected_list": ["pear", "mouse"]
                    },
                    "include_in_gallery": True
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "in": {"column": "myvalues", "letter": "p", "mostly": 0.4},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 2, 3, 4, 5],
                        "unexpected_list": ["apple", "acyclical graph", "aardvardk", "mouse", "angular"],
                    },
                    "include_in_gallery": True
                }
            ],
        }]

    * Test titles must contain no spaces so that they can be individually selected by pytest.

    * If a test should be rendered as an example, it should be marked up with ``include_in_gallery: True``

    * The tests included in the module must pass for the PR to be approved.

 * Verify that changes to existing renderers (or new renderers) make sense--the language needs to accurately describe the semantics and behavior of the Expectation. 

* Clarification about imports, renderers
    * At this time, unused module imports and commented out renderer code from the template should not be removed if left in by the contributor. However if the contributor has already removed it, please do not put it back in.

* And finally, "Does it run?"
    * Check out the PR branch and run the ``run_diagnostics`` method of the Expectation. This :ref:`how-to guide <how_to_guides__creating_and_editing_expectations__how_to_template>` shows how to do it. Check the output and make sure everything ran without errors.
