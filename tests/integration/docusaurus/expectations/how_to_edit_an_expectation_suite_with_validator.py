"""Steps

Prereqs:
1. Create a context
2. Connect to data
3. Create a suite (onboarding assistant?):

Steps without validator:
1. Load suite
2. List expectations (optional) using public methods e.g. show_expectations_by_domain_type()
    - this is currently a big JSON.
    - the only way to make it not be overhwleming is look only at Table Expectations.

3. Manipulate using public methods e.g. remove_expectation() add_expectation()
TODO: How to edit existing expectation? replace_expectation? patch_expectation? Neither are yet part of public_api
4. Update suite context.update_expectation_suite()
TODO: There are other public methods on ExpectationSuite (not public_api) for listing or removing expectations, which are useful?

Steps with validator:
1. Get validator (with suite + batch)
2. Run expectations & check output
3. Save suite (will be all expectations run)
TODO: If an expectation isn't run will it be edited? How to handle for large suites?
TODO: Discuss `save_failed_expectations` param
"""

"""
Generate Notebook with cell per Expectation
https://docs.greatexpectations.io/docs/0.15.50/guides/expectations/how_to_create_and_edit_expectations_based_on_domain_knowledge_without_inspecting_data_directly
https://docs.greatexpectations.io/docs/0.15.50/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data

* Iterate through Expectations in Suite and print out?
* use the operations in ExpectationSuite to update

patch --> update

If you want more: link to public API
* more about explaining workflow
"""
