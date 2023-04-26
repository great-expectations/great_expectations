"""Steps

Prereqs:
1. Create a context
2. Connect to data
3. Create a suite (onboarding assistant?)

Steps without validator:
1. Load suite
2. List expectations (optional) using public methods e.g. show_expectations_by_domain_type()
3. Manipulate using public methods e.g. remove_expectation() add_expectation()
TODO: How to edit existing expectation?
4. Update suite context.update_expectation_suite()

Steps with validator:
1. Get validator (with suite + batch)
2. Run expectations & check output
3. Save suite (will be all expectations run)
TODO: If an expectation isn't run will it be edited? How to handle for large suites?
TODO: Discuss `save_failed_expectations` param
"""