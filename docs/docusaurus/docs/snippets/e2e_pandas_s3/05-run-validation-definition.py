from great_expectations import get_context

context = get_context(project_root_dir="./gx")
# NOTE: It is critical to pass the batch_parameters to the run method, otherwise the validation stall
# by trying to read all the data. We will have a fix in place before the final release.
# TODO: Implement fix for above issue
context.validation_definitions.get("my_project").run(batch_parameters={"year": "2020", "month": "04"})
